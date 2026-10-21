//! Composition root: every service built from the configuration and the open connections.
//!
//! [`wire`] performs no I/O: pools and clients connect lazily, so `main.rs` (which owns the process
//! lifecycle) and the router tests (which never reach a database) build the very same graph.
use crate::{
    application::{
        self,
        activity::ActivityHub,
        command_service::CommandService,
        concert::views::concert_hall::ConcertHallView,
        fan::views::fan_wallet::FanWalletView,
        lock::BoxOfficeLock,
        operations::{rebuild::RebuildService, verify::VerifyService},
        projections::{
            maintenance::{MaintainedReadModel, ProjectionMaintenance},
            notifying::NotifyingHandler,
        },
        read_models::ReadModels,
    },
    config::Config,
    domain::{
        concert::{Concert, ConcertServices},
        fan::Fan,
    },
    infrastructure::{
        mail::http_mailer::{HttpMailer, MailClientSettings},
        mongo::read_side::{CONCERT_HALLS, FAN_WALLETS, ReadSide, scratch},
        postgres::{HistoryStore, PostgresLedger, event_history, history_repositories, history_store},
        task_runtime::dispatcher::{DispatcherDeps, dispatcher_factory},
    },
};
use cqrsx::{
    cqrs::{command::retry::AggregateConflictRetryPolicy, projection::ViewProjection},
    postgres::command::{PostgresCommandConfig, PostgresCommandConfigError, PostgresCommandExecutor},
    task::{adapter::boxed::BoxedTaskAdapter, runtime::dispatcher_factory::TaskDispatcherFactory},
};
use sqlx::PgPool;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Broadcast capacity of the activity log: a slow SSE client lags (and skips) past this many
/// entries, it never blocks a publisher.
pub const ACTIVITY_CHANNEL_CAPACITY: usize = 1024;
/// How many recent entries a new SSE subscriber receives in its `hello`.
pub const ACTIVITY_BACKLOG: usize = 100;

/// The box office over PostgreSQL.
pub type ConcertApp = application::concert::ConcertApp<PostgresCommandExecutor<Concert>, HistoryStore<Concert>>;
/// The fan ledger over PostgreSQL.
pub type FanApp = application::fan::FanApp<PostgresCommandExecutor<Fan>>;
/// The time machine over the PostgreSQL history, without snapshots.
pub type TimeTravel = application::operations::time_travel::TimeTravel<HistoryStore<Concert>>;

/// Everything the HTTP handlers use.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub concerts: Arc<ConcertApp>,
    pub activity: Arc<ActivityHub>,
    pub rebuild: Arc<RebuildService>,
    pub read: Arc<ReadModels>,
    pub time_travel: Arc<TimeTravel>,
    pub verify: Arc<VerifyService>,
    /// SSE streams end when this token is cancelled; otherwise graceful shutdown would wait for
    /// every open browser tab.
    pub shutdown: CancellationToken,
}

/// The wired application: the HTTP state and the task handlers for the outbox runtime.
pub struct Wiring {
    pub state: AppState,
    pub dispatcher: TaskDispatcherFactory<BoxedTaskAdapter>,
}

#[derive(Debug, thiserror::Error)]
pub enum WiringError {
    #[error("snapshot configuration: {0}")]
    Command(#[from] PostgresCommandConfigError),
    #[error("mail service client: {0}")]
    MailClient(#[from] reqwest::Error),
}

/// `pool` serves the commands and the outbox queue: short statements under its statement timeout.
/// `history_pool` has no statement timeout and serves the reads of whole histories: Encore!, verify,
/// the ledger totals and the time machine.
pub fn wire(
    config: Arc<Config>,
    pool: PgPool,
    history_pool: PgPool,
    mongo: &mongodb::Client,
    shutdown: CancellationToken,
) -> Result<Wiring, WiringError> {
    // MongoDB: the read models (`encore_read`). The mail inbox is the external mail service's
    // storage, not ours: the box office never opens it.
    let read_db = mongo.database(&config.read_db);
    let read = ReadSide::new(&read_db);

    // Encore! and verify: both read models, in replay order, over the PostgreSQL history.
    let history = history_repositories(history_pool.clone());
    let read_models: Vec<Arc<dyn MaintainedReadModel>> = vec![
        Arc::new(ProjectionMaintenance::<_, _, ConcertHallView, Concert>::new(
            CONCERT_HALLS,
            history.clone(),
            read.halls.clone(),
            scratch(&read_db, CONCERT_HALLS),
        )),
        Arc::new(ProjectionMaintenance::<_, _, FanWalletView, Fan>::new(
            FAN_WALLETS,
            history,
            read.wallets.clone(),
            scratch(&read_db, FAN_WALLETS),
        )),
    ];

    let activity = Arc::new(ActivityHub::new(ACTIVITY_CHANNEL_CAPACITY, ACTIVITY_BACKLOG));
    let lock = Arc::new(BoxOfficeLock::new());
    let rebuild = Arc::new(RebuildService::new(
        lock.clone(),
        read_models.clone(),
        Arc::new(PostgresLedger::new(history_pool.clone())),
        activity.clone(),
        config.replay_slow_delay,
    ));
    let verify = Arc::new(VerifyService::new(read_models));
    let time_travel = Arc::new(TimeTravel::new(event_history(history_pool)));
    let command_config = PostgresCommandConfig::new(
        config.snapshot_every,
        AggregateConflictRetryPolicy::default(),
        config.command_ordering,
    )?;
    // One executor per aggregate: with per-aggregate ordering it owns the turns of that aggregate, so the
    // HTTP commands and the chain tasks' commands to one concert or fan ledger wait for each other. Each
    // projection is registered under its collection name, the name the activity log and the
    // `projection_failed` warning report.
    let executor = PostgresCommandExecutor::new(
        pool.clone(),
        command_config.clone(),
        ConcertServices {
            ticket_limit: config.ticket_limit,
        },
    )
    .with_event_handler(Arc::new(NotifyingHandler::new(
        ViewProjection::new(read.halls.clone()).with_name(CONCERT_HALLS),
        activity.clone(),
    )));
    let concerts = Arc::new(ConcertApp::new(
        CommandService::new(executor, activity.clone(), lock.clone()),
        history_store(pool.clone(), command_config.snapshot_interval()),
        config.chain_max_attempts,
    ));
    let executor = PostgresCommandExecutor::new(pool, command_config, ()).with_event_handler(Arc::new(NotifyingHandler::new(
        ViewProjection::new(read.wallets.clone()).with_name(FAN_WALLETS),
        activity.clone(),
    )));
    let fans = Arc::new(FanApp::new(CommandService::new(executor, activity.clone(), lock)));

    // Outbox task handlers: a fresh set for every worker. The mailers share one HTTP client to the
    // external mail service (a connection pool is meant to be shared); building it is no I/O.
    let mail = MailClientSettings {
        base_url: config.mail_service_url.clone(),
        timeout: config.mail_request_timeout(),
        retry_base: config.task_retry_base,
        retry_max: config.task_retry_max,
    };
    let http = HttpMailer::client(&mail)?;
    let dispatcher = dispatcher_factory(DispatcherDeps {
        fans,
        activity: activity.clone(),
        http,
        mail,
        task_max_attempts: config.task_max_attempts,
    });

    Ok(Wiring {
        state: AppState {
            config,
            concerts,
            activity,
            rebuild,
            read: Arc::new(ReadModels {
                halls: read.halls,
                wallets: read.wallets,
            }),
            time_travel,
            verify,
            shutdown,
        },
        dispatcher,
    })
}
