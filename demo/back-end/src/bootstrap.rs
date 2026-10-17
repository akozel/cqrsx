//! Composition root: every service built from the configuration and the open connections.
//!
//! [`wire`] performs no I/O: pools and clients connect lazily, so `main.rs` (which owns the process
//! lifecycle) and the router tests (which never reach a database) build the very same graph.
use crate::{
    application::{
        activity::ActivityHub,
        command_service::{ConcertCommands, Executors, FanCommands, NamedHandler},
        lock::BoxOfficeLock,
        operations::{rebuild::RebuildService, time_travel::TimeTravel, verify::VerifyService},
    },
    config::Config,
    domain::concert::ConcertServices,
    infrastructure::{
        mail::{
            chaos::Random,
            flaky_mail_server::{MailSettings, MongoInbox},
        },
        mongo::read_side::{CONCERT_HALLS, FAN_WALLETS, MAIL_INBOX, ReadSide},
        task_runtime::{
            adapter::EncoreTaskAdapter,
            dispatcher::{DispatcherDeps, dispatcher_factory},
        },
    },
};
use cqrsx::{
    cqrs::command::retry::AggregateConflictRetryPolicy,
    postgres::command::{PostgresCommandConfig, PostgresCommandConfigError},
    task::runtime::dispatcher_factory::TaskDispatcherFactory,
};
use sqlx::PgPool;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Broadcast capacity of the activity log: a slow SSE client lags (and skips) past this many
/// entries, it never blocks a publisher.
pub const ACTIVITY_CHANNEL_CAPACITY: usize = 1024;
/// How many recent entries a new SSE subscriber receives in its `hello`.
pub const ACTIVITY_BACKLOG: usize = 100;

/// Everything the HTTP handlers use.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub concert_commands: Arc<ConcertCommands>,
    pub activity: Arc<ActivityHub>,
    pub rebuild: Arc<RebuildService>,
    pub read: Arc<ReadSide>,
    pub time_travel: Arc<TimeTravel>,
    pub verify: Arc<VerifyService>,
    /// SSE streams end when this token is cancelled; otherwise graceful shutdown would wait for
    /// every open browser tab.
    pub shutdown: CancellationToken,
}

/// The wired application: the HTTP state and the task handlers for the outbox runtime.
pub struct Wiring {
    pub state: AppState,
    pub dispatcher: TaskDispatcherFactory<EncoreTaskAdapter<MongoInbox, Random>>,
}

#[derive(Debug, thiserror::Error)]
pub enum WiringError {
    #[error("snapshot configuration: {0}")]
    Command(#[from] PostgresCommandConfigError),
}

pub fn wire(
    config: Arc<Config>,
    pool: PgPool,
    mongo: &mongodb::Client,
    shutdown: CancellationToken,
) -> Result<Wiring, WiringError> {
    // MongoDB: read models (`encore_read`) and the simulated external mail service (`mail_service`).
    let mail_db = mongo.database(&config.mail_db);
    let inbox = Arc::new(MongoInbox::new(mail_db.collection(MAIL_INBOX)));
    let read_db = mongo.database(&config.read_db);
    let read = Arc::new(ReadSide::new(&read_db, &mail_db));

    let activity = Arc::new(ActivityHub::new(ACTIVITY_CHANNEL_CAPACITY, ACTIVITY_BACKLOG));
    let lock = Arc::new(BoxOfficeLock::new());
    let rebuild = Arc::new(RebuildService::new(
        lock.clone(),
        pool.clone(),
        read.clone(),
        activity.clone(),
        config.replay_slow_delay,
    ));
    let time_travel = Arc::new(TimeTravel::new(pool.clone()));
    let verify = Arc::new(VerifyService::new(pool.clone(), read_db, read.clone()));
    let command_config = PostgresCommandConfig::new(config.snapshot_every, AggregateConflictRetryPolicy::default())?;
    let executors = Executors {
        pool,
        snapshot_every: command_config.snapshot_interval(),
        retry: command_config.conflict_retry(),
    };
    let concert_commands = Arc::new(ConcertCommands::new(
        &executors,
        vec![NamedHandler::projection(CONCERT_HALLS, read.halls.clone(), activity.clone())],
        ConcertServices {
            ticket_limit: config.ticket_limit,
        },
        activity.clone(),
        lock.clone(),
        config.chain_max_attempts,
    ));
    let fan_commands = Arc::new(FanCommands::new(
        &executors,
        vec![NamedHandler::projection(FAN_WALLETS, read.wallets.clone(), activity.clone())],
        activity.clone(),
        lock,
    ));

    // Outbox task handlers: a fresh set (and a fresh chaos RNG) for every worker.
    let dispatcher = dispatcher_factory(DispatcherDeps {
        fan_commands,
        inbox,
        activity: activity.clone(),
        mail: MailSettings {
            latency: config.task_latency,
            retry_base: config.task_retry_base,
            retry_max: config.task_retry_max,
        },
        failure_rate: config.task_failure_rate,
        chaos_seed: config.chaos_seed,
        task_max_attempts: config.task_max_attempts,
    });

    Ok(Wiring {
        state: AppState {
            config,
            concert_commands,
            activity,
            rebuild,
            read,
            time_travel,
            verify,
            shutdown,
        },
        dispatcher,
    })
}
