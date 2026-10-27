//! Composition root: what the box office is made of, built from a [`Process`] without I/O, so the binary and the
//! router tests share one graph. The modules register what they plug in ([`modules`]: `concert`, `fan`,
//! `operations`); until the engine has its PostgreSQL and MongoDB backends (backlog 05/12) the registries below build
//! the components over them from what the modules registered:
//!
//! - [`aggregates`]: one module per aggregate: its command executor, which wakes its read model's processor;
//! - [`projections`]: the read models: their collections, processors and verification;
//! - [`tasks`]: the outbox tasks: their handlers, in one dispatcher, and the policies of their types;
//! - [`routes`]: the HTTP endpoints.
//!
//! [`read_models`] and [`ledger`] are what only the composition knows of every module: the read models the HTTP
//! queries read through the engine's repositories and the journals of every aggregate type.
pub mod aggregates;
pub mod ledger;
pub mod projections;
pub mod read_models;
pub mod routes;
pub mod tasks;

use crate::{
    bootstrap::{ledger::PostgresLedger, read_models::ReadModels},
    concert::{self, domain::Concert},
    config::Config,
    fan,
    operations::{
        self,
        application::{
            rebuild::{ReadSide, ReadSideLimits, RebuildService},
            verify::VerifyService,
        },
    },
    process::Process,
    shared::{
        application::activity::ActivityHub,
        infrastructure::postgres::{HistoryStore, event_history},
    },
};
use cqrsx::{
    cqrs::projection::runtime::ProjectionRuntime,
    engine::{
        Cqrs,
        build::{BuildError, Builder},
        setup::{Settings, Setup},
    },
    postgres::command::{PostgresCommandConfigError, PostgresCommandExecutor},
    task::{
        adapter::boxed::BoxedTaskAdapter,
        dependencies::MissingDependency,
        dispatcher::{TaskDispatcher, TaskDispatcherError},
        policy::Policies,
    },
};
use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};
use tokio_util::sync::CancellationToken;

/// The time machine over the PostgreSQL history, without snapshots.
pub type TimeTravel = crate::concert::application::time_travel::TimeTravel<HistoryStore<Concert>>;

/// Everything the HTTP handlers use.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    /// The box office's executor: the HTTP handler hands it the commands.
    pub concerts: Arc<PostgresCommandExecutor<Concert>>,
    pub activity: Arc<ActivityHub>,
    /// Encore!, the rebuild of one hall and the status of the projections.
    pub rebuild: Arc<dyn ReadSide>,
    pub read: Arc<ReadModels>,
    pub time_travel: Arc<TimeTravel>,
    pub verify: Arc<VerifyService>,
    /// SSE streams end when this token is cancelled; otherwise graceful shutdown would wait for
    /// every open browser tab.
    pub shutdown: CancellationToken,
}

/// The wired application: the HTTP state, the task handlers and the policies of the task types for the outbox
/// runtime, and the projection processors that feed the read models from the journal.
pub struct Wiring {
    pub state: AppState,
    pub dispatcher: TaskDispatcher<BoxedTaskAdapter>,
    pub policies: Policies,
    pub projections: ProjectionRuntime,
}

#[derive(Debug, thiserror::Error)]
pub enum WiringError {
    #[error("snapshot configuration: {0}")]
    Command(#[from] PostgresCommandConfigError),
    #[error("modules: {0}")]
    Modules(#[from] BuildError),
    #[error("modules: {0}")]
    Dependency(#[from] MissingDependency),
    #[error("modules: no module registers the aggregate type `{0}`")]
    NotRegistered(&'static str),
    #[error("task handlers: {0}")]
    Handlers(#[from] TaskDispatcherError),
}

/// The modules of the box office in the engine, over the configuration: the engine's settings of it and the modules'
/// variables as it read them. The configuration reads and checks every variable first (until backlog 05/12).
pub fn modules(config: &Config) -> Result<Builder, WiringError> {
    Ok(Cqrs::in_memory()
        .settings(settings(config)?)
        .env("ENCORE", module_env(config))
        .module(concert::module)
        .module(fan::module)
        .module(operations::module))
}

/// The engine's settings of the configuration: every executor takes a snapshot every `ENCORE_SNAPSHOT_EVERY` events,
/// retries a conflict by the library's policy and bounds its locks by the configuration; every processor runs with
/// `ENCORE_PROJECTION_*` and this node's name; every task attempt with the task runtime's handler timeout, lease and
/// storage budget.
pub fn settings(config: &Config) -> Result<Settings, PostgresCommandConfigError> {
    let worker = config.task_runtime.worker_pool();
    Ok(Settings {
        snapshot_every: NonZeroUsize::new(config.snapshot_every).ok_or(PostgresCommandConfigError::InvalidSnapshotInterval)?,
        lock_limits: config.command_lock_limits,
        processor: config.projection,
        node_name: Some(config.node_name.clone()),
        handler_timeout: worker.handler_timeout(),
        task_lease: worker.lease_duration(),
        task_storage: config.task_runtime.storage().clone(),
        ..Settings::default()
    })
}

/// The setup a module gets in the process: its variables as the configuration read them.
pub fn setup(config: &Config) -> Result<Setup, PostgresCommandConfigError> {
    Ok(Setup::new("ENCORE", module_env(config), settings(config)?))
}

/// The variables the modules read (`ENCORE_TICKET_LIMIT`, `ENCORE_MAIL_SERVICE_URL`, `ENCORE_TASK_*`), with the values
/// of the configuration, whatever set them.
fn module_env(config: &Config) -> impl Fn(&str) -> Option<String> + Send + Sync + 'static {
    let values = HashMap::from([
        ("ENCORE_TICKET_LIMIT", config.ticket_limit.to_string()),
        ("ENCORE_MAIL_SERVICE_URL", config.mail_service_url.clone()),
        ("ENCORE_TASK_MAX_ATTEMPTS", config.task_max_attempts.to_string()),
        ("ENCORE_TASK_RETRY_BASE_MS", config.task_retry_base.as_millis().to_string()),
        ("ENCORE_TASK_RETRY_MAX_MS", config.task_retry_max.as_millis().to_string()),
    ]);
    move |name| values.get(name).cloned()
}

/// Builds the graph from what the modules registered, in dependency order: the read side (processors and their
/// wake-ups, observed by the activity log), the aggregates whose commits wake the read side, whose reactions plan with
/// the policies and whose commands the activity log observes, the operations over the whole ledger, the task handlers.
pub fn wire(process: &Process) -> Result<Wiring, WiringError> {
    let config = process.config.clone();
    let mut registrations = modules(&config)?.registrations()?;
    let activity = registrations.dependency::<ActivityHub>()?;

    let read = projections::read_models(process, &registrations);
    let concerts = aggregates::concert::wire(process, &mut registrations, &read.concert_wakeup)?;
    let fans = aggregates::fan::wire(process, &mut registrations, &read.fan_wakeup)?;

    // Operations over the whole ledger: Encore! and the status, verify and the time machine.
    let rebuild: Arc<dyn ReadSide> = Arc::new(RebuildService::new(
        read.checkpoints.clone(),
        projections::checkpoint_keys(),
        Arc::new(PostgresLedger::new(process.pool.clone(), process.history_pool.clone())),
        activity.clone(),
        ReadSideLimits {
            lag_cap: config.projection_lag_cap,
            max_pace: config.replay_max_pace,
            status_timeout: config.projection_status_timeout,
        },
        process.shutdown.clone(),
    ));
    let verify = Arc::new(VerifyService::new(read.verification));
    let time_travel = Arc::new(TimeTravel::new(event_history(process.history_pool.clone())));

    let policies = registrations.policies().clone();
    let dispatcher = registrations.take_task_handlers().executor(fans).build()?;

    Ok(Wiring {
        state: AppState {
            config,
            concerts,
            activity,
            rebuild,
            read: Arc::new(read.queries),
            time_travel,
            verify,
            shutdown: process.shutdown.clone(),
        },
        dispatcher,
        policies,
        projections: read.runtime,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fan::{application::send_e_ticket::SendETicketV1, domain::Fan},
        shared::domain::{ConcertId, FanId, venue::SeatId},
        testing,
    };
    use cqrsx::{
        cqrs::{
            aggregate::Aggregate,
            command::{
                locks::{DEFAULT_LOCK_MAX_WAIT, DEFAULT_LOCK_MAX_WAITERS, LockLimits},
                retry::AggregateConflictRetryPolicy,
            },
        },
        engine::setup::DEFAULT_SNAPSHOT_EVERY,
        postgres::command::PostgresCommandConfig,
        task::{attempt::TaskAttempt, format::TaskFormat, result::TaskResult},
    };
    use std::time::Duration;

    #[tokio::test]
    async fn the_read_side_is_wired_with_one_processor_per_read_model() {
        let wiring = wire(&testing::process().await).unwrap();
        assert_eq!(wiring.projections.len(), 2, "the concert halls and the fan wallets");
        assert_eq!(projections::checkpoint_keys().len(), 2);
    }

    /// G-01, G-05, G-07: the engine builds its executors and processors with the settings of the configuration, which
    /// are the library's lock limits and conflict retry, a snapshot every ten events (the engine's default too), the
    /// projection settings, this node and the task runtime's budgets; the executors of the process get the same.
    #[test]
    fn the_engine_settings_are_the_configuration_and_the_library_constants() {
        let config = Config::from_lookup(|_| None).unwrap();
        let settings = settings(&config).unwrap();
        let library = LockLimits::new(DEFAULT_LOCK_MAX_WAIT, DEFAULT_LOCK_MAX_WAITERS.get()).unwrap();
        assert_eq!(
            (settings.snapshot_every, settings.conflict_retry, settings.lock_limits),
            (DEFAULT_SNAPSHOT_EVERY, AggregateConflictRetryPolicy::default(), library)
        );
        assert_eq!(
            (settings.snapshot_every.get(), settings.lock_limits, settings.processor),
            (config.snapshot_every, config.command_lock_limits, config.projection)
        );
        let worker = config.task_runtime.worker_pool();
        assert_eq!(
            (settings.node_name.as_deref(), settings.handler_timeout, settings.task_lease),
            (
                Some(config.node_name.as_str()),
                worker.handler_timeout(),
                worker.lease_duration()
            )
        );
        assert_eq!(
            settings.task_storage.storage_timeout(),
            config.task_runtime.storage().storage_timeout()
        );
        assert_eq!(
            aggregates::command_config(&settings).unwrap(),
            PostgresCommandConfig::new(10, AggregateConflictRetryPolicy::default(), library).unwrap(),
            "the executors of the process"
        );
    }

    /// The modules read the variables the configuration read, and without them their defaults are the configuration's.
    #[test]
    fn the_modules_default_to_the_values_of_the_configuration() {
        let config = Config::from_lookup(|_| None).unwrap();
        let bare = || {
            Cqrs::in_memory()
                .module(concert::module)
                .module(fan::module)
                .module(operations::module)
        };
        let mut defaults = bare().registrations().unwrap();
        let mut configured = modules(&config).unwrap().registrations().unwrap();
        let limit = |registrations: &mut cqrsx::engine::build::Registrations| {
            registrations.take_rules::<Concert>().map(|rules| rules.ticket_limit)
        };
        assert_eq!(
            (limit(&mut defaults), limit(&mut configured)),
            (Some(4), Some(config.ticket_limit))
        );
        let formats = [
            "encore_record_ticket",
            "encore_record_cancellation",
            "encore_send_e_ticket",
            "encore_issue_refund",
        ]
        .map(|name| TaskFormat::new(name, 1).unwrap());
        let of = |policies: &Policies| formats.clone().map(|format| policies.of(&format).cloned());
        assert_eq!(of(defaults.policies()), of(configured.policies()));
        let default_mail = fan::mail_settings(&Setup::new("ENCORE", |_| None, settings(&config).unwrap())).unwrap();
        assert_eq!(
            (default_mail.base_url, default_mail.timeout),
            (config.mail_service_url.clone(), config.mail_request_timeout())
        );
        let mut changed = testing::config();
        (changed.ticket_limit, changed.mail_service_url) = (7, "http://mail:8025".into());
        let mut registrations = modules(&changed).unwrap().registrations().unwrap();
        assert_eq!(limit(&mut registrations), Some(7));
        assert_eq!(tasks::mail_settings(&changed).unwrap().base_url, "http://mail:8025");
    }

    #[tokio::test]
    async fn bootstrap_wires_every_aggregate_and_view_the_modules_register() {
        let registrations = modules(&testing::config()).unwrap().registrations().unwrap();
        assert_eq!(registrations.aggregate_types(), [Concert::TYPE, Fan::TYPE]);
        let keys = projections::checkpoint_keys();
        let wired: Vec<&str> = keys.iter().map(|key| key.projection()).collect();
        assert_eq!(registrations.view_names(), wired);
        let mut without_fan = Cqrs::in_memory().module(concert::module).registrations().unwrap();
        let refused = aggregates::fan::wire(&testing::process().await, &mut without_fan, &Default::default()).err();
        assert!(matches!(refused, Some(WiringError::NotRegistered("Fan"))), "{refused:?}");
    }

    /// The e-mails of the process report their attempts to the hub of the activity log the HTTP API streams.
    #[tokio::test]
    async fn the_task_handlers_report_their_attempts_to_the_feeds_hub() {
        let wiring = wire(&testing::process().await).unwrap();
        let task = SendETicketV1 {
            booking_id: uuid::Uuid::from_u128(1),
            concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
            seat: SeatId::parse("B4").unwrap(),
            fan_id: FanId::parse("fox").unwrap(),
            fan_name: "Fox".into(),
            price: 80,
        };
        let attempt = TaskAttempt::first(&task, serde_json::json!({})).unwrap();
        let answer = wiring.dispatcher.dispatch(attempt, Duration::from_secs(1)).await;
        assert_eq!(
            answer,
            TaskResult::retry("mail_unreachable"),
            "nobody listens on the test's mail address"
        );
        let kinds: Vec<String> = wiring
            .state
            .activity
            .subscribe()
            .0
            .iter()
            .map(|entry| entry.kind.clone())
            .collect();
        assert_eq!(kinds, ["task.attempt.started", "task.attempt.failed"]);
    }
}
