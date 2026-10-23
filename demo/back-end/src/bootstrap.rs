//! Composition root: what the box office is made of, built from a [`Process`] without I/O, so the
//! binary and the router tests share one graph. Each registry below is the one place to add a
//! component of its kind:
//!
//! - [`aggregates`]: one module per aggregate: its command executor and the event handlers it feeds;
//! - [`projections`]: the read models: their collections, repositories, maintenance and query ports;
//! - [`tasks`]: the outbox task handlers;
//! - [`routes`]: the HTTP endpoints.
pub mod aggregates;
pub mod projections;
pub mod routes;
pub mod tasks;

use crate::{
    application::{
        self,
        activity::ActivityHub,
        lock::BoxOfficeLock,
        operations::{rebuild::RebuildService, verify::VerifyService},
        read_models::ReadModels,
    },
    config::Config,
    domain::{concert::Concert, fan::Fan},
    infrastructure::postgres::{HistoryStore, PostgresLedger, event_history},
    process::Process,
};
use cqrsx::{
    cqrs::event_handler::delivery::DeliveryBacklog,
    postgres::command::{PostgresCommandConfigError, PostgresCommandExecutor},
    task::{adapter::boxed::BoxedTaskAdapter, runtime::dispatcher_factory::TaskDispatcherFactory},
};
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

/// The wired application: the HTTP state, the task handlers for the outbox runtime and what the
/// projections still have to apply when the process stops.
pub struct Wiring {
    pub state: AppState,
    pub dispatcher: TaskDispatcherFactory<BoxedTaskAdapter>,
    pub projection_backlogs: Vec<DeliveryBacklog>,
}

#[derive(Debug, thiserror::Error)]
pub enum WiringError {
    #[error("snapshot configuration: {0}")]
    Command(#[from] PostgresCommandConfigError),
    #[error("mail service client: {0}")]
    MailClient(#[from] reqwest::Error),
}

/// Builds the graph in dependency order: the read models, the aggregates that feed them, the
/// operations over the whole ledger, the task handlers.
pub fn wire(process: &Process) -> Result<Wiring, WiringError> {
    let config = process.config.clone();
    let activity = Arc::new(ActivityHub::new(ACTIVITY_CHANNEL_CAPACITY, ACTIVITY_BACKLOG));
    let lock = Arc::new(BoxOfficeLock::new());

    let read = projections::read_models(process);
    let (concerts, concert_backlog) = aggregates::concert::wire(process, &read, &activity, &lock)?;
    let (fans, fan_backlog) = aggregates::fan::wire(process, &read, &activity, &lock)?;
    let projection_backlogs = vec![concert_backlog, fan_backlog];

    // Operations over the whole ledger: Encore! (rebuild), verify and the time machine.
    let rebuild = Arc::new(RebuildService::new(
        lock.clone(),
        read.maintenance.clone(),
        projection_backlogs.clone(),
        Arc::new(PostgresLedger::new(process.history_pool.clone())),
        activity.clone(),
        config.replay_slow_delay,
    ));
    let verify = Arc::new(VerifyService::new(read.maintenance));
    let time_travel = Arc::new(TimeTravel::new(event_history(process.history_pool.clone())));

    let dispatcher = tasks::dispatcher(process, fans, activity.clone())?;

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
        projection_backlogs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing;
    use cqrsx::cqrs::event_handler::delivery::DeliveryConfig;

    #[tokio::test]
    async fn ordered_projections_are_wired_with_one_backlog_each() {
        let mut config = testing::config();
        config.projection_delivery = DeliveryConfig::new(8, 1).unwrap();
        let wiring = wire(&testing::process_with(config).await).unwrap();
        assert_eq!(wiring.projection_backlogs.len(), 2, "one per projection");
        assert!(wiring.projection_backlogs.iter().all(DeliveryBacklog::is_empty));
    }
}
