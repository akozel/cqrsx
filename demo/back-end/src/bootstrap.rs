//! Composition root: what the box office is made of, built from a [`Process`] without I/O, so the
//! binary and the router tests share one graph. Each registry below is the one place to add a
//! component of its kind:
//!
//! - [`aggregates`]: one module per aggregate: its command executor, which wakes its read model's processor;
//! - [`projections`]: the read models: their collections, processors, verification and query ports;
//! - [`tasks`]: the outbox tasks: the policy of every task type and its handler;
//! - [`routes`]: the HTTP endpoints.
//!
//! [`read_models`] and [`ledger`] are what only the composition knows of every module: the read models the HTTP
//! queries use and the journals of every aggregate type.
pub mod aggregates;
pub mod ledger;
pub mod projections;
pub mod read_models;
pub mod routes;
pub mod tasks;

use crate::{
    bootstrap::{ledger::PostgresLedger, read_models::ReadModels},
    concert::domain::Concert,
    config::Config,
    fan::domain::Fan,
    operations::application::{
        rebuild::{ReadSide, ReadSideLimits, RebuildService},
        verify::VerifyService,
    },
    process::Process,
    shared::{
        application::activity::ActivityHub,
        infrastructure::postgres::{HistoryStore, event_history},
    },
};
use cqrsx::{
    cqrs::projection::runtime::ProjectionRuntime,
    postgres::command::{PostgresCommandConfigError, PostgresCommandExecutor},
    task::{adapter::boxed::BoxedTaskAdapter, policy::PolicyError, runtime::dispatcher_factory::TaskDispatcherFactory},
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Broadcast capacity of the activity log: a slow SSE client lags (and skips) past this many
/// entries, it never blocks a publisher.
pub const ACTIVITY_CHANNEL_CAPACITY: usize = 1024;
/// How many recent entries a new SSE subscriber receives in its `hello`.
pub const ACTIVITY_BACKLOG: usize = 100;

/// The box office over PostgreSQL.
pub type ConcertApp = crate::concert::application::app::ConcertApp<PostgresCommandExecutor<Concert>>;
/// The fan ledger over PostgreSQL.
pub type FanApp = crate::fan::application::app::FanApp<PostgresCommandExecutor<Fan>>;
/// The time machine over the PostgreSQL history, without snapshots.
pub type TimeTravel = crate::concert::application::time_travel::TimeTravel<HistoryStore<Concert>>;

/// Everything the HTTP handlers use.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub concerts: Arc<ConcertApp>,
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

/// The wired application: the HTTP state, the task handlers for the outbox runtime and the projection
/// processors that feed the read models from the journal.
pub struct Wiring {
    pub state: AppState,
    pub dispatcher: TaskDispatcherFactory<BoxedTaskAdapter>,
    pub projections: ProjectionRuntime,
}

#[derive(Debug, thiserror::Error)]
pub enum WiringError {
    #[error("snapshot configuration: {0}")]
    Command(#[from] PostgresCommandConfigError),
    #[error("task policies: {0}")]
    Policy(#[from] PolicyError),
    #[error("mail service client: {0}")]
    MailClient(#[from] reqwest::Error),
}

/// Builds the graph in dependency order: the read side (processors and their wake-ups), the policies of the task
/// types, the aggregates whose commits wake the read side and whose reactions plan with the policies, the operations
/// over the whole ledger, the task handlers.
pub fn wire(process: &Process) -> Result<Wiring, WiringError> {
    let config = process.config.clone();
    let activity = Arc::new(ActivityHub::new(ACTIVITY_CHANNEL_CAPACITY, ACTIVITY_BACKLOG));

    let read = projections::read_models(process, &activity);
    let policies = tasks::policies(&config)?;
    let concerts = aggregates::concert::wire(process, &policies, &read.concert_wakeup, &activity)?;
    let fans = aggregates::fan::wire(process, &policies, &read.fan_wakeup, &activity)?;

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
        projections: read.runtime,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing;

    #[tokio::test]
    async fn the_read_side_is_wired_with_one_processor_per_read_model() {
        let wiring = wire(&testing::process().await).unwrap();
        assert_eq!(wiring.projections.len(), 2, "the concert halls and the fan wallets");
        assert_eq!(projections::checkpoint_keys().len(), 2);
    }
}
