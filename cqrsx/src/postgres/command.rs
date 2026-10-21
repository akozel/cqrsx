use crate::{
    cqrs::{
        CqrsFramework,
        aggregate::Aggregate,
        command::{
            CommandExecutor, CommandGatewayError,
            executor::CqrsCommandExecutor,
            retry::AggregateConflictRetryPolicy,
            turns::{AggregateTurns, CommandOrdering},
        },
        event_handler::EventHandler,
        store::persisted::PersistedEventStore,
    },
    postgres::outbox::PostgresEventRepositoryWithOutbox,
};
use sqlx::PgPool;
use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};
use thiserror::Error;

/// Checked snapshot interval, optimistic-conflict policy and command ordering. Does not read env.
#[derive(Clone, Debug, PartialEq)]
pub struct PostgresCommandConfig {
    snapshot_interval: NonZeroUsize,
    conflict_retry: AggregateConflictRetryPolicy,
    ordering: CommandOrdering,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PostgresCommandConfigError {
    #[error("snapshot interval must be positive")]
    InvalidSnapshotInterval,
}

impl PostgresCommandConfig {
    /// `ordering` has no default on purpose: [`CommandOrdering::PerAggregate`] runs the commands to one
    /// aggregate one at a time inside the process, [`CommandOrdering::Concurrent`] lets them race.
    pub fn new(
        snapshot_interval: usize,
        conflict_retry: AggregateConflictRetryPolicy,
        ordering: CommandOrdering,
    ) -> Result<Self, PostgresCommandConfigError> {
        Ok(Self {
            snapshot_interval: NonZeroUsize::new(snapshot_interval).ok_or(PostgresCommandConfigError::InvalidSnapshotInterval)?,
            conflict_retry,
            ordering,
        })
    }

    pub fn snapshot_interval(&self) -> usize {
        self.snapshot_interval.get()
    }

    pub fn conflict_retry(&self) -> AggregateConflictRetryPolicy {
        self.conflict_retry
    }

    pub fn ordering(&self) -> CommandOrdering {
        self.ordering
    }
}

/// Assemble a snapshot-backed command executor with unit services and no event handlers;
/// shorthand for [`PostgresCommandExecutor::new`] with `()` services.
///
/// Construction performs no I/O. Every command binds its immutable task list to
/// a fresh repository/framework; conflict retries reuse that framework and reload
/// state. The pool is shared, never a mutable task buffer. Accepted event, optional
/// snapshot and explicit tasks commit atomically; at most one event is supported
/// per command. Connection/timeout failures may have unknown commit outcomes and
/// are not automatically retried.
///
/// Command handling uses a working clone of committed state. Snapshot creation
/// applies accepted events to the original committed context, preserving append
/// and increment semantics without requiring repeatable event application.
///
/// With [`CommandOrdering::PerAggregate`] the executor's commands to one aggregate
/// wait for each other's turn; see [`PostgresCommandExecutor`].
///
/// For custom stores, use [`CqrsCommandExecutor::new`] with a framework factory instead.
pub fn command_executor<A>(pool: PgPool, config: PostgresCommandConfig) -> PostgresCommandExecutor<A>
where
    A: Aggregate<Services = ()> + Clone + 'static,
    A::Command: Clone + Send + 'static,
    A::Error: Send + 'static,
{
    PostgresCommandExecutor::<A>::new(pool, config, ())
}

/// Named PostgreSQL adapter sharing the pool, immutable command configuration, aggregate
/// services and event handlers. Each invocation builds one local CQRS engine with a clone of
/// the services and the shared handlers; it never retains call-specific tasks.
///
/// With [`CommandOrdering::PerAggregate`] the executor owns one turn registry, shared by all its
/// calls: its commands to one aggregate run one at a time, in arrival order, each holding the turn
/// through its conflict retries and event handlers. Two executors of one aggregate type, like two
/// processes, do not order each other; between them the optimistic check still decides.
pub struct PostgresCommandExecutor<A: Aggregate> {
    pool: PgPool,
    config: PostgresCommandConfig,
    turns: Option<AggregateTurns>,
    services: A::Services,
    event_handlers: Vec<Arc<dyn EventHandler<A>>>,
}

impl<A: Aggregate> PostgresCommandExecutor<A> {
    /// Stores the supplied configuration and services without I/O or environment access.
    pub fn new(pool: PgPool, config: PostgresCommandConfig, services: A::Services) -> Self {
        Self {
            pool,
            turns: config.ordering().turns(),
            config,
            services,
            event_handlers: Vec::new(),
        }
    }

    /// Register a handler invoked after every commit, after the handlers registered before it.
    /// One instance serves every command; its index in post-commit failures is its position.
    pub fn with_event_handler(mut self, handler: Arc<dyn EventHandler<A>>) -> Self {
        self.event_handlers.push(handler);
        self
    }
}

impl<A> PostgresCommandExecutor<A>
where
    A: Aggregate + Clone + 'static,
    A::Services: Clone,
    A::Command: Clone + Send + 'static,
    A::Error: Send + 'static,
{
    /// Execute without event metadata; see [`execute_with_metadata`](Self::execute_with_metadata).
    pub async fn execute(
        &self,
        aggregate_id: &str,
        command: A::Command,
        tasks: Vec<crate::task::NewTask>,
    ) -> Result<(), CommandGatewayError<A::Error>> {
        self.execute_with_metadata(aggregate_id, command, HashMap::new(), tasks).await
    }

    /// Execute through the common engine, retrying only optimistic conflicts, inside the aggregate's
    /// turn when the configuration orders commands per aggregate (a refused turn is `Unavailable`).
    /// `metadata` is stored in the `metadata` column of the committed event.
    pub async fn execute_with_metadata(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
        tasks: Vec<crate::task::NewTask>,
    ) -> Result<(), CommandGatewayError<A::Error>> {
        let snapshot_interval = self.config.snapshot_interval();
        let executor = CqrsCommandExecutor::<A, _>::new(
            |tasks| {
                let repository = PostgresEventRepositoryWithOutbox::new(self.pool.clone()).with_outbox(tasks);
                let store = PersistedEventStore::new_snapshot_store(repository, snapshot_interval);
                let handlers = self
                    .event_handlers
                    .iter()
                    .map(|handler| Box::new(handler.clone()) as Box<dyn EventHandler<A>>)
                    .collect();
                CqrsFramework::new(store, handlers, self.services.clone())
            },
            self.config.conflict_retry(),
        );
        let executor = match &self.turns {
            Some(turns) => executor.with_turns(turns.clone()),
            None => executor,
        };
        executor.execute_with_metadata(aggregate_id, command, metadata, tasks).await
    }
}

impl<A> CommandExecutor<A> for PostgresCommandExecutor<A>
where
    A: Aggregate + Clone + 'static,
    A::Services: Clone,
    A::Command: Clone + Send + 'static,
    A::Error: Send + 'static,
{
    async fn execute_with_metadata(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
        tasks: Vec<crate::task::NewTask>,
    ) -> Result<(), CommandGatewayError<A::Error>> {
        PostgresCommandExecutor::execute_with_metadata(self, aggregate_id, command, metadata, tasks).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cqrs::command::turns::TurnLimits;
    use std::time::Duration;

    #[test]
    fn config_keeps_its_values_and_checks_the_interval() {
        let retry = AggregateConflictRetryPolicy::new(1, Duration::ZERO, Duration::ZERO, 0.0).unwrap();
        let concurrent = PostgresCommandConfig::new(2, retry, CommandOrdering::Concurrent).unwrap();
        assert_eq!(concurrent.snapshot_interval(), 2);
        assert_eq!(concurrent.conflict_retry(), retry);
        assert_eq!(concurrent.ordering(), CommandOrdering::Concurrent);
        let turns = CommandOrdering::PerAggregate(TurnLimits::new(Duration::from_millis(5), 2).unwrap());
        assert_eq!(PostgresCommandConfig::new(25, retry, turns).unwrap().ordering(), turns);
        assert_eq!(
            PostgresCommandConfig::new(0, retry, turns),
            Err(PostgresCommandConfigError::InvalidSnapshotInterval)
        );
    }
}
