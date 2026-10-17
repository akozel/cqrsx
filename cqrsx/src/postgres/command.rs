use crate::{
    cqrs::{
        CqrsFramework,
        aggregate::Aggregate,
        command::{CommandExecutor, CommandGatewayError, executor::CqrsCommandExecutor, retry::AggregateConflictRetryPolicy},
        store::persisted::PersistedEventStore,
    },
    postgres::outbox::PostgresEventRepositoryWithOutbox,
};
use sqlx::PgPool;
use std::{collections::HashMap, marker::PhantomData, num::NonZeroUsize};
use thiserror::Error;

/// Checked snapshot interval and optimistic-conflict policy. Does not read env.
#[derive(Clone, Debug, PartialEq)]
pub struct PostgresCommandConfig {
    snapshot_interval: NonZeroUsize,
    conflict_retry: AggregateConflictRetryPolicy,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PostgresCommandConfigError {
    #[error("snapshot interval must be positive")]
    InvalidSnapshotInterval,
}

impl PostgresCommandConfig {
    pub fn new(
        snapshot_interval: usize,
        conflict_retry: AggregateConflictRetryPolicy,
    ) -> Result<Self, PostgresCommandConfigError> {
        Ok(Self {
            snapshot_interval: NonZeroUsize::new(snapshot_interval).ok_or(PostgresCommandConfigError::InvalidSnapshotInterval)?,
            conflict_retry,
        })
    }

    pub fn snapshot_interval(&self) -> usize {
        self.snapshot_interval.get()
    }

    pub fn conflict_retry(&self) -> AggregateConflictRetryPolicy {
        self.conflict_retry
    }
}

impl Default for PostgresCommandConfig {
    fn default() -> Self {
        Self::new(25, AggregateConflictRetryPolicy::default()).expect("valid default PostgreSQL command configuration")
    }
}

/// Assemble a snapshot-backed command executor with unit services and no event_handlers.
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
/// For custom stores, services or event_handlers, use
/// [`CqrsCommandExecutor::new`] with a framework factory instead.
pub fn command_executor<A>(pool: PgPool, config: PostgresCommandConfig) -> PostgresCommandExecutor<A>
where
    A: Aggregate<Services = ()> + Clone + 'static,
    A::Command: Clone + Send + 'static,
    A::Error: Send + 'static,
{
    PostgresCommandExecutor::<A>::new(pool, config)
}

/// Named PostgreSQL adapter sharing the pool and immutable command configuration.
/// Each invocation builds one local CQRS engine; it never retains call-specific tasks.
pub struct PostgresCommandExecutor<A> {
    pool: PgPool,
    config: PostgresCommandConfig,
    aggregate: PhantomData<fn() -> A>,
}

impl<A> PostgresCommandExecutor<A> {
    /// Stores the supplied configuration without I/O or environment access.
    pub fn new(pool: PgPool, config: PostgresCommandConfig) -> Self {
        Self {
            pool,
            config,
            aggregate: PhantomData,
        }
    }
}

impl<A> PostgresCommandExecutor<A>
where
    A: Aggregate<Services = ()> + Clone + 'static,
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

    /// Execute through the common engine, retrying only optimistic conflicts.
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
                CqrsFramework::new(store, Vec::new(), ())
            },
            self.config.conflict_retry(),
        );
        executor.execute_with_metadata(aggregate_id, command, metadata, tasks).await
    }
}

impl<A> CommandExecutor<A> for PostgresCommandExecutor<A>
where
    A: Aggregate<Services = ()> + Clone + 'static,
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
    use std::time::Duration;

    #[test]
    fn config_defaults_overrides_and_zero_interval_are_checked() {
        let defaults = PostgresCommandConfig::default();
        assert_eq!(defaults.snapshot_interval(), 25);
        assert_eq!(defaults.conflict_retry(), AggregateConflictRetryPolicy::default());
        let retry = AggregateConflictRetryPolicy::new(1, Duration::ZERO, Duration::ZERO, 0.0).unwrap();
        let custom = PostgresCommandConfig::new(2, retry).unwrap();
        assert_eq!(custom.snapshot_interval(), 2);
        assert_eq!(custom.conflict_retry(), retry);
        assert_eq!(
            PostgresCommandConfig::new(0, retry),
            Err(PostgresCommandConfigError::InvalidSnapshotInterval)
        );
    }
}
