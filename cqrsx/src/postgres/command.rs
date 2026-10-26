use crate::{
    cqrs::{
        CqrsFramework,
        aggregate::Aggregate,
        command::{
            CommandExecutor, CommandGatewayError,
            executor::{CommandExecutorCore, FrameworkFactory},
            locks::LockLimits,
            retry::AggregateConflictRetryPolicy,
            wakeup::CommitWakeup,
        },
        store::persisted::PersistedEventStore,
    },
    postgres::outbox::PostgresEventRepositoryWithOutbox,
    task::NewTask,
};
use sqlx::PgPool;
use std::{collections::HashMap, num::NonZeroUsize};
use thiserror::Error;

/// Checked snapshot interval, optimistic-conflict policy and aggregate lock limits. Does not read env.
#[derive(Clone, Debug, PartialEq)]
pub struct PostgresCommandConfig {
    snapshot_interval: NonZeroUsize,
    conflict_retry: AggregateConflictRetryPolicy,
    lock_limits: LockLimits,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum PostgresCommandConfigError {
    #[error("snapshot interval must be positive")]
    InvalidSnapshotInterval,
}

impl PostgresCommandConfig {
    /// `lock_limits` bound how long, and how many, commands wait for one aggregate's lock; pass
    /// [`LockLimits::default`] for the library's bounds.
    pub fn new(
        snapshot_interval: usize,
        conflict_retry: AggregateConflictRetryPolicy,
        lock_limits: LockLimits,
    ) -> Result<Self, PostgresCommandConfigError> {
        Ok(Self {
            snapshot_interval: NonZeroUsize::new(snapshot_interval).ok_or(PostgresCommandConfigError::InvalidSnapshotInterval)?,
            conflict_retry,
            lock_limits,
        })
    }

    pub fn snapshot_interval(&self) -> usize {
        self.snapshot_interval.get()
    }

    pub fn conflict_retry(&self) -> AggregateConflictRetryPolicy {
        self.conflict_retry
    }

    pub fn lock_limits(&self) -> LockLimits {
        self.lock_limits
    }
}

/// Builds the framework of one command: the executor's repository bound to the command's outbox tasks, a
/// snapshot-backed store over it and a clone of the services. The repository is built once, with the
/// executor; a command gets a clone, which shares the pool and the SQL texts.
struct PostgresFrameworkFactory<A: Aggregate> {
    repository: PostgresEventRepositoryWithOutbox,
    snapshot_interval: usize,
    services: A::Services,
}

impl<A: Aggregate> PostgresFrameworkFactory<A> {
    fn repository(&self, tasks: Vec<NewTask>) -> PostgresEventRepositoryWithOutbox {
        self.repository.clone().with_outbox(tasks)
    }
}

impl<A> FrameworkFactory<A> for PostgresFrameworkFactory<A>
where
    A: Aggregate,
    A::Services: Clone,
{
    type Store = PersistedEventStore<PostgresEventRepositoryWithOutbox, A>;

    fn framework(&self, tasks: Vec<NewTask>) -> CqrsFramework<A, Self::Store> {
        let store = PersistedEventStore::new_snapshot_store(self.repository(tasks), self.snapshot_interval);
        CqrsFramework::new(store, self.services.clone())
    }
}

/// The command executor over PostgreSQL: the [`CommandExecutorCore`] with a factory that binds every
/// command's outbox tasks to a clone of the executor's repository (built once: the shared pool and the SQL
/// texts), with a snapshot-backed store and a clone of the services. One per aggregate type and process: it
/// owns the aggregate locks, so share it (e.g. in an
/// `Arc`) instead of building a second one, and never build a core of your own next to it for the same
/// aggregate type. Commands run through [`CommandExecutor`].
///
/// Construction performs no I/O. Accepted event, optional snapshot and explicit tasks commit atomically; at
/// most one event is supported per command. Connection/timeout failures may have unknown commit outcomes
/// and are not automatically retried. Command handling uses a working clone of committed state; snapshot
/// creation applies accepted events to the original committed context, preserving append and increment
/// semantics without requiring repeatable event application.
///
/// For custom stores, use [`CommandExecutorCore::new`] with a framework factory instead.
pub struct PostgresCommandExecutor<A: Aggregate> {
    inner: CommandExecutorCore<A, PostgresFrameworkFactory<A>>,
}

impl<A: Aggregate + 'static> PostgresCommandExecutor<A> {
    /// Stores the pool, the configuration and the services without I/O or environment access: the conflict
    /// retry policy and the lock limits go to the executor, the snapshot interval to every command's store.
    pub fn new(pool: PgPool, config: PostgresCommandConfig, services: A::Services) -> Self {
        let factory = PostgresFrameworkFactory {
            repository: PostgresEventRepositoryWithOutbox::new(pool),
            snapshot_interval: config.snapshot_interval(),
            services,
        };
        Self {
            inner: CommandExecutorCore::new(factory, config.conflict_retry(), config.lock_limits()),
        }
    }

    /// Wakes `wakeup` after every commit with events, for the projection processors of this aggregate type.
    pub fn with_commit_wakeup(self, wakeup: CommitWakeup) -> Self {
        Self {
            inner: self.inner.with_commit_wakeup(wakeup),
        }
    }
}

impl<A> CommandExecutor<A> for PostgresCommandExecutor<A>
where
    A: Aggregate + Clone + 'static,
    A::Services: Clone,
    A::Command: Clone + Send + 'static,
    A::Error: Send + 'static,
{
    /// Runs the command under the aggregate's lock with conflict retry (a refused lock is `Unavailable`);
    /// `metadata` is stored with the committed event.
    async fn execute_with_metadata(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
        tasks: Vec<NewTask>,
    ) -> Result<(), CommandGatewayError<A::Error>> {
        self.inner.execute_with_metadata(aggregate_id, command, metadata, tasks).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
    struct Quiet;

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Said;

    impl crate::cqrs::event::DomainEvent for Said {
        fn event_type(&self) -> String {
            "said".into()
        }

        fn event_version(&self) -> String {
            "1".into()
        }
    }

    impl Aggregate for Quiet {
        const TYPE: &'static str = "quiet";
        type Command = ();
        type Event = Said;
        type Error = std::convert::Infallible;
        type Services = ();

        async fn handle(&mut self, _: (), _: &(), _: &crate::cqrs::event_sink::EventSink<Self>) -> Result<(), Self::Error> {
            Ok(())
        }

        fn apply(&mut self, _: Said) {}
    }

    #[tokio::test]
    async fn every_command_reads_with_the_sql_texts_of_its_executor() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://nobody@localhost/none")
            .unwrap();
        let factory = PostgresFrameworkFactory::<Quiet> {
            repository: PostgresEventRepositoryWithOutbox::new(pool),
            snapshot_interval: 1,
            services: (),
        };
        let (first, second) = (factory.repository(Vec::new()), factory.repository(Vec::new()));
        assert!(first.shares_queries_with(&second));
        assert!(first.shares_queries_with(&factory.repository));
    }

    #[tokio::test]
    async fn the_core_takes_the_lock_limits_of_the_config() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://nobody@localhost/none")
            .unwrap();
        let limits = LockLimits::new(Duration::from_millis(5), 2).unwrap();
        let config = PostgresCommandConfig::new(1, AggregateConflictRetryPolicy::default(), limits).unwrap();
        let executor = PostgresCommandExecutor::<Quiet>::new(pool, config, ()).with_commit_wakeup(CommitWakeup::new());
        assert_eq!(executor.inner.lock_limits(), limits);
    }

    #[test]
    fn config_keeps_its_values_and_checks_the_interval() {
        let retry = AggregateConflictRetryPolicy::new(1, Duration::ZERO, Duration::ZERO, 0.0).unwrap();
        let limits = LockLimits::new(Duration::from_millis(5), 2).unwrap();
        let config = PostgresCommandConfig::new(2, retry, limits).unwrap();
        assert_eq!(config.snapshot_interval(), 2);
        assert_eq!(config.conflict_retry(), retry);
        assert_eq!(config.lock_limits(), limits);
        assert_eq!(
            PostgresCommandConfig::new(0, retry, limits),
            Err(PostgresCommandConfigError::InvalidSnapshotInterval)
        );
    }
}
