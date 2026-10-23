use crate::{
    cqrs::{
        CqrsFramework,
        aggregate::Aggregate,
        command::{
            CommandExecutor, CommandGatewayError,
            executor::{CqrsCommandExecutor, FrameworkFactory},
            locks::LockLimits,
            retry::AggregateConflictRetryPolicy,
        },
        event_handler::{
            EventHandler,
            delivery::{DeliveryBacklog, DeliveryConfig},
        },
        store::persisted::PersistedEventStore,
    },
    postgres::outbox::PostgresEventRepositoryWithOutbox,
    task::NewTask,
};
use sqlx::PgPool;
use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};
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

/// Builds the framework of one command: a repository on the shared pool bound to the command's outbox
/// tasks, a snapshot-backed store over it and a clone of the services.
struct PostgresFrameworkFactory<A: Aggregate> {
    pool: PgPool,
    snapshot_interval: usize,
    services: A::Services,
}

impl<A> FrameworkFactory<A> for PostgresFrameworkFactory<A>
where
    A: Aggregate,
    A::Services: Clone,
{
    type Store = PersistedEventStore<PostgresEventRepositoryWithOutbox, A>;

    fn framework(&self, tasks: Vec<NewTask>) -> CqrsFramework<A, Self::Store> {
        let repository = PostgresEventRepositoryWithOutbox::new(self.pool.clone()).with_outbox(tasks);
        let store = PersistedEventStore::new_snapshot_store(repository, self.snapshot_interval);
        CqrsFramework::new(store, self.services.clone())
    }
}

/// The command executor over PostgreSQL: a [`CqrsCommandExecutor`] whose factory binds every command's
/// outbox tasks to a fresh repository on the shared pool, with a snapshot-backed store and a clone of the
/// services. One per aggregate type and process: it owns the aggregate locks and the handlers' buffers, so
/// share it (e.g. in an `Arc`) instead of building a second one. Commands run through [`CommandExecutor`].
///
/// Construction performs no I/O. Accepted event, optional snapshot and explicit tasks commit atomically; at
/// most one event is supported per command. Connection/timeout failures may have unknown commit outcomes
/// and are not automatically retried. Command handling uses a working clone of committed state; snapshot
/// creation applies accepted events to the original committed context, preserving append and increment
/// semantics without requiring repeatable event application.
///
/// For custom stores, use [`CqrsCommandExecutor::new`] with a framework factory instead.
pub struct PostgresCommandExecutor<A: Aggregate> {
    inner: CqrsCommandExecutor<A, PostgresFrameworkFactory<A>>,
}

impl<A: Aggregate + 'static> PostgresCommandExecutor<A> {
    /// Stores the pool, the configuration and the services without I/O or environment access: the conflict
    /// retry policy and the lock limits go to the executor, the snapshot interval to every command's store.
    pub fn new(pool: PgPool, config: PostgresCommandConfig, services: A::Services) -> Self {
        let factory = PostgresFrameworkFactory {
            pool,
            snapshot_interval: config.snapshot_interval(),
            services,
        };
        Self {
            inner: CqrsCommandExecutor::new(factory, config.conflict_retry(), config.lock_limits()),
        }
    }

    /// Registers a handler after the handlers registered before it; it gets the committed events through
    /// a buffer per aggregate key bounded by `config`. One instance serves every command; its index in
    /// post-commit failures is its position. Register every handler before the executor is shared.
    pub fn with_event_handler(self, handler: Arc<dyn EventHandler<A>>, config: DeliveryConfig) -> Self {
        Self {
            inner: self.inner.with_event_handler(handler, config),
        }
    }

    /// The hand-overs of this executor not handled yet; empty without handlers. Take it before the
    /// executor is moved away, to wait for the handlers before maintenance or shutdown.
    pub fn backlog(&self) -> DeliveryBacklog {
        self.inner.backlog()
    }
}

impl<A> CommandExecutor<A> for PostgresCommandExecutor<A>
where
    A: Aggregate + Clone + 'static,
    A::Services: Clone,
    A::Command: Clone + Send + 'static,
    A::Error: Send + 'static,
{
    /// Runs the command under the aggregate's lock with conflict retry (a refused lock, or no space for a
    /// handler before the lock's deadline, is `Unavailable`); `metadata` is stored with the committed event.
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

    struct Ignore;

    #[async_trait::async_trait]
    impl EventHandler<Quiet> for Ignore {
        async fn handle(
            &self,
            _: &str,
            _: &[crate::cqrs::event::EventEnvelope<Quiet>],
        ) -> Result<(), crate::cqrs::store::PersistenceError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn handlers_register_and_share_one_backlog() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://nobody@localhost/none")
            .unwrap();
        let config = PostgresCommandConfig::new(1, AggregateConflictRetryPolicy::default(), LockLimits::default()).unwrap();
        let executor = PostgresCommandExecutor::<Quiet>::new(pool, config, ())
            .with_event_handler(Arc::new(Ignore), DeliveryConfig::default())
            .with_event_handler(Arc::new(Ignore), DeliveryConfig::new(1, 1).unwrap());
        assert!(executor.backlog().is_empty());
        assert_eq!(executor.inner.lock_limits(), LockLimits::default());
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
