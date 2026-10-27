use crate::{
    cqrs::{
        CommitReport, CqrsFramework,
        aggregate::Aggregate,
        command::{
            CommandExecutor, CommandGatewayError, executor::CommandExecutorCore, locks::LockLimits, planner::TaskPlanner,
            retry::AggregateConflictRetryPolicy, wakeup::CommitWakeup,
        },
        store::persisted::PersistedEventStore,
    },
    postgres::outbox::PostgresEventRepositoryWithOutbox,
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

/// The store every command of a PostgreSQL executor runs against: one outbox repository, built with the
/// executor (the shared pool and the SQL texts), under a snapshot-backed store.
type PostgresStore<A> = PersistedEventStore<PostgresEventRepositoryWithOutbox, A>;

/// The command executor over PostgreSQL: the [`CommandExecutorCore`] over one snapshot-backed store on an
/// outbox repository, built once with the executor, and the rules. One per aggregate type and process: it
/// owns the aggregate locks, so share it (e.g. in an `Arc`) instead of building a second one, and never build
/// a core of your own next to it for the same aggregate type. Commands run through [`CommandExecutor`].
///
/// Construction performs no I/O. The accepted event, an optional snapshot and the tasks the executor's
/// [planner](Self::with_task_planner) planned from the decision commit atomically; a command decides one
/// event. Connection/timeout failures may have unknown commit outcomes and are not
/// automatically retried. A command decides on the committed state; snapshot creation applies
/// the accepted event to the original committed context, preserving append and increment semantics without
/// requiring repeatable event application.
///
/// For custom stores, use [`CommandExecutorCore::new`] with a framework of your own instead.
pub struct PostgresCommandExecutor<A: Aggregate> {
    inner: CommandExecutorCore<A, PostgresStore<A>>,
}

impl<A: Aggregate + 'static> PostgresCommandExecutor<A> {
    /// Stores the pool, the configuration and the rules without I/O or environment access: the conflict
    /// retry policy and the lock limits go to the executor, the snapshot interval to its store.
    pub fn new(pool: PgPool, config: PostgresCommandConfig, rules: A::Rules) -> Self {
        let store =
            PersistedEventStore::new_snapshot_store(PostgresEventRepositoryWithOutbox::new(pool), config.snapshot_interval());
        Self {
            inner: CommandExecutorCore::new(
                CqrsFramework::new(store, rules),
                config.conflict_retry(),
                config.lock_limits(),
            ),
        }
    }

    /// Plans every decision's follow-up tasks through `planner`; they commit in the event's statement.
    pub fn with_task_planner(self, planner: impl TaskPlanner<A> + 'static) -> Self
    where
        A::Command: Clone,
    {
        Self {
            inner: self.inner.with_task_planner(planner),
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
    A::Command: Clone + 'static,
    A::Error: Send + 'static,
{
    /// Runs the command under the aggregate's lock with conflict retry (a refused lock is `Unavailable`);
    /// `metadata` is stored with the committed event.
    async fn execute_with_metadata(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, CommandGatewayError<A::Error>> {
        self.inner.execute_with_metadata(aggregate_id, command, metadata).await
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
        type Rules = ();

        fn apply(&mut self, _: Said) {}
    }

    impl crate::cqrs::command::decide::Decide<Quiet> for () {
        fn decide(self, _: &Quiet, _: &()) -> Result<Said, std::convert::Infallible> {
            Ok(Said)
        }
    }

    struct NoTasks;

    impl TaskPlanner<Quiet> for NoTasks {
        fn plan(
            &self,
            _: &crate::cqrs::command::planner::Decision<'_, Quiet>,
        ) -> Result<Vec<crate::task::NewTask>, crate::cqrs::command::planner::TaskPlanError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn the_core_takes_the_lock_limits_of_the_config() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://nobody@localhost/none")
            .unwrap();
        let limits = LockLimits::new(Duration::from_millis(5), 2).unwrap();
        let config = PostgresCommandConfig::new(1, AggregateConflictRetryPolicy::default(), limits).unwrap();
        let executor = PostgresCommandExecutor::<Quiet>::new(pool, config, ())
            .with_task_planner(NoTasks)
            .with_commit_wakeup(CommitWakeup::new());
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
