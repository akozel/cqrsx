use crate::{
    cqrs::{
        CommitReport, CqrsFramework,
        aggregate::Aggregate,
        command::{
            CommandExecutor, CommandGatewayError, executor::CommandExecutorCore, locks::LockLimits, observer::CommandObserver,
            planner::TaskPlanner, retry::AggregateConflictRetryPolicy, wakeup::CommitWakeup,
        },
        store::persisted::PersistedEventStore,
    },
    postgres::outbox::PostgresEventRepositoryWithOutbox,
};
use sqlx::PgPool;
use std::{collections::HashMap, fmt, num::NonZeroUsize, sync::Arc};

/// Snapshot interval, optimistic-conflict policy and aggregate lock limits. Does not read env.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PostgresCommandConfig {
    snapshot_interval: NonZeroUsize,
    conflict_retry: AggregateConflictRetryPolicy,
    lock_limits: LockLimits,
}

impl PostgresCommandConfig {
    /// A snapshot every `snapshot_interval` events (never zero, so nothing is left to check); `lock_limits` bound how
    /// long, and how many, commands wait for one aggregate's lock: pass [`LockLimits::default`] for the library's
    /// bounds.
    pub fn new(snapshot_interval: NonZeroUsize, conflict_retry: AggregateConflictRetryPolicy, lock_limits: LockLimits) -> Self {
        Self {
            snapshot_interval,
            conflict_retry,
            lock_limits,
        }
    }

    pub fn snapshot_interval(&self) -> NonZeroUsize {
        self.snapshot_interval
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
#[must_use]
pub struct PostgresCommandExecutor<A: Aggregate> {
    inner: CommandExecutorCore<A, PostgresStore<A>>,
}

// By hand: a derive would require `A` to be `Debug`.
impl<A: Aggregate> fmt::Debug for PostgresCommandExecutor<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresCommandExecutor")
            .field("executor", &self.inner)
            .finish()
    }
}

impl<A: Aggregate + 'static> PostgresCommandExecutor<A> {
    /// Stores the pool, the configuration and the rules without I/O or environment access: the conflict
    /// retry policy and the lock limits go to the executor, the snapshot interval to its store.
    pub fn new(pool: PgPool, config: PostgresCommandConfig, rules: A::Rules) -> Self {
        let store =
            PersistedEventStore::new(PostgresEventRepositoryWithOutbox::new(pool)).with_snapshots(config.snapshot_interval());
        Self {
            inner: CommandExecutorCore::new(
                CqrsFramework::new(store, rules),
                config.conflict_retry(),
                config.lock_limits(),
            ),
        }
    }

    /// The core this executor wraps: its settings, for a test of an assembly.
    #[cfg(test)]
    pub(crate) fn core(&self) -> &CommandExecutorCore<A, PostgresStore<A>> {
        &self.inner
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

    /// Tells `observer` about every command once its aggregate's lock is released, before the answer (see
    /// [`CommandObserver`]).
    pub fn with_observer(self, observer: Arc<dyn CommandObserver<A>>) -> Self {
        Self {
            inner: self.inner.with_observer(observer),
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
        id: &A::Id,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, CommandGatewayError<A::Error>> {
        self.inner.execute_with_metadata(id, command, metadata).await
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
        type Id = String;
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
            .connect_lazy("postgres://nobody:s3cret@localhost/none")
            .unwrap();
        let limits = LockLimits::try_new(Duration::from_millis(5), 2).unwrap();
        let config = PostgresCommandConfig::new(NonZeroUsize::MIN, AggregateConflictRetryPolicy::default(), limits);
        let executor = PostgresCommandExecutor::<Quiet>::new(pool.clone(), config, ())
            .with_task_planner(NoTasks)
            .with_commit_wakeup(CommitWakeup::new());
        assert_eq!(executor.inner.lock_limits(), limits);
        let printed = format!("{executor:?}");
        assert!(
            printed.starts_with(
                r#"PostgresCommandExecutor { executor: CommandExecutorCore { framework: CqrsFramework { aggregate_type: "quiet", task_planner: true, .. }"#
            ),
            "{printed}"
        );
        assert!(printed.contains("commit_wakeup: true"), "{printed}");
        let stores = [
            format!(
                "{:?}",
                crate::postgres::checkpoint_store::PostgresCheckpointStore::new(pool.clone())
            ),
            format!("{:?}", crate::postgres::task_queue::PostgresTaskQueue::new(pool.clone())),
            format!("{:?}", PostgresEventRepositoryWithOutbox::new(pool)),
        ];
        for printed in stores {
            assert!(printed.contains("pool: Pool {") && !printed.contains("s3cret"), "{printed}");
        }
    }

    /// The interval is a `NonZeroUsize`: a zero interval, which made every commit divide by zero, does not compile.
    #[test]
    fn config_keeps_its_values_and_its_interval_is_never_zero() {
        let retry = AggregateConflictRetryPolicy::try_new(1, Duration::ZERO, Duration::ZERO, 0.0).unwrap();
        let limits = LockLimits::try_new(Duration::from_millis(5), 2).unwrap();
        let every_two = NonZeroUsize::new(2).unwrap();
        let config = PostgresCommandConfig::new(every_two, retry, limits);
        assert_eq!(config.snapshot_interval(), every_two);
        assert_eq!(config.conflict_retry(), retry);
        assert_eq!(config.lock_limits(), limits);
    }

    /// Notes the outcome of every command.
    #[derive(Default)]
    struct Heard(std::sync::Mutex<Vec<&'static str>>);

    impl CommandObserver<Quiet> for Heard {
        fn command(&self, report: &crate::cqrs::command::observer::CommandReport<'_, Quiet>) {
            use crate::cqrs::command::observer::Outcome;
            self.0.lock().unwrap().push(match report.outcome {
                Outcome::Unavailable => "unavailable",
                Outcome::Accepted { .. } | Outcome::Rejected(_) | Outcome::Conflict => "ran",
            });
        }
    }

    #[tokio::test]
    async fn the_preset_reports_its_commands_to_its_observer() {
        // Nobody listens on port 1: the first acquire fails at once.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgres://nobody@127.0.0.1:1/none")
            .unwrap();
        let config = PostgresCommandConfig::new(
            NonZeroUsize::MIN,
            AggregateConflictRetryPolicy::default(),
            LockLimits::default(),
        );
        let heard = Arc::new(Heard::default());
        let executor = PostgresCommandExecutor::<Quiet>::new(pool, config, ()).with_observer(heard.clone());
        assert_eq!(
            executor.execute(&"quiet".into(), ()).await.map(|report| report.events.len()),
            Err(CommandGatewayError::Unavailable)
        );
        assert_eq!(*heard.0.lock().unwrap(), ["unavailable"]);
    }
}
