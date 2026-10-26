use super::{
    CommandExecutor, CommandGatewayError,
    locks::{AggregateLockRegistry, LockLimits, LockRefused},
    planner::{Planning, TaskPlanner},
    retry::retry_on_aggregate_conflict,
};
use crate::cqrs::{
    AggregateError, CommitReport, CqrsFramework,
    aggregate::{Aggregate, AggregateKey},
    command::{retry::AggregateConflictRetryPolicy, wakeup::CommitWakeup},
    store::EventStore,
};
use std::{collections::HashMap, error::Error};

/// The core of command execution for one aggregate type, independent of the store: the [`CqrsFramework`] it
/// owns for its life holds the store, the services and the task planner. `postgres::command::PostgresCommandExecutor`
/// and the test executor are this core over a store of their own, not other kinds of executor; build the core
/// yourself only for a store of your own. One long-lived executor per aggregate type and process, shared (e.g. in
/// an `Arc`) by everything that issues its commands: a second executor of the same type, whether a core or a
/// preset, has locks of its own.
///
/// A command first waits for its aggregate's lock, at most the lock's `max_wait`, so the executor's commands
/// to one aggregate run one at a time, in arrival order; past the wait the command is
/// [`CommandGatewayError::Unavailable`] and nothing is loaded, planned or written. Two executors of one
/// aggregate type are like two processes, which meet only in the store's optimistic check. Holding the lock
/// the command runs through the framework with bounded conflict retry: every attempt reloads the aggregate,
/// repeats clones of the command and of its metadata and plans its tasks again from its own decision
/// ([`with_task_planner`](Self::with_task_planner)). A commit with events wakes the aggregate type's projection
/// processors ([`with_commit_wakeup`](Self::with_commit_wakeup)), which read the journal themselves; then the
/// command releases the lock and answers with what it committed.
pub struct CommandExecutorCore<A: Aggregate, S: EventStore<A>> {
    framework: CqrsFramework<A, S>,
    retry_policy: AggregateConflictRetryPolicy,
    locks: AggregateLockRegistry,
    wakeup: Option<CommitWakeup>,
}

impl<A: Aggregate + 'static, S: EventStore<A>> CommandExecutorCore<A, S> {
    /// An executor over `framework` with a lock registry of its own, bounded by `lock_limits`, that wakes nobody.
    pub fn new(framework: CqrsFramework<A, S>, retry_policy: AggregateConflictRetryPolicy, lock_limits: LockLimits) -> Self {
        Self {
            framework,
            retry_policy,
            locks: AggregateLockRegistry::new(lock_limits),
            wakeup: None,
        }
    }

    /// Wakes `wakeup` after every commit with events, before the client gets its answer: the projection
    /// processors of this aggregate type then read the journal at once instead of at their next poll.
    pub fn with_commit_wakeup(mut self, wakeup: CommitWakeup) -> Self {
        self.set_commit_wakeup(wakeup);
        self
    }

    /// Plans every decision's follow-up tasks through `planner`, on every attempt, under the aggregate's lock
    /// (see [`CqrsFramework::with_task_planner`]).
    pub fn with_task_planner(mut self, planner: impl TaskPlanner<A> + 'static) -> Self
    where
        A::Command: Clone,
    {
        self.set_task_planner(Planning::new(planner));
        self
    }

    pub(crate) fn set_commit_wakeup(&mut self, wakeup: CommitWakeup) {
        self.wakeup = Some(wakeup);
    }

    pub(crate) fn set_task_planner(&mut self, planning: Planning<A>) {
        self.framework.set_task_planner(planning);
    }

    pub(crate) fn framework(&self) -> &CqrsFramework<A, S> {
        &self.framework
    }

    #[cfg(all(test, feature = "postgres"))]
    pub(crate) fn lock_limits(&self) -> LockLimits {
        self.locks.limits()
    }
}

impl<A, S> CommandExecutorCore<A, S>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone + Send,
    S: EventStore<A> + 'static,
    S::AC: Send,
{
    /// Executes without event metadata; see [`execute_with_metadata`](Self::execute_with_metadata).
    pub async fn execute(
        &self,
        aggregate_id: &str,
        command: A::Command,
    ) -> Result<CommitReport<A>, CommandGatewayError<A::Error>> {
        self.execute_with_metadata(aggregate_id, command, HashMap::new()).await
    }

    /// Waits for the aggregate's lock, then runs the command with bounded conflict retry. Each attempt
    /// receives a clone of `metadata`, so a retried command commits exactly the metadata of its first
    /// attempt, and plans its tasks from its own decision.
    pub async fn execute_with_metadata(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, CommandGatewayError<A::Error>> {
        // Held through every attempt and the wake-up.
        let _lock = self
            .locks
            .acquire(AggregateKey::of::<A>(aggregate_id))
            .await
            .map_err(lock_refused::<A::Error>(A::TYPE))?;
        let framework = &self.framework;
        let metadata_ref = &metadata;
        let report = retry_on_aggregate_conflict(self.retry_policy, move || {
            framework.execute_with_metadata(aggregate_id, command.clone(), metadata_ref.clone())
        })
        .await
        .inspect_err(|error| log_aggregate_error(A::TYPE, error))
        .map_err(map_aggregate_error)?;
        if !report.events.is_empty()
            && let Some(wakeup) = &self.wakeup
        {
            wakeup.wake(); // the journal has the events: the processors may read them now
        }
        Ok(report)
    }
}

fn lock_refused<D>(aggregate_type: &'static str) -> impl FnOnce(LockRefused) -> CommandGatewayError<D> {
    move |refused| {
        crate::diagnostics::log_lock_refused(aggregate_type, refused);
        CommandGatewayError::Unavailable
    }
}

/// Logs the technical failures of a load, a plan or a commit; domain rejections and conflicts are outcomes.
fn log_aggregate_error<D: Error>(aggregate_type: &str, error: &AggregateError<D>) {
    let (category, source): (&'static str, &(dyn Error + 'static)) = match error {
        AggregateError::DatabaseConnectionError(source) => ("connection", source.as_ref()),
        AggregateError::DeserializationError(source) => ("deserialization", source.as_ref()),
        AggregateError::UnexpectedError(source) => ("unexpected", source.as_ref()),
        AggregateError::TaskPlan(source) => ("task_plan", source),
        AggregateError::Domain(_) | AggregateError::AggregateConflict => return,
    };
    crate::diagnostics::log_command_failure(aggregate_type, category, source);
}

impl<A, S> CommandExecutor<A> for CommandExecutorCore<A, S>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone + Send + 'static,
    A::Error: Send + 'static,
    S: EventStore<A> + 'static,
    S::AC: Send,
{
    async fn execute_with_metadata(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, CommandGatewayError<A::Error>> {
        CommandExecutorCore::execute_with_metadata(self, aggregate_id, command, metadata).await
    }
}

pub(super) fn map_aggregate_error<D: Error>(error: AggregateError<D>) -> CommandGatewayError<D> {
    match error {
        AggregateError::Domain(error) => CommandGatewayError::Domain(error),
        AggregateError::AggregateConflict => CommandGatewayError::Conflict,
        AggregateError::DatabaseConnectionError(_)
        | AggregateError::DeserializationError(_)
        | AggregateError::UnexpectedError(_)
        | AggregateError::TaskPlan(_) => CommandGatewayError::Unavailable,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use crate::cqrs::event::DomainEvent;
    use crate::cqrs::event::EventEnvelope;
    use crate::cqrs::event_sink::EventSink;
    use crate::cqrs::store::AggregateContext;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::cqrs::AggregateError;
    use crate::cqrs::CqrsFramework;
    use crate::cqrs::aggregate::Aggregate;
    use crate::cqrs::command::{
        causation::Causation,
        locks::LockLimits,
        planner::{Decision, TaskPlanError},
    };
    use crate::cqrs::store::EventStore;
    use crate::task::{NewTask, format::TaskPayload};
    use std::time::Duration;
    use thiserror::Error;

    #[derive(Clone, Debug, Error, Eq, PartialEq)]
    #[error("rejected")]
    struct DummyError;

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum DummyCommand {
        Record(String),
        Reject,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    enum DummyEvent {
        Recorded(String),
    }

    impl DomainEvent for DummyEvent {
        fn event_type(&self) -> String {
            "recorded".to_owned()
        }

        fn event_version(&self) -> String {
            "1".to_owned()
        }
    }

    #[derive(Clone, Default, Serialize, Deserialize)]
    struct DummyAggregate;

    impl Aggregate for DummyAggregate {
        const TYPE: &'static str = "dummy";
        type Command = DummyCommand;
        type Event = DummyEvent;
        type Error = DummyError;
        type Services = Arc<Mutex<Vec<DummyCommand>>>;

        async fn handle(
            &mut self,
            command: Self::Command,
            seen: &Self::Services,
            sink: &EventSink<Self>,
        ) -> Result<(), Self::Error> {
            seen.lock().unwrap().push(command.clone());
            match command {
                DummyCommand::Record(value) => {
                    sink.write(DummyEvent::Recorded(value), self).await;
                    Ok(())
                }
                DummyCommand::Reject => Err(DummyError),
            }
        }

        fn apply(&mut self, _: Self::Event) {}
    }

    struct DummyContext(DummyAggregate);

    impl AggregateContext<DummyAggregate> for DummyContext {
        fn aggregate(&mut self) -> &mut DummyAggregate {
            &mut self.0
        }
    }

    /// Conflicts on its first `conflicts` commits and records the tasks every commit carried.
    #[derive(Clone)]
    struct DummyStore {
        commits: Arc<AtomicUsize>,
        conflicts: usize,
        load_error: Option<fn() -> AggregateError<DummyError>>,
        tasks: Arc<Mutex<Vec<Vec<NewTask>>>>,
    }

    impl DummyStore {
        fn new(conflicts: usize) -> Self {
            Self {
                commits: Arc::default(),
                conflicts,
                load_error: None,
                tasks: Arc::default(),
            }
        }

        fn failing(load_error: fn() -> AggregateError<DummyError>) -> Self {
            Self {
                load_error: Some(load_error),
                ..Self::new(0)
            }
        }
    }

    impl EventStore<DummyAggregate> for DummyStore {
        type AC = DummyContext;

        async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<DummyAggregate>>, AggregateError<DummyError>> {
            Ok(Vec::new())
        }

        async fn load_aggregate(&self, _: &str) -> Result<Self::AC, AggregateError<DummyError>> {
            if let Some(error) = self.load_error {
                return Err(error());
            }
            Ok(DummyContext(DummyAggregate))
        }

        async fn commit(
            &self,
            _: Vec<DummyEvent>,
            _: Self::AC,
            _: HashMap<String, String>,
            tasks: &[NewTask],
        ) -> Result<Vec<EventEnvelope<DummyAggregate>>, AggregateError<DummyError>> {
            self.tasks.lock().unwrap().push(tasks.to_vec());
            if self.commits.fetch_add(1, Ordering::SeqCst) < self.conflicts {
                Err(AggregateError::AggregateConflict)
            } else {
                Ok(Vec::new())
            }
        }
    }

    #[derive(Serialize)]
    struct DummyTask {
        value: String,
    }

    impl TaskPayload for DummyTask {
        const TASK_TYPE: &'static str = "dummy";
        const TASK_VERSION: i32 = 1;
    }

    /// One task per recorded value; refuses `refuse` and panics on `panic`; counts its calls and writes
    /// `plan <value>` into `log` when it has one.
    #[derive(Clone, Default)]
    struct ValuePlanner {
        calls: Arc<AtomicUsize>,
        log: Option<Arc<Mutex<Vec<String>>>>,
    }

    impl TaskPlanner<DummyAggregate> for ValuePlanner {
        fn plan(&self, decision: &Decision<'_, DummyAggregate>) -> Result<Vec<NewTask>, TaskPlanError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut tasks = Vec::new();
            for DummyEvent::Recorded(value) in decision.events {
                match value.as_str() {
                    "refuse" => return Err(TaskPlanError::application("private_refusal")),
                    "panic" => panic!("the planner panics"),
                    _ => {}
                }
                if let Some(log) = &self.log {
                    log.lock().unwrap().push(format!("plan {value}"));
                }
                tasks.push(NewTask::new(DummyTask { value: value.clone() })?);
            }
            Ok(tasks)
        }
    }

    fn no_delay_policy(max_attempts: usize) -> AggregateConflictRetryPolicy {
        AggregateConflictRetryPolicy::new(max_attempts, Duration::ZERO, Duration::ZERO, 0.0).unwrap()
    }

    fn dummy_executor(store: DummyStore, attempts: usize) -> CommandExecutorCore<DummyAggregate, DummyStore> {
        CommandExecutorCore::new(
            CqrsFramework::new(store, Arc::default()),
            no_delay_policy(attempts),
            LockLimits::default(),
        )
    }

    #[derive(Clone, Default, Serialize, Deserialize)]
    struct SendOnlyAggregate;

    impl Aggregate for SendOnlyAggregate {
        const TYPE: &'static str = "send_only";
        type Command = std::cell::Cell<u32>;
        type Event = DummyEvent;
        type Error = DummyError;
        type Services = ();

        async fn handle(
            &mut self,
            command: Self::Command,
            _: &(),
            sink: &crate::cqrs::event_sink::EventSink<Self>,
        ) -> Result<(), DummyError> {
            sink.write(DummyEvent::Recorded(command.get().to_string()), self).await;
            Ok(())
        }

        fn apply(&mut self, _: DummyEvent) {}
    }

    /// Plans one task from the command, read through the shared reference the decision holds.
    struct CommandPlanner;

    impl TaskPlanner<SendOnlyAggregate> for CommandPlanner {
        fn plan(&self, decision: &Decision<'_, SendOnlyAggregate>) -> Result<Vec<NewTask>, TaskPlanError> {
            Ok(vec![NewTask::new(DummyTask {
                value: decision.command.get().to_string(),
            })?])
        }
    }

    #[tokio::test]
    async fn executor_accepts_send_commands_without_requiring_sync() {
        let executor = Arc::new(CommandExecutorCore::new(
            CqrsFramework::new(crate::cqrs::testing::mem_store::MemStore::<SendOnlyAggregate>::default(), ()),
            no_delay_policy(1),
            LockLimits::default(),
        ));
        let report =
            tokio::spawn(async move { CommandExecutor::execute(executor.as_ref(), "send-only", std::cell::Cell::new(7)).await })
                .await
                .unwrap()
                .unwrap();
        assert_eq!(report.events.len(), 1);
        assert!(report.tasks.is_empty(), "no planner, no tasks");
    }

    #[tokio::test]
    async fn a_planner_reads_a_send_only_command_and_the_future_stays_send() {
        let executor = Arc::new(
            CommandExecutorCore::new(
                CqrsFramework::new(crate::cqrs::testing::mem_store::MemStore::<SendOnlyAggregate>::default(), ()),
                no_delay_policy(1),
                LockLimits::default(),
            )
            .with_task_planner(CommandPlanner),
        );
        let report =
            tokio::spawn(async move { CommandExecutor::execute(executor.as_ref(), "send-only", std::cell::Cell::new(7)).await })
                .await
                .unwrap()
                .unwrap();
        assert_eq!(report.tasks.len(), 1);
        assert_eq!(report.tasks[0].payload()["value"], "7");
    }

    struct BorrowingExecutor;

    impl CommandExecutor<SendOnlyAggregate> for BorrowingExecutor {
        async fn execute_with_metadata(
            &self,
            aggregate_id: &str,
            command: std::cell::Cell<u32>,
            metadata: HashMap<String, String>,
        ) -> Result<CommitReport<SendOnlyAggregate>, CommandGatewayError<DummyError>> {
            let address = aggregate_id.as_ptr() as usize;
            command.set(command.get() + 1);
            tokio::task::yield_now().await;
            assert_eq!(aggregate_id, "borrowed-id");
            assert_eq!(aggregate_id.as_ptr() as usize, address);
            assert_eq!(command.get(), 8);
            assert!(metadata.is_empty(), "the default execute must pass an empty map");
            Ok(CommitReport::default())
        }
    }

    #[tokio::test]
    async fn native_command_contract_keeps_a_borrowed_id_and_send_only_command_across_await() {
        fn require_send<F: std::future::Future + Send>(_: &F) {}
        let executor = BorrowingExecutor;
        let aggregate_id = String::from("borrowed-id");
        let future = CommandExecutor::execute(&executor, &aggregate_id, std::cell::Cell::new(7));
        require_send(&future);
        assert_eq!(future.await, Ok(CommitReport::default()));
        assert_eq!(aggregate_id, "borrowed-id");
    }

    /// Records the metadata of every commit and conflicts a configured number of times first.
    #[derive(Clone)]
    struct MetadataStore {
        conflicts: usize,
        commits: Arc<Mutex<Vec<HashMap<String, String>>>>,
    }

    impl EventStore<DummyAggregate> for MetadataStore {
        type AC = DummyContext;

        async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<DummyAggregate>>, AggregateError<DummyError>> {
            Ok(Vec::new())
        }

        async fn load_aggregate(&self, _: &str) -> Result<Self::AC, AggregateError<DummyError>> {
            Ok(DummyContext(DummyAggregate))
        }

        async fn commit(
            &self,
            _: Vec<DummyEvent>,
            _: Self::AC,
            metadata: HashMap<String, String>,
            _: &[NewTask],
        ) -> Result<Vec<EventEnvelope<DummyAggregate>>, AggregateError<DummyError>> {
            let mut commits = self.commits.lock().unwrap();
            commits.push(metadata);
            if commits.len() <= self.conflicts {
                Err(AggregateError::AggregateConflict)
            } else {
                Ok(Vec::new())
            }
        }
    }

    fn metadata_executor(
        conflicts: usize,
        commits: &Arc<Mutex<Vec<HashMap<String, String>>>>,
    ) -> impl CommandExecutor<DummyAggregate> {
        CommandExecutorCore::new(
            CqrsFramework::new(
                MetadataStore {
                    conflicts,
                    commits: commits.clone(),
                },
                Arc::default(),
            ),
            no_delay_policy(3),
            LockLimits::default(),
        )
    }

    #[tokio::test]
    async fn metadata_reaches_the_commit_and_stays_identical_across_conflict_retries() {
        let commits = Arc::new(Mutex::new(Vec::new()));
        let executor = metadata_executor(2, &commits);
        let metadata = HashMap::from([
            ("command_id".to_owned(), "cmd-1".to_owned()),
            ("source".to_owned(), "ui".to_owned()),
        ]);
        executor
            .execute_with_metadata("aggregate", DummyCommand::Record("value".to_owned()), metadata.clone())
            .await
            .unwrap();
        assert_eq!(*commits.lock().unwrap(), vec![metadata.clone(), metadata.clone(), metadata]);
    }

    #[tokio::test]
    async fn execute_without_metadata_commits_an_empty_map_through_both_entry_points() {
        let commits = Arc::new(Mutex::new(Vec::new()));
        let executor = metadata_executor(0, &commits);
        CommandExecutor::execute(&executor, "aggregate", DummyCommand::Record("trait".to_owned()))
            .await
            .unwrap();
        let commits_inherent = Arc::new(Mutex::new(Vec::new()));
        let inherent = CommandExecutorCore::new(
            CqrsFramework::new(
                MetadataStore {
                    conflicts: 0,
                    commits: commits_inherent.clone(),
                },
                Arc::default(),
            ),
            no_delay_policy(1),
            LockLimits::default(),
        );
        inherent
            .execute("aggregate", DummyCommand::Record("inherent".to_owned()))
            .await
            .unwrap();
        assert_eq!(*commits.lock().unwrap(), vec![HashMap::new()]);
        assert_eq!(*commits_inherent.lock().unwrap(), vec![HashMap::new()]);
    }

    #[tokio::test]
    async fn a_retried_command_repeats_the_unchanged_command_and_plans_every_attempt_with_the_same_ids() {
        let store = DummyStore::new(2);
        let commands = Arc::new(Mutex::new(Vec::new()));
        let planner = ValuePlanner::default();
        let executor = CommandExecutorCore::new(
            CqrsFramework::new(store.clone(), commands.clone()),
            no_delay_policy(3),
            LockLimits::default(),
        )
        .with_task_planner(planner.clone());
        let metadata = Causation::root("test").event_metadata();

        let report =
            CommandExecutor::execute_with_metadata(&executor, "aggregate", DummyCommand::Record("same".to_owned()), metadata)
                .await
                .unwrap();

        assert_eq!(store.commits.load(Ordering::SeqCst), 3);
        assert_eq!(
            planner.calls.load(Ordering::SeqCst),
            3,
            "every attempt plans its own decision"
        );
        assert_eq!(*commands.lock().unwrap(), vec![DummyCommand::Record("same".to_owned()); 3]);
        let attempts = store.tasks.lock().unwrap().clone();
        assert_eq!(attempts.len(), 3);
        assert!(
            attempts.iter().all(|tasks| tasks == &attempts[0]),
            "the same decision plans the same tasks, ids included"
        );
        assert_eq!(
            report.tasks, attempts[2],
            "the report holds what the committing attempt published"
        );
    }

    #[tokio::test]
    async fn executor_maps_domain_conflict_and_technical_errors_without_wrong_retries() {
        let domain_executor = dummy_executor(DummyStore::new(0), 3);
        assert_eq!(
            CommandExecutor::execute(&domain_executor, "aggregate", DummyCommand::Reject).await,
            Err(CommandGatewayError::Domain(DummyError))
        );

        let store = DummyStore::new(usize::MAX);
        let conflict_executor = dummy_executor(store.clone(), 3);
        assert_eq!(
            CommandExecutor::execute(&conflict_executor, "aggregate", DummyCommand::Record("value".to_owned())).await,
            Err(CommandGatewayError::Conflict)
        );
        assert_eq!(store.commits.load(Ordering::SeqCst), 3);

        for error in [
            || AggregateError::DatabaseConnectionError(Box::new(io::Error::other("connection"))),
            || AggregateError::DeserializationError(Box::new(io::Error::other("json"))),
            || AggregateError::UnexpectedError(Box::new(io::Error::other("unexpected"))),
        ] {
            let executor = dummy_executor(DummyStore::failing(error), 3);
            assert_eq!(
                CommandExecutor::execute(&executor, "aggregate", DummyCommand::Record("value".to_owned())).await,
                Err(CommandGatewayError::Unavailable)
            );
        }
    }

    #[tokio::test]
    async fn exhausted_conflicts_publish_nothing_and_report_the_conflict() {
        let store = DummyStore::new(usize::MAX);
        let planner = ValuePlanner::default();
        let executor = dummy_executor(store.clone(), 3).with_task_planner(planner.clone());
        assert_eq!(
            executor.execute("aggregate", DummyCommand::Record("value".to_owned())).await,
            Err(CommandGatewayError::Conflict)
        );
        assert_eq!(planner.calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            store.commits.load(Ordering::SeqCst),
            3,
            "every planned attempt reached the store"
        );
    }

    #[tokio::test]
    async fn a_planning_failure_refuses_the_command_before_its_commit_and_is_logged_once_without_details() {
        let (_, log) = crate::diagnostics::test_support::capture(async {
            let store = DummyStore::new(0);
            let planner = ValuePlanner::default();
            let executor = dummy_executor(store.clone(), 3).with_task_planner(planner.clone());
            assert_eq!(
                executor
                    .execute("private_id", DummyCommand::Record("refuse".to_owned()))
                    .await,
                Err(CommandGatewayError::Unavailable)
            );
            assert_eq!(planner.calls.load(Ordering::SeqCst), 1, "a planning failure is never retried");
            assert_eq!(store.commits.load(Ordering::SeqCst), 0, "nothing reached the store");
        })
        .await;
        assert_eq!(log.matches("command execution failed").count(), 1, "{log}");
        assert!(log.contains("category=\"task_plan\""), "{log}");
        assert!(!log.contains("private_"), "{log}");
    }

    #[tokio::test]
    async fn a_planning_failure_after_a_conflict_is_unavailable_and_writes_nothing_more() {
        let store = TallyStore::new(1);
        let executor = CommandExecutorCore::new(
            CqrsFramework::new(store.clone(), ()),
            no_delay_policy(3),
            LockLimits::default(),
        )
        .with_task_planner(TallyPlanner { refuse_above: Some(0) });
        assert_eq!(executor.execute("tally", 5).await, Err(CommandGatewayError::Unavailable));
        assert_eq!(
            store.attempts.lock().unwrap().len(),
            1,
            "the first attempt conflicted; the second one's plan was refused before its commit"
        );
    }

    #[tokio::test]
    async fn executor_logs_technical_failure_once_and_suppresses_domain_and_conflict_failures() {
        let (_, log) = crate::diagnostics::test_support::capture(async {
            for error in [
                || AggregateError::DatabaseConnectionError(Box::new(io::Error::other("private_connection"))),
                || AggregateError::DeserializationError(Box::new(io::Error::other("private_json"))),
                || AggregateError::UnexpectedError(Box::new(io::Error::other("private_unexpected"))),
            ] {
                let executor = dummy_executor(DummyStore::failing(error), 3);
                assert_eq!(
                    executor
                        .execute("private_id@example.com", DummyCommand::Record("private_command".into()))
                        .await,
                    Err(CommandGatewayError::Unavailable)
                );
            }
            let store = DummyStore::new(usize::MAX);
            let executor = dummy_executor(store.clone(), 3);
            assert_eq!(
                executor.execute("private_id", DummyCommand::Reject).await,
                Err(CommandGatewayError::Domain(DummyError))
            );
            assert_eq!(
                executor
                    .execute("private_id", DummyCommand::Record("private_command".into()))
                    .await,
                Err(CommandGatewayError::Conflict)
            );
            assert_eq!(store.commits.load(Ordering::SeqCst), 3);
        })
        .await;
        assert_eq!(log.matches("command execution failed").count(), 3);
        assert!(log.contains("aggregate_type=\"dummy\""));
        for category in ["connection", "deserialization", "unexpected"] {
            assert!(log.contains(category));
        }
        assert!(!log.contains("private_"));
    }

    /// A journal with an optimistic check that yields between load and commit, so concurrent commands
    /// really interleave; `external_conflicts` first commits fail as if another node had written.
    #[derive(Clone, Default)]
    struct VersionedStore {
        journal: Arc<Mutex<Vec<String>>>,
        calls: Arc<Mutex<Vec<String>>>,
        external_conflicts: Arc<AtomicUsize>,
        /// When set, every load takes a permit first: a command holds its lock while its load waits.
        load_gate: Option<Arc<tokio::sync::Semaphore>>,
        /// Loads begun, counted before the gate.
        loads: Arc<AtomicUsize>,
        /// The tasks of every successful commit, with the sequence of its event.
        published: Arc<Mutex<Vec<(usize, NewTask)>>>,
    }

    struct VersionedContext(DummyAggregate, usize);

    impl AggregateContext<DummyAggregate> for VersionedContext {
        fn aggregate(&mut self) -> &mut DummyAggregate {
            &mut self.0
        }
    }

    impl EventStore<DummyAggregate> for VersionedStore {
        type AC = VersionedContext;

        async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<DummyAggregate>>, AggregateError<DummyError>> {
            Ok(Vec::new())
        }

        async fn load_aggregate(&self, _: &str) -> Result<Self::AC, AggregateError<DummyError>> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            if let Some(gate) = &self.load_gate {
                gate.acquire().await.unwrap().forget();
            }
            self.calls.lock().unwrap().push("load".to_owned());
            let sequence = self.journal.lock().unwrap().len();
            tokio::task::yield_now().await;
            Ok(VersionedContext(DummyAggregate, sequence))
        }

        async fn commit(
            &self,
            events: Vec<DummyEvent>,
            context: Self::AC,
            _: HashMap<String, String>,
            tasks: &[NewTask],
        ) -> Result<Vec<EventEnvelope<DummyAggregate>>, AggregateError<DummyError>> {
            let [DummyEvent::Recorded(value)] = &events[..] else {
                panic!("one event per command");
            };
            let mut journal = self.journal.lock().unwrap();
            let external = self
                .external_conflicts
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok();
            if external || journal.len() != context.1 {
                self.calls.lock().unwrap().push(format!("conflict {value}"));
                return Err(AggregateError::AggregateConflict);
            }
            journal.push(value.clone());
            self.calls.lock().unwrap().push(format!("commit {value}"));
            let sequence = journal.len();
            self.published
                .lock()
                .unwrap()
                .extend(tasks.iter().map(|task| (sequence, task.clone())));
            Ok(vec![EventEnvelope {
                aggregate_id: "aggregate".to_owned(),
                sequence,
                payload: events[0].clone(),
                metadata: HashMap::new(),
            }])
        }
    }

    fn versioned_executor(store: &VersionedStore, attempts: usize) -> CommandExecutorCore<DummyAggregate, VersionedStore> {
        versioned_executor_with(store, attempts, LockLimits::default())
    }

    fn versioned_executor_with(
        store: &VersionedStore,
        attempts: usize,
        limits: LockLimits,
    ) -> CommandExecutorCore<DummyAggregate, VersionedStore> {
        CommandExecutorCore::new(
            CqrsFramework::new(store.clone(), Arc::default()),
            no_delay_policy(attempts),
            limits,
        )
    }

    fn record(value: &str) -> DummyCommand {
        DummyCommand::Record(value.to_owned())
    }

    #[tokio::test]
    async fn locks_accept_twenty_parallel_commands_with_a_single_attempt_where_separate_executors_conflict() {
        for shared in [true, false] {
            let store = VersionedStore::default();
            let executor = Arc::new(versioned_executor(&store, 1));
            let mut commands = tokio::task::JoinSet::new();
            for index in 0..20 {
                // An executor of its own per command is like a process per command: nothing orders them.
                let executor = if shared {
                    executor.clone()
                } else {
                    Arc::new(versioned_executor(&store, 1))
                };
                commands.spawn(async move { executor.execute("aggregate", record(&index.to_string())).await });
            }
            let mut conflicts = 0;
            while let Some(result) = commands.join_next().await {
                match result.unwrap() {
                    Ok(_) => {}
                    Err(CommandGatewayError::Conflict) => conflicts += 1,
                    other => panic!("unexpected result {other:?}"),
                }
            }
            let accepted = store.journal.lock().unwrap().len();
            if shared {
                assert_eq!(
                    (accepted, conflicts),
                    (20, 0),
                    "one at a time, every load sees the last commit"
                );
            } else {
                assert_eq!((accepted, conflicts), (1, 19), "the control: all load the same version");
            }
        }
    }

    #[tokio::test]
    async fn a_conflict_retries_inside_the_lock_before_the_next_command_starts() {
        for shared in [true, false] {
            let store = VersionedStore::default();
            store.external_conflicts.store(1, Ordering::SeqCst);
            let executor = Arc::new(versioned_executor(&store, 3));
            let second_executor = if shared {
                executor.clone()
            } else {
                Arc::new(versioned_executor(&store, 3))
            };
            let first = tokio::spawn({
                let executor = executor.clone();
                async move { executor.execute("aggregate", record("a")).await }
            });
            let second = tokio::spawn(async move { second_executor.execute("aggregate", record("b")).await });
            assert!(first.await.unwrap().is_ok() && second.await.unwrap().is_ok());
            let calls = store.calls.lock().unwrap().clone();
            if shared {
                assert_eq!(calls, ["load", "conflict a", "load", "commit a", "load", "commit b"]);
            } else {
                assert_eq!(
                    calls[..2],
                    ["load", "load"],
                    "the control: with an executor of its own `b` loads while `a` is still deciding"
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_planner_runs_inside_the_aggregate_lock_between_the_load_and_the_commit() {
        let store = VersionedStore::default();
        let planner = ValuePlanner {
            log: Some(store.calls.clone()),
            ..ValuePlanner::default()
        };
        let executor = Arc::new(versioned_executor(&store, 1).with_task_planner(planner));
        let mut commands = tokio::task::JoinSet::new();
        for index in 0..20 {
            let executor = executor.clone();
            commands.spawn(async move { executor.execute("aggregate", record(&index.to_string())).await });
        }
        while let Some(result) = commands.join_next().await {
            assert_eq!(result.unwrap().unwrap().tasks.len(), 1);
        }
        let calls = store.calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 60);
        for step in calls.chunks(3) {
            let value = step[1].strip_prefix("plan ").expect("a plan follows every load");
            assert_eq!(
                step,
                ["load".to_owned(), format!("plan {value}"), format!("commit {value}")],
                "no other command of the key loads between a load and its commit: {calls:?}"
            );
        }
        let published = store.published.lock().unwrap().clone();
        let journal = store.journal.lock().unwrap().clone();
        assert_eq!(published.len(), 20);
        for (sequence, task) in published {
            assert_eq!(
                task.payload()["value"],
                journal[sequence - 1],
                "every task goes with its own event"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_executors_publish_only_the_tasks_of_attempts_that_committed() {
        let store = VersionedStore::default();
        let planner = ValuePlanner::default();
        let nodes = [
            Arc::new(versioned_executor(&store, 3).with_task_planner(planner.clone())),
            Arc::new(versioned_executor(&store, 3).with_task_planner(planner.clone())),
        ];
        let mut commands = tokio::task::JoinSet::new();
        for index in 0..20 {
            let node = nodes[index % 2].clone();
            commands.spawn(async move { (index, node.execute("aggregate", record(&index.to_string())).await) });
        }
        let mut reports = Vec::new();
        while let Some(result) = commands.join_next().await {
            match result.unwrap() {
                (index, Ok(report)) => reports.push((index, report)),
                (_, Err(CommandGatewayError::Conflict)) => {}
                (_, other) => panic!("unexpected result {other:?}"),
            }
        }
        let journal = store.journal.lock().unwrap().clone();
        let published = store.published.lock().unwrap().clone();
        assert_eq!(
            published.len(),
            journal.len(),
            "one task per committed event, none of a lost attempt"
        );
        for (sequence, task) in &published {
            assert_eq!(task.payload()["value"], journal[sequence - 1]);
        }
        assert_eq!(reports.len(), journal.len());
        for (index, report) in reports {
            assert_eq!(report.tasks.len(), 1);
            assert_eq!(report.tasks[0].payload()["value"], index.to_string());
            assert_eq!(report.events[0].payload, DummyEvent::Recorded(index.to_string()));
        }
        assert!(
            planner.calls.load(Ordering::SeqCst) >= journal.len(),
            "lost attempts planned too, and published nothing"
        );
    }

    #[tokio::test]
    async fn a_panicking_planner_unwinds_and_releases_the_aggregate_lock() {
        let store = VersionedStore::default();
        let executor = Arc::new(
            versioned_executor_with(&store, 1, LockLimits::new(Duration::from_millis(500), 4).unwrap())
                .with_task_planner(ValuePlanner::default()),
        );
        let panicking = tokio::spawn({
            let executor = executor.clone();
            async move { executor.execute("aggregate", record("panic")).await }
        });
        assert!(panicking.await.unwrap_err().is_panic());
        let report = executor.execute("aggregate", record("after")).await.unwrap();
        assert_eq!(report.tasks.len(), 1, "the lock was released and the next command ran");
        assert_eq!(
            *store.journal.lock().unwrap(),
            ["after"],
            "the panicking command wrote nothing"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_lock_is_unavailable_logged_without_id_and_loads_or_plans_nothing() {
        let load_gate = closed_gate();
        let store = VersionedStore {
            load_gate: Some(load_gate.clone()),
            ..VersionedStore::default()
        };
        let planner = ValuePlanner::default();
        let executor = Arc::new(
            versioned_executor_with(&store, 1, LockLimits::new(Duration::from_millis(100), 1).unwrap())
                .with_task_planner(planner.clone()),
        );
        // The holder keeps the key's lock while its load waits at the gate.
        let holder = tokio::spawn({
            let executor = executor.clone();
            async move { executor.execute("private_id", record("private_holder")).await }
        });
        until(|| store.loads.load(Ordering::SeqCst) == 1).await;
        let (_, log) = crate::diagnostics::test_support::capture_at(tracing::Level::WARN, async {
            let mut waiting = Box::pin(executor.execute("private_id", record("private_waiting")));
            assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(waiting.as_mut().poll(cx).is_pending())).await);
            assert_eq!(
                executor.execute("private_id", record("private_refused")).await,
                Err(CommandGatewayError::Unavailable),
                "the line is full"
            );
            // The clock is paused: the idle runtime advances it to the waiter's deadline.
            assert_eq!(
                waiting.await,
                Err(CommandGatewayError::Unavailable),
                "the lock was not acquired in time"
            );
        })
        .await;
        load_gate.add_permits(1);
        assert_eq!(holder.await.unwrap().unwrap().tasks.len(), 1);
        assert_eq!(
            (store.loads.load(Ordering::SeqCst), planner.calls.load(Ordering::SeqCst)),
            (1, 1),
            "only the holder loaded and planned: a refused command loads, plans and writes nothing"
        );
        assert_eq!(log.matches("command refused before it ran").count(), 2, "{log}");
        assert!(
            log.contains("reason=\"too_many_waiters\"") && log.contains("reason=\"timed_out\""),
            "{log}"
        );
        assert!(log.contains("aggregate_type=\"dummy\""), "{log}");
        assert!(!log.contains("private_"), "{log}");
    }

    #[test]
    fn the_command_future_holding_a_lock_is_send() {
        fn require_send<F: std::future::Future + Send>(_: F) {}
        let executor = versioned_executor(&VersionedStore::default(), 1).with_task_planner(ValuePlanner::default());
        require_send(executor.execute("aggregate", record("a")));
    }

    fn closed_gate() -> Arc<tokio::sync::Semaphore> {
        Arc::new(tokio::sync::Semaphore::new(0))
    }

    async fn until(mut done: impl FnMut() -> bool) {
        for _ in 0..1000 {
            if done() {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("the condition never came true");
    }

    #[tokio::test]
    async fn the_commit_wakeup_fires_after_a_commit_with_events_and_not_otherwise() {
        use std::{future::Future, pin::pin, task::Context, task::Waker};
        let wakeup = CommitWakeup::new();
        let store = VersionedStore::default();
        let executor = versioned_executor(&store, 1).with_commit_wakeup(wakeup.clone());
        let mut woken = pin!(wakeup.wait());
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        executor.execute("aggregate", record("a")).await.unwrap();
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_ready());
        // Neither a rejected command nor one without events wakes anybody.
        let quiet = dummy_executor(DummyStore::new(0), 1).with_commit_wakeup(wakeup.clone());
        let mut silent = pin!(wakeup.wait());
        assert!(silent.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        assert_eq!(
            quiet.execute("aggregate", DummyCommand::Reject).await,
            Err(CommandGatewayError::Domain(DummyError))
        );
        assert_eq!(quiet.execute("aggregate", record("a")).await, Ok(CommitReport::default()));
        assert!(silent.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    }

    #[test]
    fn maps_every_framework_error_category() {
        assert_eq!(
            map_aggregate_error(AggregateError::Domain(DummyError)),
            CommandGatewayError::Domain(DummyError)
        );
        assert_eq!(
            map_aggregate_error::<DummyError>(AggregateError::AggregateConflict),
            CommandGatewayError::Conflict
        );
        for error in [
            AggregateError::DatabaseConnectionError(Box::new(io::Error::other("db"))),
            AggregateError::DeserializationError(Box::new(io::Error::other("json"))),
            AggregateError::UnexpectedError(Box::new(io::Error::other("unexpected"))),
            AggregateError::TaskPlan(TaskPlanError::application("refused")),
        ] {
            assert_eq!(map_aggregate_error::<DummyError>(error), CommandGatewayError::Unavailable);
        }
    }

    /// A running total whose state the planner can see: every load returns what the previous attempts
    /// left, so a retried attempt decides on a fresh state.
    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct Tally(u32);

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Added(u32);

    impl DomainEvent for Added {
        fn event_type(&self) -> String {
            "added".into()
        }
        fn event_version(&self) -> String {
            "1".into()
        }
    }

    impl Aggregate for Tally {
        const TYPE: &'static str = "tally";
        type Command = u32;
        type Event = Added;
        type Error = DummyError;
        type Services = ();

        async fn handle(&mut self, amount: u32, _: &(), sink: &EventSink<Self>) -> Result<(), DummyError> {
            sink.write(Added(amount), self).await;
            Ok(())
        }

        fn apply(&mut self, event: Added) {
            self.0 += event.0;
        }
    }

    struct TallyContext(Tally);

    impl AggregateContext<Tally> for TallyContext {
        fn aggregate(&mut self) -> &mut Tally {
            &mut self.0
        }
    }

    /// Every load starts from 10 more than the last, as if another node committed in between; the first
    /// `conflicts` commits conflict. Records the tasks of every commit attempt.
    #[derive(Clone)]
    struct TallyStore {
        loads: Arc<AtomicUsize>,
        conflicts: usize,
        attempts: Arc<Mutex<Vec<Vec<NewTask>>>>,
    }

    impl TallyStore {
        fn new(conflicts: usize) -> Self {
            Self {
                loads: Arc::default(),
                conflicts,
                attempts: Arc::default(),
            }
        }
    }

    impl EventStore<Tally> for TallyStore {
        type AC = TallyContext;

        async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<Tally>>, AggregateError<DummyError>> {
            Ok(Vec::new())
        }

        async fn load_aggregate(&self, _: &str) -> Result<Self::AC, AggregateError<DummyError>> {
            let loads = self.loads.fetch_add(1, Ordering::SeqCst);
            Ok(TallyContext(Tally(10 * loads as u32)))
        }

        async fn commit(
            &self,
            events: Vec<Added>,
            _: Self::AC,
            metadata: HashMap<String, String>,
            tasks: &[NewTask],
        ) -> Result<Vec<EventEnvelope<Tally>>, AggregateError<DummyError>> {
            let mut attempts = self.attempts.lock().unwrap();
            attempts.push(tasks.to_vec());
            if attempts.len() <= self.conflicts {
                return Err(AggregateError::AggregateConflict);
            }
            Ok(events
                .into_iter()
                .map(|payload| EventEnvelope {
                    aggregate_id: "tally".to_owned(),
                    sequence: attempts.len(),
                    payload,
                    metadata: metadata.clone(),
                })
                .collect())
        }
    }

    #[derive(Serialize)]
    struct Total {
        before: u32,
        after: u32,
    }

    impl TaskPayload for Total {
        const TASK_TYPE: &'static str = "total";
        const TASK_VERSION: i32 = 1;
    }

    /// Plans the state before and after the decision; refuses a decision on a state above `refuse_above`.
    struct TallyPlanner {
        refuse_above: Option<u32>,
    }

    impl TaskPlanner<Tally> for TallyPlanner {
        fn plan(&self, decision: &Decision<'_, Tally>) -> Result<Vec<NewTask>, TaskPlanError> {
            if self.refuse_above.is_some_and(|limit| decision.before.0 > limit) {
                return Err(TaskPlanError::application("too much"));
            }
            Ok(vec![NewTask::new(Total {
                before: decision.before.0,
                after: decision.after.0,
            })?])
        }
    }

    #[tokio::test]
    async fn a_conflict_replans_from_the_fresh_state_and_keeps_the_task_ids() {
        let store = TallyStore::new(2);
        let executor = CommandExecutorCore::new(
            CqrsFramework::new(store.clone(), ()),
            no_delay_policy(3),
            LockLimits::default(),
        )
        .with_task_planner(TallyPlanner { refuse_above: None });
        let report = executor
            .execute_with_metadata("tally", 5, Causation::root("test").event_metadata())
            .await
            .unwrap();
        let attempts = store.attempts.lock().unwrap().clone();
        let totals: Vec<_> = attempts
            .iter()
            .map(|tasks| (tasks[0].payload()["before"].clone(), tasks[0].payload()["after"].clone()))
            .collect();
        assert_eq!(
            totals,
            [
                (serde_json::json!(0), serde_json::json!(5)),
                (serde_json::json!(10), serde_json::json!(15)),
                (serde_json::json!(20), serde_json::json!(25)),
            ],
            "every attempt planned from the state it loaded"
        );
        assert!(
            attempts.iter().all(|tasks| tasks[0].id() == attempts[0][0].id()),
            "the id is the command's, whatever the attempt decided"
        );
        assert_eq!(report.tasks, attempts[2]);
        assert_eq!(report.events.len(), 1);
    }
}
