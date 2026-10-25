use super::{
    CommandExecutor, CommandGatewayError,
    locks::{AggregateLockRegistry, LockLimits, LockRefused},
    retry::retry_on_aggregate_conflict,
};
use crate::{
    cqrs::{
        AggregateError, CqrsFramework,
        aggregate::{Aggregate, AggregateKey},
        command::{retry::AggregateConflictRetryPolicy, wakeup::CommitWakeup},
        store::EventStore,
    },
    task::NewTask,
};
use std::{collections::HashMap, error::Error, marker::PhantomData};

/// Builds the framework one command runs through: the event store, bound to the command's outbox tasks
/// where the store publishes them, and the aggregate's services. Called once per command, before its first
/// attempt; conflict retries reuse the framework, so the tasks cannot change between attempts.
///
/// Every `Fn(Vec<NewTask>) -> CqrsFramework<A, S>` is a factory, which is enough for a custom store; the
/// PostgreSQL and the test executors have factories of their own.
pub trait FrameworkFactory<A: Aggregate>: Send + Sync {
    type Store: EventStore<A>;

    fn framework(&self, tasks: Vec<NewTask>) -> CqrsFramework<A, Self::Store>;
}

impl<A, S, F> FrameworkFactory<A> for F
where
    A: Aggregate,
    S: EventStore<A>,
    F: Fn(Vec<NewTask>) -> CqrsFramework<A, S> + Send + Sync,
{
    type Store = S;

    fn framework(&self, tasks: Vec<NewTask>) -> CqrsFramework<A, S> {
        self(tasks)
    }
}

/// The core of command execution for one aggregate type, independent of the store: the store comes from
/// the [`FrameworkFactory`] `F`. `postgres::command::PostgresCommandExecutor` and the test executor are this
/// core with a factory of their own, not other kinds of executor; build the core yourself only for a store of
/// your own. One long-lived executor per aggregate type and process, shared (e.g. in an `Arc`) by everything
/// that issues its commands: a second executor of the same type, whether a core or a preset, has locks of its
/// own.
///
/// A command first waits for its aggregate's lock, at most the lock's `max_wait`, so the executor's commands
/// to one aggregate run one at a time, in arrival order; past the wait the command is
/// [`CommandGatewayError::Unavailable`] and nothing is built or written. Two executors of one aggregate type
/// are like two processes, which meet only in the store's optimistic check. Holding the lock the command
/// builds its framework through the [`FrameworkFactory`] and commits with bounded conflict retry (every
/// attempt reloads the aggregate through that framework and repeats clones of the command and of its
/// metadata). A commit with events wakes the aggregate type's projection processors
/// ([`with_commit_wakeup`](Self::with_commit_wakeup)), which read the journal themselves; then the command
/// releases the lock and answers.
pub struct CommandExecutorCore<A: Aggregate, F> {
    factory: F,
    retry_policy: AggregateConflictRetryPolicy,
    locks: AggregateLockRegistry,
    wakeup: Option<CommitWakeup>,
    _aggregate: PhantomData<fn() -> A>,
}

impl<A: Aggregate + 'static, F> CommandExecutorCore<A, F> {
    /// An executor with a lock registry of its own, bounded by `lock_limits`, that wakes nobody.
    pub fn new(factory: F, retry_policy: AggregateConflictRetryPolicy, lock_limits: LockLimits) -> Self {
        Self {
            factory,
            retry_policy,
            locks: AggregateLockRegistry::new(lock_limits),
            wakeup: None,
            _aggregate: PhantomData,
        }
    }

    /// Wakes `wakeup` after every commit with events, before the client gets its answer: the projection
    /// processors of this aggregate type then read the journal at once instead of at their next poll.
    pub fn with_commit_wakeup(mut self, wakeup: CommitWakeup) -> Self {
        self.set_commit_wakeup(wakeup);
        self
    }

    pub(crate) fn set_commit_wakeup(&mut self, wakeup: CommitWakeup) {
        self.wakeup = Some(wakeup);
    }

    pub(crate) fn factory(&self) -> &F {
        &self.factory
    }

    #[cfg(all(test, feature = "postgres"))]
    pub(crate) fn lock_limits(&self) -> LockLimits {
        self.locks.limits()
    }
}

impl<A, F> CommandExecutorCore<A, F>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone + Send,
    F: FrameworkFactory<A>,
    F::Store: 'static,
    <F::Store as EventStore<A>>::AC: Send,
{
    /// Executes without event metadata; see [`execute_with_metadata`](Self::execute_with_metadata).
    pub async fn execute(
        &self,
        aggregate_id: &str,
        command: A::Command,
        tasks: Vec<NewTask>,
    ) -> Result<(), CommandGatewayError<A::Error>> {
        self.execute_with_metadata(aggregate_id, command, HashMap::new(), tasks).await
    }

    /// Waits for the aggregate's lock, builds one framework for `tasks`, then runs the command with bounded
    /// conflict retry. Each attempt receives a clone of `metadata`, so a retried command commits exactly the
    /// metadata of its first attempt.
    pub async fn execute_with_metadata(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
        tasks: Vec<NewTask>,
    ) -> Result<(), CommandGatewayError<A::Error>> {
        // Held through every attempt and the wake-up.
        let _lock = self
            .locks
            .acquire(AggregateKey::of::<A>(aggregate_id))
            .await
            .map_err(lock_refused::<A::Error>(A::TYPE))?;
        let framework = self.factory.framework(tasks);
        let framework_ref = &framework;
        let metadata_ref = &metadata;
        let committed = retry_on_aggregate_conflict(self.retry_policy, move || {
            framework_ref.execute_with_metadata(aggregate_id, command.clone(), metadata_ref.clone())
        })
        .await
        .inspect_err(|error| log_aggregate_error(A::TYPE, error))
        .map_err(map_aggregate_error)?;
        if !committed.is_empty()
            && let Some(wakeup) = &self.wakeup
        {
            wakeup.wake(); // the journal has the events: the processors may read them now
        }
        Ok(())
    }
}

fn lock_refused<D>(aggregate_type: &'static str) -> impl FnOnce(LockRefused) -> CommandGatewayError<D> {
    move |refused| {
        crate::diagnostics::log_lock_refused(aggregate_type, refused);
        CommandGatewayError::Unavailable
    }
}

/// Logs the technical failures of a load or commit; domain rejections and conflicts are outcomes.
fn log_aggregate_error<D: Error>(aggregate_type: &str, error: &AggregateError<D>) {
    let (category, source) = match error {
        AggregateError::DatabaseConnectionError(source) => ("connection", source),
        AggregateError::DeserializationError(source) => ("deserialization", source),
        AggregateError::UnexpectedError(source) => ("unexpected", source),
        AggregateError::Domain(_) | AggregateError::AggregateConflict => return,
    };
    crate::diagnostics::log_command_failure(aggregate_type, category, source.as_ref());
}

impl<A, F> CommandExecutor<A> for CommandExecutorCore<A, F>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone + Send + 'static,
    A::Error: Send + 'static,
    F: FrameworkFactory<A>,
    F::Store: 'static,
    <F::Store as EventStore<A>>::AC: Send,
{
    async fn execute_with_metadata(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
        tasks: Vec<NewTask>,
    ) -> Result<(), CommandGatewayError<A::Error>> {
        CommandExecutorCore::execute_with_metadata(self, aggregate_id, command, metadata, tasks).await
    }
}

pub(super) fn map_aggregate_error<D: Error>(error: AggregateError<D>) -> CommandGatewayError<D> {
    match error {
        AggregateError::Domain(error) => CommandGatewayError::Domain(error),
        AggregateError::AggregateConflict => CommandGatewayError::Conflict,
        AggregateError::DatabaseConnectionError(_)
        | AggregateError::DeserializationError(_)
        | AggregateError::UnexpectedError(_) => CommandGatewayError::Unavailable,
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
    use crate::cqrs::command::locks::LockLimits;
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

    #[derive(Clone)]
    struct DummyStore {
        commits: Arc<AtomicUsize>,
        conflicts: usize,
        load_error: Option<fn() -> AggregateError<DummyError>>,
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
        ) -> Result<Vec<EventEnvelope<DummyAggregate>>, AggregateError<DummyError>> {
            if self.commits.fetch_add(1, Ordering::SeqCst) < self.conflicts {
                Err(AggregateError::AggregateConflict)
            } else {
                Ok(Vec::new())
            }
        }
    }

    #[derive(Serialize)]
    struct DummyTask {
        value: &'static str,
    }

    impl TaskPayload for DummyTask {
        const TASK_TYPE: &'static str = "dummy";
        const TASK_VERSION: i32 = 1;
    }

    fn no_delay_policy(max_attempts: usize) -> AggregateConflictRetryPolicy {
        AggregateConflictRetryPolicy::new(max_attempts, Duration::ZERO, Duration::ZERO, 0.0).unwrap()
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

    #[tokio::test]
    async fn executor_accepts_send_commands_without_requiring_sync() {
        let executor = Arc::new(CommandExecutorCore::new(
            |_| CqrsFramework::new(crate::cqrs::testing::mem_store::MemStore::<SendOnlyAggregate>::default(), ()),
            no_delay_policy(1),
            LockLimits::default(),
        ));
        let result = tokio::spawn(async move {
            CommandExecutor::execute(executor.as_ref(), "send-only", std::cell::Cell::new(7), vec![]).await
        })
        .await
        .unwrap();
        assert_eq!(result, Ok(()));
    }

    struct BorrowingExecutor;

    impl CommandExecutor<SendOnlyAggregate> for BorrowingExecutor {
        async fn execute_with_metadata(
            &self,
            aggregate_id: &str,
            command: std::cell::Cell<u32>,
            metadata: HashMap<String, String>,
            tasks: Vec<NewTask>,
        ) -> Result<(), CommandGatewayError<DummyError>> {
            let address = aggregate_id.as_ptr() as usize;
            command.set(command.get() + 1);
            tokio::task::yield_now().await;
            assert_eq!(aggregate_id, "borrowed-id");
            assert_eq!(aggregate_id.as_ptr() as usize, address);
            assert_eq!(command.get(), 8);
            assert!(metadata.is_empty(), "the default execute must pass an empty map");
            assert!(tasks.is_empty());
            Ok(())
        }
    }

    #[tokio::test]
    async fn native_command_contract_keeps_a_borrowed_id_and_send_only_command_across_await() {
        fn require_send<F: std::future::Future + Send>(_: &F) {}
        let executor = BorrowingExecutor;
        let aggregate_id = String::from("borrowed-id");
        let future = CommandExecutor::execute(&executor, &aggregate_id, std::cell::Cell::new(7), vec![]);
        require_send(&future);
        future.await.unwrap();
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
        let commits = commits.clone();
        CommandExecutorCore::<DummyAggregate, _>::new(
            move |_| {
                CqrsFramework::new(
                    MetadataStore {
                        conflicts,
                        commits: commits.clone(),
                    },
                    Arc::default(),
                )
            },
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
            .execute_with_metadata(
                "aggregate",
                DummyCommand::Record("value".to_owned()),
                metadata.clone(),
                vec![],
            )
            .await
            .unwrap();
        assert_eq!(*commits.lock().unwrap(), vec![metadata.clone(), metadata.clone(), metadata]);
    }

    #[tokio::test]
    async fn execute_without_metadata_commits_an_empty_map_through_both_entry_points() {
        let commits = Arc::new(Mutex::new(Vec::new()));
        let executor = metadata_executor(0, &commits);
        CommandExecutor::execute(&executor, "aggregate", DummyCommand::Record("trait".to_owned()), vec![])
            .await
            .unwrap();
        let commits_inherent = Arc::new(Mutex::new(Vec::new()));
        let inherent = CommandExecutorCore::<DummyAggregate, _>::new(
            {
                let commits = commits_inherent.clone();
                move |_| {
                    CqrsFramework::new(
                        MetadataStore {
                            conflicts: 0,
                            commits: commits.clone(),
                        },
                        Arc::default(),
                    )
                }
            },
            no_delay_policy(1),
            LockLimits::default(),
        );
        inherent
            .execute("aggregate", DummyCommand::Record("inherent".to_owned()), vec![])
            .await
            .unwrap();
        assert_eq!(*commits.lock().unwrap(), vec![HashMap::new()]);
        assert_eq!(*commits_inherent.lock().unwrap(), vec![HashMap::new()]);
    }

    #[tokio::test]
    async fn executor_builds_once_and_retries_the_unchanged_command_and_tasks() {
        let builds = Arc::new(AtomicUsize::new(0));
        let commits = Arc::new(AtomicUsize::new(0));
        let commands = Arc::new(Mutex::new(Vec::new()));
        let captured_tasks = Arc::new(Mutex::new(Vec::new()));
        let executor = Arc::new(CommandExecutorCore::<DummyAggregate, _>::new(
            {
                let builds = builds.clone();
                let commits = commits.clone();
                let commands = commands.clone();
                let captured_tasks = captured_tasks.clone();
                move |tasks| {
                    builds.fetch_add(1, Ordering::SeqCst);
                    captured_tasks.lock().unwrap().push(tasks);
                    CqrsFramework::new(
                        DummyStore {
                            commits: commits.clone(),
                            conflicts: 2,
                            load_error: None,
                        },
                        commands.clone(),
                    )
                }
            },
            no_delay_policy(3),
            LockLimits::default(),
        ));
        let task = NewTask::new(DummyTask { value: "same" }).unwrap();

        CommandExecutor::execute(
            executor.as_ref(),
            "aggregate",
            DummyCommand::Record("same".to_owned()),
            vec![task.clone()],
        )
        .await
        .unwrap();

        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(commits.load(Ordering::SeqCst), 3);
        assert_eq!(
            *commands.lock().unwrap(),
            vec![
                DummyCommand::Record("same".to_owned()),
                DummyCommand::Record("same".to_owned()),
                DummyCommand::Record("same".to_owned()),
            ]
        );
        assert_eq!(*captured_tasks.lock().unwrap(), vec![vec![task]]);
    }

    #[tokio::test]
    async fn executor_maps_domain_conflict_and_technical_errors_without_wrong_retries() {
        let domain_executor = Arc::new(CommandExecutorCore::<DummyAggregate, _>::new(
            |_| {
                CqrsFramework::new(
                    DummyStore {
                        commits: Arc::default(),
                        conflicts: 0,
                        load_error: None,
                    },
                    Arc::default(),
                )
            },
            no_delay_policy(3),
            LockLimits::default(),
        ));
        assert_eq!(
            CommandExecutor::execute(domain_executor.as_ref(), "aggregate", DummyCommand::Reject, vec![]).await,
            Err(CommandGatewayError::Domain(DummyError))
        );

        let commits = Arc::new(AtomicUsize::new(0));
        let conflict_executor = Arc::new(CommandExecutorCore::<DummyAggregate, _>::new(
            {
                let commits = commits.clone();
                move |_| {
                    CqrsFramework::new(
                        DummyStore {
                            commits: commits.clone(),
                            conflicts: usize::MAX,
                            load_error: None,
                        },
                        Arc::default(),
                    )
                }
            },
            no_delay_policy(3),
            LockLimits::default(),
        ));
        assert_eq!(
            CommandExecutor::execute(
                conflict_executor.as_ref(),
                "aggregate",
                DummyCommand::Record("value".to_owned()),
                vec![]
            )
            .await,
            Err(CommandGatewayError::Conflict)
        );
        assert_eq!(commits.load(Ordering::SeqCst), 3);

        for error in [
            || AggregateError::DatabaseConnectionError(Box::new(io::Error::other("connection"))),
            || AggregateError::DeserializationError(Box::new(io::Error::other("json"))),
            || AggregateError::UnexpectedError(Box::new(io::Error::other("unexpected"))),
        ] {
            let executor = Arc::new(CommandExecutorCore::<DummyAggregate, _>::new(
                move |_| {
                    CqrsFramework::new(
                        DummyStore {
                            commits: Arc::default(),
                            conflicts: 0,
                            load_error: Some(error),
                        },
                        Arc::default(),
                    )
                },
                no_delay_policy(3),
                LockLimits::default(),
            ));
            assert_eq!(
                CommandExecutor::execute(
                    executor.as_ref(),
                    "aggregate",
                    DummyCommand::Record("value".to_owned()),
                    vec![]
                )
                .await,
                Err(CommandGatewayError::Unavailable)
            );
        }
    }

    #[tokio::test]
    async fn executor_logs_technical_failure_once_and_suppresses_domain_and_conflict_failures() {
        let (_, log) = crate::diagnostics::test_support::capture(async {
            for error in [
                || AggregateError::DatabaseConnectionError(Box::new(io::Error::other("private_connection"))),
                || AggregateError::DeserializationError(Box::new(io::Error::other("private_json"))),
                || AggregateError::UnexpectedError(Box::new(io::Error::other("private_unexpected"))),
            ] {
                let executor = CommandExecutorCore::<DummyAggregate, _>::new(
                    move |_| {
                        CqrsFramework::new(
                            DummyStore {
                                commits: Arc::default(),
                                conflicts: 0,
                                load_error: Some(error),
                            },
                            Arc::default(),
                        )
                    },
                    no_delay_policy(3),
                    LockLimits::default(),
                );
                assert_eq!(
                    executor
                        .execute(
                            "private_id@example.com",
                            DummyCommand::Record("private_command".into()),
                            vec![]
                        )
                        .await,
                    Err(CommandGatewayError::Unavailable)
                );
            }
            let commits = Arc::new(AtomicUsize::new(0));
            let count = commits.clone();
            let executor = CommandExecutorCore::<DummyAggregate, _>::new(
                move |_| {
                    CqrsFramework::new(
                        DummyStore {
                            commits: count.clone(),
                            conflicts: usize::MAX,
                            load_error: None,
                        },
                        Arc::default(),
                    )
                },
                no_delay_policy(3),
                LockLimits::default(),
            );
            assert_eq!(
                executor.execute("private_id", DummyCommand::Reject, vec![]).await,
                Err(CommandGatewayError::Domain(DummyError))
            );
            assert_eq!(
                executor
                    .execute("private_id", DummyCommand::Record("private_command".into()), vec![])
                    .await,
                Err(CommandGatewayError::Conflict)
            );
            assert_eq!(commits.load(Ordering::SeqCst), 3);
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
            Ok(vec![EventEnvelope {
                aggregate_id: "aggregate".to_owned(),
                sequence: journal.len(),
                payload: events[0].clone(),
                metadata: HashMap::new(),
            }])
        }
    }

    type VersionedFramework = CqrsFramework<DummyAggregate, VersionedStore>;

    fn versioned_executor(
        store: &VersionedStore,
        attempts: usize,
    ) -> CommandExecutorCore<DummyAggregate, impl Fn(Vec<NewTask>) -> VersionedFramework + Send + Sync + use<>> {
        versioned_executor_with(store, attempts, LockLimits::default())
    }

    fn versioned_executor_with(
        store: &VersionedStore,
        attempts: usize,
        limits: LockLimits,
    ) -> CommandExecutorCore<DummyAggregate, impl Fn(Vec<NewTask>) -> VersionedFramework + Send + Sync + use<>> {
        let store = store.clone();
        CommandExecutorCore::new(
            move |_| CqrsFramework::new(store.clone(), Arc::default()),
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
                commands.spawn(async move { executor.execute("aggregate", record(&index.to_string()), vec![]).await });
            }
            let mut conflicts = 0;
            while let Some(result) = commands.join_next().await {
                match result.unwrap() {
                    Ok(()) => {}
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
                async move { executor.execute("aggregate", record("a"), vec![]).await }
            });
            let second = tokio::spawn(async move { second_executor.execute("aggregate", record("b"), vec![]).await });
            assert_eq!((first.await.unwrap(), second.await.unwrap()), (Ok(()), Ok(())));
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

    #[tokio::test(start_paused = true)]
    async fn a_refused_lock_is_unavailable_logged_without_id_and_builds_nothing() {
        let builds = Arc::new(AtomicUsize::new(0));
        let load_gate = closed_gate();
        let store = VersionedStore {
            load_gate: Some(load_gate.clone()),
            ..VersionedStore::default()
        };
        let executor = Arc::new(CommandExecutorCore::<DummyAggregate, _>::new(
            {
                let builds = builds.clone();
                move |_| {
                    builds.fetch_add(1, Ordering::SeqCst);
                    CqrsFramework::new(store.clone(), Arc::default())
                }
            },
            no_delay_policy(1),
            LockLimits::new(Duration::from_millis(100), 1).unwrap(),
        ));
        // The holder keeps the key's lock while its load waits at the gate.
        let holder = tokio::spawn({
            let executor = executor.clone();
            async move { executor.execute("private_id", record("private_holder"), vec![]).await }
        });
        until(|| builds.load(Ordering::SeqCst) == 1).await;
        let (_, log) = crate::diagnostics::test_support::capture_at(tracing::Level::WARN, async {
            let mut waiting = Box::pin(executor.execute("private_id", record("private_waiting"), vec![]));
            assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(waiting.as_mut().poll(cx).is_pending())).await);
            assert_eq!(
                executor.execute("private_id", record("private_refused"), vec![]).await,
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
        assert_eq!(holder.await.unwrap(), Ok(()));
        assert_eq!(
            builds.load(Ordering::SeqCst),
            1,
            "only the holder built a framework: a refused command builds and writes nothing"
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
        let executor = versioned_executor(&VersionedStore::default(), 1);
        require_send(executor.execute("aggregate", record("a"), vec![]));
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
        executor.execute("aggregate", record("a"), vec![]).await.unwrap();
        assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_ready());
        // Neither a rejected command nor one without events wakes anybody.
        let quiet = CommandExecutorCore::<DummyAggregate, _>::new(
            |_| {
                CqrsFramework::new(
                    DummyStore {
                        commits: Arc::default(),
                        conflicts: 0,
                        load_error: None,
                    },
                    Arc::default(),
                )
            },
            no_delay_policy(1),
            LockLimits::default(),
        )
        .with_commit_wakeup(wakeup.clone());
        let mut silent = pin!(wakeup.wait());
        assert!(silent.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        assert_eq!(
            quiet.execute("aggregate", DummyCommand::Reject, vec![]).await,
            Err(CommandGatewayError::Domain(DummyError))
        );
        assert_eq!(quiet.execute("aggregate", record("a"), vec![]).await, Ok(()));
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
        ] {
            assert_eq!(map_aggregate_error::<DummyError>(error), CommandGatewayError::Unavailable);
        }
    }
}
