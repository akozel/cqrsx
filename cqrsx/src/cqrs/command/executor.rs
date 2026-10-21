use super::{
    CommandExecutor, CommandGatewayError, FailedHandler,
    retry::retry_on_aggregate_conflict,
    turns::{AggregateTurns, TurnRefused},
};
use crate::{
    cqrs::{
        AggregateError, CqrsFramework,
        aggregate::{Aggregate, AggregateKey},
        command::retry::AggregateConflictRetryPolicy,
        store::EventStore,
    },
    task::NewTask,
};
use std::{collections::HashMap, error::Error, marker::PhantomData};
/// Executes commands for one aggregate type through frameworks supplied by the
/// composition root or a runtime adapter factory.
///
/// The framework is built exactly once per [`execute_with_metadata`](Self::execute_with_metadata)
/// call. Every optimistic-conflict attempt reloads aggregate state through that
/// same framework and repeats clones of the original command and of its event
/// metadata. The immutable outbox task list is handed to the factory once and
/// therefore cannot be regenerated or changed between attempts.
///
/// With [`with_turns`](Self::with_turns) a command first waits for its aggregate's turn and holds it
/// through every attempt and the event handlers; a refused turn is [`CommandGatewayError::Unavailable`]
/// and nothing is built or written. An event handler must not execute a command synchronously through
/// an executor with turns: for its own aggregate it would wait for its own turn, for another one two
/// such handlers could wait for each other; both end only when the wait times out. Use the outbox.
pub struct CqrsCommandExecutor<A, BuildFramework> {
    build_framework: BuildFramework,
    retry_policy: AggregateConflictRetryPolicy,
    turns: Option<AggregateTurns>,
    aggregate: PhantomData<fn() -> A>,
}

impl<A, BuildFramework> CqrsCommandExecutor<A, BuildFramework> {
    /// Commands to one aggregate run concurrently; the store's optimistic check resolves races.
    pub fn new(build_framework: BuildFramework, retry_policy: AggregateConflictRetryPolicy) -> Self {
        Self {
            build_framework,
            retry_policy,
            turns: None,
            aggregate: PhantomData,
        }
    }

    /// Commands to one aggregate run one at a time through `turns`. Pass the same registry (a clone) to
    /// every executor whose commands must order each other, e.g. to one built per call.
    pub fn with_turns(mut self, turns: AggregateTurns) -> Self {
        self.turns = Some(turns);
        self
    }
}

impl<A, BuildFramework> CqrsCommandExecutor<A, BuildFramework>
where
    A: Aggregate + Clone,
    A::Command: Clone + Send,
{
    /// Executes without event metadata; see [`execute_with_metadata`](Self::execute_with_metadata).
    pub async fn execute<ES>(
        &self,
        aggregate_id: &str,
        command: A::Command,
        tasks: Vec<NewTask>,
    ) -> Result<(), CommandGatewayError<A::Error>>
    where
        BuildFramework: Fn(Vec<NewTask>) -> CqrsFramework<A, ES> + Send + Sync,
        ES: EventStore<A> + 'static,
        ES::AC: Send,
    {
        self.execute_with_metadata(aggregate_id, command, HashMap::new(), tasks).await
    }

    /// Waits for the aggregate's turn when the executor has turns, builds one framework for `tasks`,
    /// then runs the command with bounded conflict retry. Each attempt receives a clone of `metadata`,
    /// so a retried command commits exactly the metadata of its first attempt.
    pub async fn execute_with_metadata<ES>(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
        tasks: Vec<NewTask>,
    ) -> Result<(), CommandGatewayError<A::Error>>
    where
        BuildFramework: Fn(Vec<NewTask>) -> CqrsFramework<A, ES> + Send + Sync,
        ES: EventStore<A> + 'static,
        ES::AC: Send,
    {
        // Held until the end of this function: through every attempt and the event handlers.
        let _turn = match &self.turns {
            Some(turns) => Some(
                turns
                    .enter(AggregateKey::of::<A>(aggregate_id))
                    .await
                    .map_err(turn_refused::<A::Error>(A::TYPE))?,
            ),
            None => None,
        };
        let framework = (self.build_framework)(tasks);
        let framework_ref = &framework;
        let metadata_ref = &metadata;
        retry_on_aggregate_conflict(self.retry_policy, move || {
            framework_ref.execute_with_metadata(aggregate_id, command.clone(), metadata_ref.clone())
        })
        .await
        .inspect_err(|error| log_aggregate_error(A::TYPE, error))
        .map_err(map_aggregate_error)
    }
}

fn turn_refused<D>(aggregate_type: &'static str) -> impl FnOnce(TurnRefused) -> CommandGatewayError<D> {
    move |refused| {
        crate::diagnostics::log_turn_refused(aggregate_type, refused);
        CommandGatewayError::Unavailable
    }
}

fn log_aggregate_error<D: Error>(aggregate_type: &str, error: &AggregateError<D>) {
    let (category, source) = match error {
        AggregateError::DatabaseConnectionError(source) => ("connection", source),
        AggregateError::DeserializationError(source) => ("deserialization", source),
        AggregateError::UnexpectedError(source) => ("unexpected", source),
        AggregateError::CommittedWithEventHandlerFailure(failure) => {
            for error in &failure.errors {
                crate::diagnostics::log_command_failure(aggregate_type, "event_handler", &error.error);
            }
            return;
        }
        AggregateError::Domain(_) | AggregateError::AggregateConflict => return,
    };
    crate::diagnostics::log_command_failure(aggregate_type, category, source.as_ref());
}

impl<A, BuildFramework, ES> CommandExecutor<A> for CqrsCommandExecutor<A, BuildFramework>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone + Send + 'static,
    A::Error: Send + 'static,
    BuildFramework: Fn(Vec<NewTask>) -> CqrsFramework<A, ES> + Send + Sync,
    ES: EventStore<A> + 'static,
    ES::AC: Send,
{
    async fn execute_with_metadata(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
        tasks: Vec<NewTask>,
    ) -> Result<(), CommandGatewayError<A::Error>> {
        CqrsCommandExecutor::execute_with_metadata(self, aggregate_id, command, metadata, tasks).await
    }
}

pub(super) fn map_aggregate_error<D: Error>(error: AggregateError<D>) -> CommandGatewayError<D> {
    match error {
        AggregateError::Domain(error) => CommandGatewayError::Domain(error),
        AggregateError::AggregateConflict => CommandGatewayError::Conflict,
        AggregateError::CommittedWithEventHandlerFailure(failure) => CommandGatewayError::CommittedWithEventHandlerFailure {
            last_sequence: failure.last_sequence,
            handlers: failure
                .errors
                .into_iter()
                .map(|e| FailedHandler {
                    index: e.handler_index,
                    name: e.handler_name,
                })
                .collect(),
        },
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
    use crate::cqrs::command::turns::TurnLimits;
    use crate::cqrs::event_handler::EventHandler;
    use crate::cqrs::store::{EventStore, PersistenceError};
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
        let executor = Arc::new(CqrsCommandExecutor::new(
            |_| {
                CqrsFramework::new(
                    crate::cqrs::testing::mem_store::MemStore::<SendOnlyAggregate>::default(),
                    vec![],
                    (),
                )
            },
            no_delay_policy(1),
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
        CqrsCommandExecutor::<DummyAggregate, _>::new(
            move |_| {
                CqrsFramework::new(
                    MetadataStore {
                        conflicts,
                        commits: commits.clone(),
                    },
                    vec![],
                    Arc::default(),
                )
            },
            no_delay_policy(3),
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
        let inherent = CqrsCommandExecutor::<DummyAggregate, _>::new(
            {
                let commits = commits_inherent.clone();
                move |_| {
                    CqrsFramework::new(
                        MetadataStore {
                            conflicts: 0,
                            commits: commits.clone(),
                        },
                        vec![],
                        Arc::default(),
                    )
                }
            },
            no_delay_policy(1),
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
        let executor = Arc::new(CqrsCommandExecutor::<DummyAggregate, _>::new(
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
                        vec![],
                        commands.clone(),
                    )
                }
            },
            no_delay_policy(3),
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
        let domain_executor = Arc::new(CqrsCommandExecutor::<DummyAggregate, _>::new(
            |_| {
                CqrsFramework::new(
                    DummyStore {
                        commits: Arc::default(),
                        conflicts: 0,
                        load_error: None,
                    },
                    vec![],
                    Arc::default(),
                )
            },
            no_delay_policy(3),
        ));
        assert_eq!(
            CommandExecutor::execute(domain_executor.as_ref(), "aggregate", DummyCommand::Reject, vec![]).await,
            Err(CommandGatewayError::Domain(DummyError))
        );

        let commits = Arc::new(AtomicUsize::new(0));
        let conflict_executor = Arc::new(CqrsCommandExecutor::<DummyAggregate, _>::new(
            {
                let commits = commits.clone();
                move |_| {
                    CqrsFramework::new(
                        DummyStore {
                            commits: commits.clone(),
                            conflicts: usize::MAX,
                            load_error: None,
                        },
                        vec![],
                        Arc::default(),
                    )
                }
            },
            no_delay_policy(3),
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
            let executor = Arc::new(CqrsCommandExecutor::<DummyAggregate, _>::new(
                move |_| {
                    CqrsFramework::new(
                        DummyStore {
                            commits: Arc::default(),
                            conflicts: 0,
                            load_error: Some(error),
                        },
                        vec![],
                        Arc::default(),
                    )
                },
                no_delay_policy(3),
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
                let executor = CqrsCommandExecutor::<DummyAggregate, _>::new(
                    move |_| {
                        CqrsFramework::new(
                            DummyStore {
                                commits: Arc::default(),
                                conflicts: 0,
                                load_error: Some(error),
                            },
                            vec![],
                            Arc::default(),
                        )
                    },
                    no_delay_policy(3),
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
            let executor = CqrsCommandExecutor::<DummyAggregate, _>::new(
                move |_| {
                    CqrsFramework::new(
                        DummyStore {
                            commits: count.clone(),
                            conflicts: usize::MAX,
                            load_error: None,
                        },
                        vec![],
                        Arc::default(),
                    )
                },
                no_delay_policy(3),
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

    fn versioned_executor<H>(
        store: &VersionedStore,
        attempts: usize,
        handlers: H,
    ) -> CqrsCommandExecutor<DummyAggregate, impl Fn(Vec<NewTask>) -> VersionedFramework + Send + Sync + use<H>>
    where
        H: Fn() -> Vec<Box<dyn EventHandler<DummyAggregate>>> + Send + Sync,
    {
        let store = store.clone();
        CqrsCommandExecutor::new(
            move |_| CqrsFramework::new(store.clone(), handlers(), Arc::default()),
            no_delay_policy(attempts),
        )
    }

    fn record(value: &str) -> DummyCommand {
        DummyCommand::Record(value.to_owned())
    }

    #[tokio::test]
    async fn turns_accept_twenty_parallel_commands_with_a_single_attempt_where_concurrency_conflicts() {
        for with_turns in [false, true] {
            let store = VersionedStore::default();
            let executor = versioned_executor(&store, 1, Vec::new);
            let executor = Arc::new(if with_turns {
                executor.with_turns(AggregateTurns::new(TurnLimits::default()))
            } else {
                executor
            });
            let mut commands = tokio::task::JoinSet::new();
            for index in 0..20 {
                let executor = executor.clone();
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
            if with_turns {
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
    async fn a_conflict_retries_inside_the_turn_before_the_next_command_starts() {
        for with_turns in [false, true] {
            let store = VersionedStore::default();
            store.external_conflicts.store(1, Ordering::SeqCst);
            let executor = versioned_executor(&store, 3, Vec::new);
            let executor = Arc::new(if with_turns {
                executor.with_turns(AggregateTurns::new(TurnLimits::default()))
            } else {
                executor
            });
            let first = tokio::spawn({
                let executor = executor.clone();
                async move { executor.execute("aggregate", record("a"), vec![]).await }
            });
            let second = tokio::spawn({
                let executor = executor.clone();
                async move { executor.execute("aggregate", record("b"), vec![]).await }
            });
            assert_eq!((first.await.unwrap(), second.await.unwrap()), (Ok(()), Ok(())));
            let calls = store.calls.lock().unwrap().clone();
            if with_turns {
                assert_eq!(calls, ["load", "conflict a", "load", "commit a", "load", "commit b"]);
            } else {
                assert_eq!(
                    calls[..2],
                    ["load", "load"],
                    "the control: `b` loads while `a` is still deciding"
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_turn_is_unavailable_logged_without_id_and_builds_nothing() {
        let builds = Arc::new(AtomicUsize::new(0));
        let turns = AggregateTurns::new(TurnLimits::new(Duration::from_millis(100), 1).unwrap());
        let executor = CqrsCommandExecutor::<DummyAggregate, _>::new(
            {
                let builds = builds.clone();
                move |_| {
                    builds.fetch_add(1, Ordering::SeqCst);
                    CqrsFramework::new(VersionedStore::default(), vec![], Arc::default())
                }
            },
            no_delay_policy(1),
        )
        .with_turns(turns.clone());
        let held = turns.enter(AggregateKey::of::<DummyAggregate>("private_id")).await.unwrap();
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
                "the turn did not come in time"
            );
        })
        .await;
        drop(held);
        assert_eq!(
            builds.load(Ordering::SeqCst),
            0,
            "a refused command builds and writes nothing"
        );
        assert_eq!(log.matches("command refused before it ran").count(), 2, "{log}");
        assert!(
            log.contains("reason=\"too_many_waiting\"") && log.contains("reason=\"timed_out\""),
            "{log}"
        );
        assert!(log.contains("aggregate_type=\"dummy\""), "{log}");
        assert!(!log.contains("private_"), "{log}");
    }

    /// Does what a command executed synchronously from a handler would do first: wait for its aggregate's turn.
    struct WaitsForItsOwnTurn(AggregateTurns);

    #[async_trait::async_trait]
    impl EventHandler<DummyAggregate> for WaitsForItsOwnTurn {
        fn name(&self) -> String {
            "nested".to_owned()
        }

        async fn handle(&self, aggregate_id: &str, _: &[EventEnvelope<DummyAggregate>]) -> Result<(), PersistenceError> {
            let turn = self.0.enter(AggregateKey::of::<DummyAggregate>(aggregate_id)).await;
            turn.map(drop)
                .map_err(|refused| PersistenceError::UnknownError(Box::new(refused)))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_handler_waiting_for_its_own_aggregate_times_out_instead_of_hanging() {
        let store = VersionedStore::default();
        let turns = AggregateTurns::new(TurnLimits::new(Duration::from_millis(100), 8).unwrap());
        let nested = turns.clone();
        let executor = versioned_executor(&store, 1, move || {
            vec![Box::new(WaitsForItsOwnTurn(nested.clone())) as Box<dyn EventHandler<DummyAggregate>>]
        })
        .with_turns(turns);
        assert_eq!(
            executor.execute("aggregate", record("a"), vec![]).await,
            Err(CommandGatewayError::CommittedWithEventHandlerFailure {
                last_sequence: 1,
                handlers: vec![FailedHandler {
                    index: 0,
                    name: "nested".to_owned()
                }]
            })
        );
        assert_eq!(*store.journal.lock().unwrap(), ["a"]);
    }

    #[test]
    fn the_command_future_with_a_turn_is_send() {
        fn require_send<F: std::future::Future + Send>(_: F) {}
        let executor =
            versioned_executor(&VersionedStore::default(), 1, Vec::new).with_turns(AggregateTurns::new(TurnLimits::default()));
        require_send(executor.execute("aggregate", record("a"), vec![]));
    }

    #[test]
    fn maps_every_framework_error_category() {
        assert_eq!(
            CommandGatewayError::<DummyError>::CommittedWithEventHandlerFailure {
                last_sequence: 42,
                handlers: vec![FailedHandler {
                    index: 2,
                    name: "halls".into()
                }]
            }
            .to_string(),
            "command committed through sequence 42; event handlers failed"
        );
        assert_eq!(
            map_aggregate_error(AggregateError::Domain(DummyError)),
            CommandGatewayError::Domain(DummyError)
        );
        assert_eq!(
            map_aggregate_error::<DummyError>(AggregateError::AggregateConflict),
            CommandGatewayError::Conflict
        );
        assert_eq!(
            map_aggregate_error::<DummyError>(AggregateError::CommittedWithEventHandlerFailure(
                crate::cqrs::CommittedWithEventHandlerFailure {
                    last_sequence: 42,
                    errors: vec![crate::cqrs::EventHandlerFailure {
                        handler_index: 2,
                        handler_name: "halls".into(),
                        error: crate::cqrs::store::PersistenceError::OptimisticLockError
                    }]
                }
            )),
            CommandGatewayError::CommittedWithEventHandlerFailure {
                last_sequence: 42,
                handlers: vec![FailedHandler {
                    index: 2,
                    name: "halls".into()
                }]
            }
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
