//! What a task handler gets besides its task: the attempt, the pause of its retry, its dependencies and the command
//! executors of the application.
use crate::{
    command::{CommandExecutor, CommandGatewayError, CommitReport, metadata::CommandMetadata},
    domain::aggregate::Aggregate,
    task::attempt::TaskAttempt,
    worker::dependencies::{Dependencies, MissingDependency},
};
use std::{
    any::{Any, TypeId, type_name},
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use thiserror::Error;
use uuid::Uuid;

/// What a task handler gets besides its task, for one attempt.
///
/// - [`task_id`](Self::task_id) and [`attempt`](Self::attempt): the task's stable id (an idempotency key), the claim
///   number, the format and the metadata; no lease credentials.
/// - [`retry_pause`](Self::retry_pause): the pause the executor applies if this attempt answers
///   [`TaskResult::Retry`](super::TaskResult::Retry).
/// - [`dependency`](Self::dependency): what the assembly provided, by type.
/// - [`execute`](Self::execute): a command to another aggregate that continues the task's causation chain.
///
/// The runtime builds one per attempt; [`new`](Self::new) builds one for calling a handler outside it, in its tests:
///
/// ```
/// use cqrsx::task::{attempt::TaskAttempt, Task};
/// use cqrsx::worker::context::TaskContext;
/// use serde::Serialize;
/// use std::time::Duration;
///
/// #[derive(Serialize)]
/// struct Ping {}
/// impl Task for Ping {
///     const TASK_TYPE: &'static str = "ping";
///     const TASK_VERSION: i32 = 1;
/// }
///
/// let attempt = TaskAttempt::try_first(&Ping {}, serde_json::json!({}))?;
/// let id = attempt.task_id();
/// let ctx = TaskContext::new(attempt, Duration::from_secs(1));
/// assert_eq!((ctx.task_id(), ctx.attempt().number(), ctx.retry_pause()), (id, 1, Duration::from_secs(1)));
/// assert!(ctx.dependency::<String>().is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[must_use]
pub struct TaskContext {
    attempt: Arc<TaskAttempt>,
    retry_pause: Duration,
    dependencies: Dependencies,
    executors: Executors,
}

impl TaskContext {
    /// A context for `attempt`, whose retry would wait `retry_pause`, with no dependencies and no executors.
    pub fn new(attempt: TaskAttempt, retry_pause: Duration) -> Self {
        Self {
            attempt: Arc::new(attempt),
            retry_pause,
            dependencies: Dependencies::default(),
            executors: Executors::default(),
        }
    }

    /// The context the dispatcher builds for one attempt, with what it holds for every handler.
    pub(crate) fn from_parts(
        attempt: Arc<TaskAttempt>,
        retry_pause: Duration,
        dependencies: Dependencies,
        executors: Executors,
    ) -> Self {
        Self {
            attempt,
            retry_pause,
            dependencies,
            executors,
        }
    }

    /// The same context with `dependencies`.
    pub fn with_dependencies(mut self, dependencies: Dependencies) -> Self {
        self.dependencies = dependencies;
        self
    }

    /// The same context with `executors`.
    pub fn with_executors(mut self, executors: Executors) -> Self {
        self.executors = executors;
        self
    }

    /// The stable id of the task, the same on every attempt.
    pub fn task_id(&self) -> Uuid {
        self.attempt.task_id()
    }

    /// The attempt: its number, the task's format, payload and metadata.
    pub fn attempt(&self) -> &TaskAttempt {
        &self.attempt
    }

    /// The pause the executor applies if this attempt answers retry: the policy of the task's type, or the queue's
    /// redelivery backoff for a type without one, chosen before the handler runs. What a handler reports of its retry
    /// (an activity feed) reads it here.
    pub fn retry_pause(&self) -> Duration {
        self.retry_pause
    }

    /// The dependency of type `T` the assembly provided.
    pub fn dependency<T: ?Sized + 'static>(&self) -> Result<&T, MissingDependency> {
        self.dependencies.get::<T>().ok_or(MissingDependency(type_name::<T>()))
    }

    /// Runs `command` on the aggregate `id` with the executor of `A`. The command continues the task's causation
    /// chain: the task's correlation id, caused by the task, the source `task:<type>`, and `recorded_at`
    /// ([`CommandMetadata::from_task`]). The answer converts into the task's result
    /// ([`TaskResult::from`](super::TaskResult)). Without an executor of `A` the command does not run: the
    /// answer is `Unavailable` and the missing executor is logged.
    pub async fn execute<A: Aggregate + 'static>(
        &self,
        id: &A::Id,
        command: A::Command,
    ) -> Result<CommitReport<A>, CommandGatewayError<A::Error>> {
        let Some(executor) = self.executors.get::<A>() else {
            crate::diagnostics::log_missing_executor(A::TYPE);
            return Err(CommandGatewayError::Unavailable);
        };
        let metadata = CommandMetadata::from_task(&self.attempt).to_map();
        executor.run(id, command, metadata).await
    }
}

impl fmt::Debug for TaskContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskContext")
            .field("task_id", &self.attempt.task_id())
            .field("attempt", &self.attempt.number())
            .field("retry_pause", &self.retry_pause)
            .finish_non_exhaustive()
    }
}

type Answer<A> = Result<CommitReport<A>, CommandGatewayError<<A as Aggregate>::Error>>;
type AnswerFuture<'a, A> = Pin<Box<dyn Future<Output = Answer<A>> + Send + 'a>>;

/// A command executor of `A` behind one type: one allocation per command.
pub(crate) trait ErasedExecutor<A: Aggregate + 'static>: Send + Sync {
    fn run<'a>(&'a self, id: &'a A::Id, command: A::Command, metadata: HashMap<String, String>) -> AnswerFuture<'a, A>;
}

impl<A: Aggregate + 'static, E: CommandExecutor<A>> ErasedExecutor<A> for E {
    fn run<'a>(&'a self, id: &'a A::Id, command: A::Command, metadata: HashMap<String, String>) -> AnswerFuture<'a, A> {
        Box::pin(self.execute_with_metadata(id, command, metadata))
    }
}

/// The command executors task handlers run commands with ([`TaskContext::execute`]), one per aggregate type: the
/// assembly fills it ([`TaskHandlers::executor`](super::handlers::TaskHandlers::executor)). A clone is cheap (`Arc`).
#[derive(Clone, Default)]
pub struct Executors(Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>);

impl Executors {
    /// `executor` for the commands of `A`; a second executor of one aggregate type is refused, as a second executor
    /// would order nothing with the first.
    pub fn with<A: Aggregate + 'static, E: CommandExecutor<A> + 'static>(
        mut self,
        executor: Arc<E>,
    ) -> Result<Self, ExecutorTwice> {
        let executors = Arc::make_mut(&mut self.0);
        if executors.contains_key(&TypeId::of::<A>()) {
            return Err(ExecutorTwice(A::TYPE));
        }
        let erased: Arc<dyn ErasedExecutor<A>> = executor;
        executors.insert(TypeId::of::<A>(), Arc::new(erased));
        Ok(self)
    }

    /// The executor of `A`, if one is registered: what `TaskContext::execute` and the engine's `execute` run.
    pub(crate) fn get<A: Aggregate + 'static>(&self) -> Option<&Arc<dyn ErasedExecutor<A>>> {
        self.0.get(&TypeId::of::<A>())?.downcast_ref()
    }
}

impl fmt::Debug for Executors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Executors").field("count", &self.0.len()).finish()
    }
}

/// A second command executor of one aggregate type.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("the command executor of the aggregate type `{0}` is registered twice")]
pub struct ExecutorTwice(pub &'static str);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command::{
            causation::{CAUSATION_ID, COMMAND_ID, CORRELATION_ID, SOURCE},
            metadata::RECORDED_AT,
        },
        domain::{command::Decide, event::Events},
        task::format::TaskFormat,
        worker::test_support::DummyTask,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::sync::Mutex;

    /// A ledger that records whatever it is told.
    #[derive(Default, Serialize, Deserialize)]
    struct Ledger;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Recorded;

    impl Events for Recorded {
        fn event_type(&self) -> &'static str {
            "Recorded"
        }
        fn event_version(&self) -> &'static str {
            "1"
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("refused")]
    struct Refused;

    #[derive(Clone, Debug, PartialEq)]
    struct Record(u8);

    impl Decide<Ledger> for Record {
        fn decide(self, _: &Ledger, _: &()) -> Result<Recorded, Refused> {
            Ok(Recorded)
        }
    }

    impl Aggregate for Ledger {
        const TYPE: &'static str = "Ledger";
        type Id = String;
        type Command = Record;
        type Event = Recorded;
        type Error = Refused;
        type Rules = ();

        fn apply(&mut self, _: Recorded) {}
    }

    /// The id, the command and the metadata of one command.
    type Call = (String, Record, HashMap<String, String>);

    /// Answers every command with `answer` and records the id, the command and the metadata it got.
    struct Scripted {
        answer: fn() -> Answer<Ledger>,
        calls: Mutex<Vec<Call>>,
    }

    impl Scripted {
        fn new(answer: fn() -> Answer<Ledger>) -> Arc<Self> {
            Arc::new(Self {
                answer,
                calls: Mutex::new(Vec::new()),
            })
        }
    }

    impl CommandExecutor<Ledger> for Scripted {
        async fn execute_with_metadata(&self, id: &String, command: Record, metadata: HashMap<String, String>) -> Answer<Ledger> {
            self.calls.lock().unwrap().push((id.clone(), command, metadata));
            (self.answer)()
        }
    }

    fn attempt(metadata: serde_json::Value) -> TaskAttempt {
        TaskAttempt::try_new(Uuid::new_v4(), TaskFormat::of::<DummyTask>(), json!({}), metadata, 3).unwrap()
    }

    fn command() -> Record {
        Record(7)
    }

    #[test]
    fn a_context_tells_the_handler_its_task_its_attempt_and_the_pause_of_its_retry() {
        let attempt = attempt(json!({ "correlation_id": "corr-q" }));
        let id = attempt.task_id();
        let ctx = TaskContext::new(attempt, Duration::from_millis(1500));
        assert_eq!(ctx.task_id(), id);
        assert_eq!(ctx.attempt().number(), 3);
        assert_eq!(ctx.attempt().format(), &TaskFormat::of::<DummyTask>());
        assert_eq!(ctx.attempt().metadata(), &json!({ "correlation_id": "corr-q" }));
        assert_eq!(ctx.retry_pause(), Duration::from_millis(1500));
        let rendered = format!("{ctx:?}");
        assert!(
            rendered.contains(&id.to_string()) && rendered.contains("attempt: 3"),
            "{rendered}"
        );
        assert!(!rendered.contains("corr-q"), "no metadata in diagnostics: {rendered}");
    }

    #[test]
    fn a_dependency_nobody_provided_is_named_in_the_error() {
        let ctx = TaskContext::new(attempt(json!({})), Duration::ZERO);
        assert_eq!(ctx.dependency::<String>(), Err(MissingDependency("alloc::string::String")));
        let ctx = ctx.with_dependencies(Dependencies::default().provide(Arc::new(String::from("fox"))));
        assert_eq!(ctx.dependency::<String>().map(String::as_str), Ok("fox"));
    }

    #[tokio::test]
    async fn a_command_from_the_context_continues_the_tasks_causation_with_a_fixed_width_time() {
        let correlation_id = Uuid::now_v7();
        let attempt = attempt(json!({ "correlation_id": correlation_id }));
        let task_id = attempt.task_id();
        let executor = Scripted::new(|| Ok(CommitReport::default()));
        let ctx = TaskContext::new(attempt, Duration::ZERO)
            .with_executors(Executors::default().with::<Ledger, _>(executor.clone()).unwrap());
        assert!(ctx.execute::<Ledger>(&"fox".to_owned(), command()).await.is_ok());
        let calls = executor.calls.lock().unwrap();
        let [(id, sent, metadata)] = &calls[..] else {
            panic!("one command: {calls:?}");
        };
        assert_eq!((id.as_str(), sent), ("fox", &command()));
        let mut keys: Vec<&str> = metadata.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, [CAUSATION_ID, COMMAND_ID, CORRELATION_ID, RECORDED_AT, SOURCE]);
        assert_eq!(metadata[CORRELATION_ID], correlation_id.to_string());
        assert_eq!(metadata[CAUSATION_ID], task_id.to_string());
        assert_eq!(metadata[SOURCE], "task:dummy_task");
        assert_ne!(metadata[COMMAND_ID], correlation_id.to_string());
        assert_eq!(metadata[RECORDED_AT].len(), "2026-10-17T09:12:03.123456Z".len());
    }

    #[tokio::test]
    async fn the_executor_of_each_aggregate_type_runs_its_commands_and_a_second_one_is_refused() {
        let accepted = Scripted::new(|| Ok(CommitReport::default()));
        let rejected = Scripted::new(|| Err(CommandGatewayError::Conflict));
        let executors = Executors::default().with::<Ledger, _>(accepted.clone()).unwrap();
        assert_eq!(
            executors.clone().with::<Ledger, _>(rejected.clone()).unwrap_err(),
            ExecutorTwice("Ledger")
        );
        assert_eq!(
            ExecutorTwice("Ledger").to_string(),
            "the command executor of the aggregate type `Ledger` is registered twice"
        );
        assert_eq!(format!("{executors:?}"), "Executors { count: 1 }");
        let ctx = TaskContext::new(attempt(json!({})), Duration::ZERO).with_executors(executors);
        let answer = ctx.execute::<Ledger>(&"fox".to_owned(), command()).await.unwrap();
        assert!(answer.events.is_empty() && answer.tasks.is_empty());
        assert_eq!(accepted.calls.lock().unwrap().len(), 1);
        assert!(rejected.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn without_an_executor_of_its_aggregate_type_a_command_is_unavailable_and_logged_once_without_ids() {
        let ctx = TaskContext::new(attempt(json!({})), Duration::ZERO);
        let (answer, log) =
            crate::diagnostics::test_support::capture(ctx.execute::<Ledger>(&"private_id".to_owned(), command())).await;
        assert!(matches!(answer, Err(CommandGatewayError::Unavailable)));
        assert_eq!(log.matches("no command executor for the task's command").count(), 1, "{log}");
        assert!(log.contains("aggregate_type=\"Ledger\""), "{log}");
        assert!(!log.contains("private_id"), "{log}");
    }
}
