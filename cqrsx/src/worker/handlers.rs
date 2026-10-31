//! The assembly of an application's task handlers into the one dispatcher every worker shares.
use crate::{
    command::CommandExecutor,
    domain::aggregate::Aggregate,
    task::Task,
    worker::{
        TaskHandler,
        adapter::{TaskAdapter, boxed::BoxedTaskAdapter},
        context::Executors,
        dependencies::Dependencies,
        dispatcher::{TaskDispatcher, TaskDispatcherError},
    },
};
use serde::de::DeserializeOwned;
use std::sync::Arc;

/// The task handlers of an application, their dependencies and the command executors their commands run with,
/// assembled into one [`TaskDispatcher`] that every worker of the runtime shares.
///
/// [`build`](Self::build) refuses what a running handler would trip over: no handler, a task format twice or one that
/// is not valid, a dependency a handler declares and nobody provided, a second executor of one aggregate type. The
/// first refusal is the one reported. The policies of the task types (the pause before a retry, the attempt budget)
/// are not the handlers': the runtime gets them ([`Policies`](crate::task::policy::Policies)).
///
/// ```
/// use cqrsx::{
///     task::Task,
///     worker::{TaskResult, context::TaskContext, handlers::TaskHandlers, task_handler},
/// };
/// use serde::{Deserialize, Serialize};
/// use std::sync::Arc;
///
/// #[derive(Serialize, Deserialize, Task)]
/// #[task(name = "send_e_ticket", version = 1)]
/// struct SendETicket {
///     seat: String,
/// }
///
/// trait MailService: Send + Sync {
///     fn send(&self, seat: &str) -> bool;
/// }
/// type Mailer = dyn MailService;
///
/// /// One e-ticket per task; a refused delivery is retried after the pause of the task type's policy.
/// #[task_handler]
/// async fn send_e_ticket(task: SendETicket, mail: &Mailer) -> TaskResult {
///     if mail.send(&task.seat) { TaskResult::done() } else { TaskResult::retry("mail_unavailable") }
/// }
///
/// struct Outbox;
/// impl MailService for Outbox {
///     fn send(&self, _: &str) -> bool { true }
/// }
///
/// let mail: Arc<Mailer> = Arc::new(Outbox);
/// let dispatcher = TaskHandlers::new().handler(send_e_ticket).provide(mail).build()?;
/// assert_eq!(dispatcher.formats()[0].to_string(), "send_e_ticket v1");
/// // Nobody provides the mailer: refused before a worker could run the handler.
/// assert!(TaskHandlers::new().handler(send_e_ticket).build().is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Default)]
#[must_use]
pub struct TaskHandlers {
    adapters: Vec<BoxedTaskAdapter>,
    dependencies: Dependencies,
    executors: Executors,
    refused: Option<TaskDispatcherError>,
}

impl TaskHandlers {
    pub fn new() -> Self {
        Self::default()
    }

    /// The handlers of `adapters` with `dependencies`: what the modules of an engine registered.
    pub(crate) fn assembled(adapters: Vec<BoxedTaskAdapter>, dependencies: Dependencies) -> Self {
        Self {
            adapters,
            dependencies,
            ..Self::default()
        }
    }

    /// `handler` for the tasks of its payload type `T`, decoded from JSON
    /// ([`JsonTaskAdapter`](super::adapter::json::JsonTaskAdapter)).
    pub fn handler<T, H>(mut self, handler: H) -> Self
    where
        T: Task + DeserializeOwned + Send + 'static,
        H: TaskHandler<T> + 'static,
    {
        self.adapters.push(BoxedTaskAdapter::json::<T, H>(handler));
        self
    }

    /// An adapter of its own, e.g. one that converts several versions of a task into one handler's input.
    pub fn adapter(mut self, adapter: impl TaskAdapter + 'static) -> Self {
        self.adapters.push(BoxedTaskAdapter::new(adapter));
        self
    }

    /// `dependency` for every handler that names its type `T` (a trait object too); a second one of a type replaces
    /// the first.
    pub fn provide<T: ?Sized + Send + Sync + 'static>(mut self, dependency: Arc<T>) -> Self {
        self.dependencies = self.dependencies.provide(dependency);
        self
    }

    /// `executor` for the commands of `A` the handlers run ([`TaskContext::execute`](super::context::TaskContext::execute));
    /// one per aggregate type.
    pub fn executor<A, E>(mut self, executor: Arc<E>) -> Self
    where
        A: Aggregate + 'static,
        E: CommandExecutor<A> + 'static,
    {
        match self.executors.clone().with::<A, E>(executor) {
            Ok(executors) => self.executors = executors,
            Err(twice) => self.refuse(twice.into()),
        }
        self
    }

    /// The dispatcher of the handlers, or the first refusal.
    pub fn build(self) -> Result<TaskDispatcher<BoxedTaskAdapter>, TaskDispatcherError> {
        if let Some(refused) = self.refused {
            return Err(refused);
        }
        TaskDispatcher::assemble(self.adapters, self.dependencies, self.executors)
    }

    fn refuse(&mut self, error: TaskDispatcherError) {
        self.refused.get_or_insert(error);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command::{CommandGatewayError, CommitReport},
        domain::{command::Decide, event::Events},
        task::{attempt::TaskAttempt, format::TaskFormat},
        worker::{
            TaskResult,
            context::{ExecutorTwice, TaskContext},
            dependencies::DependencyId,
        },
    };
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::{collections::HashMap, time::Duration};

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

    impl Decide<Ledger> for () {
        fn decide(self, _: &Ledger, _: &()) -> Result<Recorded, Refused> {
            Ok(Recorded)
        }
    }

    impl Aggregate for Ledger {
        const TYPE: &'static str = "Ledger";
        type Id = String;
        type Command = ();
        type Event = Recorded;
        type Error = Refused;
        type Rules = ();

        fn apply(&mut self, _: Recorded) {}
    }

    /// Answers every command with a conflict.
    struct Conflicting;

    impl CommandExecutor<Ledger> for Conflicting {
        async fn execute_with_metadata(
            &self,
            _: &String,
            _: (),
            _: HashMap<String, String>,
        ) -> Result<CommitReport<Ledger>, CommandGatewayError<Refused>> {
            Err(CommandGatewayError::Conflict)
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Record {
        ledger: String,
    }

    impl Task for Record {
        const TASK_TYPE: &'static str = "record";
        const TASK_VERSION: i32 = 1;
    }

    #[derive(Serialize, Deserialize)]
    struct Stamp {}

    impl Task for Stamp {
        const TASK_TYPE: &'static str = "stamp";
        const TASK_VERSION: i32 = 1;
    }

    /// Runs the ledger's command and reports the prefix it was given and the answer.
    struct RecordHandler;

    impl TaskHandler<Record> for RecordHandler {
        const DEPENDENCIES: &'static [DependencyId] = &[DependencyId::of::<String>()];

        async fn handle(&self, task: Record, ctx: &TaskContext) -> TaskResult {
            let Ok(prefix) = ctx.dependency::<String>() else {
                return TaskResult::fail("no prefix");
            };
            let answer = match ctx.execute::<Ledger>(&task.ledger, ()).await {
                Err(CommandGatewayError::Conflict) => "conflict",
                _ => "something else",
            };
            TaskResult::retry(format!("{prefix}: {answer}"))
        }
    }

    struct StampHandler;

    impl TaskHandler<Stamp> for StampHandler {
        async fn handle(&self, _: Stamp, _: &TaskContext) -> TaskResult {
            TaskResult::Done
        }
    }

    /// A multi-format adapter of its own.
    struct Legacy;

    impl TaskAdapter for Legacy {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![TaskFormat::try_new("legacy", 1).unwrap()]
        }
        async fn invoke(&self, _: &TaskContext) -> TaskResult {
            TaskResult::fail("legacy")
        }
    }

    fn attempt<P: Task + Serialize>(payload: &P) -> TaskAttempt {
        TaskAttempt::try_first(payload, json!({})).unwrap()
    }

    #[tokio::test]
    async fn the_handlers_assemble_one_dispatcher_with_their_dependencies_and_executors() {
        let dispatcher = TaskHandlers::new()
            .handler(RecordHandler)
            .handler(StampHandler)
            .adapter(Legacy)
            .provide(Arc::new(String::from("first")))
            .provide(Arc::new(String::from("ledger")))
            .executor(Arc::new(Conflicting))
            .build()
            .unwrap();
        assert_eq!(
            dispatcher.formats().iter().map(ToString::to_string).collect::<Vec<_>>(),
            ["record v1", "stamp v1", "legacy v1"]
        );
        let record = Record { ledger: "fox".into() };
        assert_eq!(
            dispatcher.dispatch(Arc::new(attempt(&record)), Duration::ZERO).await,
            TaskResult::retry("ledger: conflict"),
            "the second provided prefix, the executor's answer"
        );
        assert_eq!(
            dispatcher.dispatch(Arc::new(attempt(&Stamp {})), Duration::ZERO).await,
            TaskResult::Done
        );
        let legacy = TaskAttempt::try_new(
            uuid::Uuid::nil(),
            TaskFormat::try_new("legacy", 1).unwrap(),
            json!({}),
            json!({}),
            1,
        );
        assert_eq!(
            dispatcher.dispatch(Arc::new(legacy.unwrap()), Duration::ZERO).await,
            TaskResult::fail("legacy")
        );
    }

    #[test]
    fn assembly_refuses_a_missing_dependency_a_second_executor_a_duplicate_or_invalid_format_and_nothing() {
        let refusal = |handlers: TaskHandlers| handlers.build().err().unwrap();
        assert_eq!(
            refusal(TaskHandlers::new().handler(RecordHandler)),
            TaskDispatcherError::MissingDependency {
                format: TaskFormat::of::<Record>(),
                dependency: DependencyId::of::<String>().name(),
            }
        );
        assert_eq!(
            refusal(
                TaskHandlers::new()
                    .handler(StampHandler)
                    .executor(Arc::new(Conflicting))
                    .executor(Arc::new(Conflicting))
            ),
            TaskDispatcherError::ExecutorTwice(ExecutorTwice("Ledger"))
        );
        assert_eq!(
            refusal(TaskHandlers::new().handler(StampHandler).adapter(StampHandlerAdapter)),
            TaskDispatcherError::DuplicateFormat(TaskFormat::of::<Stamp>())
        );
        assert_eq!(refusal(TaskHandlers::new()), TaskDispatcherError::Empty);
    }

    #[tokio::test]
    async fn assembled_handlers_are_built_with_the_dependencies_they_were_given() {
        let adapters = vec![BoxedTaskAdapter::json::<Record, _>(RecordHandler)];
        let dependencies = Dependencies::default().provide(Arc::new(String::from("module")));
        let dispatcher = TaskHandlers::assembled(adapters, dependencies)
            .executor::<Ledger, _>(Arc::new(Conflicting))
            .build()
            .unwrap();
        let record = Record { ledger: "fox".into() };
        assert_eq!(
            dispatcher.dispatch(Arc::new(attempt(&record)), Duration::ZERO).await,
            TaskResult::retry("module: conflict")
        );
        let adapters = vec![BoxedTaskAdapter::json::<Record, _>(RecordHandler)];
        assert_eq!(
            TaskHandlers::assembled(adapters, Dependencies::default()).build().err(),
            Some(TaskDispatcherError::MissingDependency {
                format: TaskFormat::of::<Record>(),
                dependency: DependencyId::of::<String>().name(),
            })
        );
    }

    #[test]
    fn the_first_refusal_is_the_one_reported() {
        let twice = TaskHandlers::new()
            .handler(StampHandler)
            .executor(Arc::new(Conflicting))
            .executor(Arc::new(Conflicting))
            .executor(Arc::new(Conflicting))
            .handler(RecordHandler); // would be refused for its dependency too, but the executor came first
        assert_eq!(
            twice.build().err(),
            Some(TaskDispatcherError::ExecutorTwice(ExecutorTwice("Ledger")))
        );
    }

    /// A second registration of the stamp's format.
    struct StampHandlerAdapter;

    impl TaskAdapter for StampHandlerAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![TaskFormat::of::<Stamp>()]
        }
        async fn invoke(&self, _: &TaskContext) -> TaskResult {
            TaskResult::Done
        }
    }
}
