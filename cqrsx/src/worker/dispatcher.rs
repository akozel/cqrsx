use crate::task::{attempt::TaskAttempt, format::TaskFormat};
use crate::worker::{
    TaskResult,
    adapter::TaskAdapter,
    context::{Executors, TaskContext},
    dependencies::Dependencies,
};
use std::{collections::HashMap, fmt, sync::Arc, time::Duration};
use thiserror::Error;

#[derive(Error, PartialEq, Eq)]
pub enum TaskDispatcherError {
    #[error("task dispatcher must contain at least one adapter")]
    Empty,
    #[error("duplicate task format: {} v{}", crate::diagnostics::safe_type(.0.task_type()), .0.task_version())]
    DuplicateFormat(TaskFormat),
    /// The handler of a format needs a dependency nobody provided.
    #[error(
        "the handler of {} v{} needs `{dependency}`, which nothing provides",
        crate::diagnostics::safe_type(.format.task_type()),
        .format.task_version()
    )]
    MissingDependency { format: TaskFormat, dependency: &'static str },
    /// A second command executor of one aggregate type.
    #[error("{0}")]
    ExecutorTwice(#[from] crate::worker::context::ExecutorTwice),
}

impl fmt::Debug for TaskDispatcherError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("Empty"),
            Self::DuplicateFormat(format) => f
                .debug_struct("DuplicateFormat")
                .field("task_type", &crate::diagnostics::safe_type(format.task_type()))
                .field("task_version", &format.task_version())
                .finish(),
            Self::MissingDependency { format, dependency } => f
                .debug_struct("MissingDependency")
                .field("task_type", &crate::diagnostics::safe_type(format.task_type()))
                .field("task_version", &format.task_version())
                .field("dependency", dependency)
                .finish(),
            Self::ExecutorTwice(error) => f.debug_tuple("ExecutorTwice").field(error).finish(),
        }
    }
}

/// Routes each persisted task format to exactly one adapter, for every worker: one dispatcher serves them all.
/// It holds what its handlers use besides their tasks, the dependencies and the command executors, and hands them to
/// every handler in its context ([`TaskContext`]).
/// A dispatcher is a router, not a wire adapter:
/// ```compile_fail
/// use cqrsx::worker::{adapter::TaskAdapter, dispatcher::TaskDispatcher};
/// fn requires_adapter<T: TaskAdapter>() {}
/// fn check<A: TaskAdapter>() { requires_adapter::<TaskDispatcher<A>>(); }
/// ```
pub struct TaskDispatcher<A> {
    adapters: Vec<A>,
    formats: Vec<TaskFormat>,
    routes: HashMap<TaskFormat, usize>,
    dependencies: Dependencies,
    executors: Executors,
}

// By hand: the adapters need not be `Debug`; the formats name what they handle.
impl<A> fmt::Debug for TaskDispatcher<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskDispatcher")
            .field("formats", &self.formats)
            .field("dependencies", &self.dependencies)
            .field("executors", &self.executors)
            .finish_non_exhaustive()
    }
}

impl<A: TaskAdapter> TaskDispatcher<A> {
    /// A dispatcher over `adapters`, without dependencies or executors: an adapter that needs a dependency is refused.
    pub fn try_new(adapters: Vec<A>) -> Result<Self, TaskDispatcherError> {
        Self::assemble(adapters, Dependencies::default(), Executors::default())
    }

    /// A dispatcher over `adapters` whose handlers get `dependencies` and `executors`; every dependency an adapter
    /// declares must be provided ([`TaskHandlers`](super::handlers::TaskHandlers) assembles one).
    pub(crate) fn assemble(
        adapters: Vec<A>,
        dependencies: Dependencies,
        executors: Executors,
    ) -> Result<Self, TaskDispatcherError> {
        if adapters.is_empty() {
            return Err(TaskDispatcherError::Empty);
        }
        let (routes, formats) = adapters
            .iter()
            .enumerate()
            .flat_map(|(index, adapter)| adapter.formats().into_iter().map(move |format| (index, format)))
            .try_fold((HashMap::new(), Vec::new()), |(mut routes, mut formats), (index, format)| {
                if routes.insert(format.clone(), index).is_some() {
                    return Err(TaskDispatcherError::DuplicateFormat(format));
                }
                formats.push(format);
                Ok((routes, formats))
            })?;
        if formats.is_empty() {
            return Err(TaskDispatcherError::Empty);
        }
        for adapter in &adapters {
            let missing = adapter.dependencies().into_iter().find(|id| !dependencies.contains(*id));
            if let (Some(dependency), Some(format)) = (missing, adapter.formats().into_iter().next()) {
                return Err(TaskDispatcherError::MissingDependency {
                    format,
                    dependency: dependency.name(),
                });
            }
        }
        Ok(Self {
            adapters,
            formats,
            routes,
            dependencies,
            executors,
        })
    }

    /// Borrow the supported formats in routing and polling order.
    pub fn formats(&self) -> &[TaskFormat] {
        &self.formats
    }

    /// Route an attempt by its complete format and invoke that adapter with the attempt's context: the pause its
    /// retry would get (`retry_pause`), the dependencies and the executors.
    pub async fn dispatch(&self, attempt: Arc<TaskAttempt>, retry_pause: Duration) -> TaskResult {
        let ctx = TaskContext::shared(attempt, retry_pause, self.dependencies.clone(), self.executors.clone());
        match self
            .routes
            .get(ctx.attempt().format())
            .and_then(|&index| self.adapters.get(index))
        {
            Some(adapter) => adapter.invoke(&ctx).await,
            None => TaskResult::fail("unsupported task format"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{attempt::TaskAttempt, format::TaskFormat};
    use crate::worker::{
        adapter::TaskAdapter,
        dependencies::{DependencyId, MissingDependency},
        test_support::claimed,
    };

    use std::sync::{Arc, Mutex};

    #[test]
    fn dispatcher_error_formatting_sanitizes_names_without_changing_the_format() {
        for (name, safe_name) in [
            ("create_account", "create_account"),
            ("private_format\nmail@example.com", "unknown"),
        ] {
            let format = TaskFormat::try_new(name, 2).unwrap();
            let error = TaskDispatcherError::DuplicateFormat(format.clone());
            let rendered = format!("{error} {error:?}");
            assert!(rendered.contains(safe_name));
            assert!(rendered.contains('2'));
            assert!(!rendered.contains("private_"));
            assert!(!rendered.contains("mail@example.com"));
            assert!(matches!(error, TaskDispatcherError::DuplicateFormat(actual) if actual == format));
        }
        assert_eq!(format!("{:?}", TaskDispatcherError::Empty), "Empty");
    }

    struct RecordingAdapter {
        formats: Vec<TaskFormat>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl TaskAdapter for RecordingAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            self.formats.clone()
        }
        async fn invoke(&self, ctx: &TaskContext) -> TaskResult {
            self.calls.lock().unwrap().push(ctx.attempt().format().task_type().into());
            TaskResult::Done
        }
    }
    fn adapter(formats: Vec<TaskFormat>, calls: &Arc<Mutex<Vec<String>>>) -> RecordingAdapter {
        RecordingAdapter {
            formats,
            calls: calls.clone(),
        }
    }
    #[tokio::test]
    async fn routes_in_order_and_rejects_duplicates_empty_and_unknown_formats() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let first = TaskFormat::try_new("first", 1).unwrap();
        let second = TaskFormat::try_new("second", 2).unwrap();
        let dispatcher = TaskDispatcher::try_new(vec![
            adapter(vec![first.clone()], &calls),
            adapter(vec![second.clone()], &calls),
        ])
        .unwrap();
        assert_eq!(dispatcher.formats(), vec![first.clone(), second.clone()]);
        let base = claimed();
        for format in [second, first.clone()] {
            let input = TaskAttempt::try_new(
                base.attempt().task_id(),
                format,
                base.attempt().payload().clone(),
                base.attempt().metadata().clone(),
                1,
            )
            .unwrap();
            assert_eq!(dispatcher.dispatch(Arc::new(input), Duration::ZERO).await, TaskResult::Done);
        }
        assert_eq!(*calls.lock().unwrap(), vec!["second", "first"]);
        assert!(matches!(
            dispatcher.dispatch(Arc::new(base.attempt().clone()), Duration::ZERO).await,
            TaskResult::Fail { .. }
        ));
        assert!(matches!(
            TaskDispatcher::<RecordingAdapter>::try_new(vec![]),
            Err(TaskDispatcherError::Empty)
        ));
        assert!(matches!(
            TaskDispatcher::try_new(vec![adapter(vec![], &calls)]),
            Err(TaskDispatcherError::Empty)
        ));
        assert!(matches!(
            TaskDispatcher::try_new(vec![adapter(vec![first.clone(), first], &calls)]),
            Err(TaskDispatcherError::DuplicateFormat(_))
        ));
    }

    #[tokio::test]
    async fn typed_adapters_route_versions_separately_and_reject_duplicate_formats() {
        use crate::task::Task;
        use crate::worker::{TaskHandler, adapter::json::JsonTaskAdapter};
        use serde::{Deserialize, Serialize};
        use serde_json::json;

        #[derive(Serialize, Deserialize)]
        struct Payload<const VERSION: i32> {
            value: String,
        }
        impl<const VERSION: i32> Task for Payload<VERSION> {
            const TASK_TYPE: &'static str = "versioned_operation";
            const TASK_VERSION: i32 = VERSION;
        }
        struct Handler(Arc<Mutex<Vec<(i32, String)>>>);
        impl<const VERSION: i32> TaskHandler<Payload<VERSION>> for Handler {
            async fn handle(&self, payload: Payload<VERSION>, _: &TaskContext) -> TaskResult {
                self.0.lock().unwrap().push((VERSION, payload.value));
                TaskResult::Done
            }
        }

        enum VersionedAdapter {
            V1(JsonTaskAdapter<Payload<1>, Handler>),
            V2(JsonTaskAdapter<Payload<2>, Handler>),
        }
        impl TaskAdapter for VersionedAdapter {
            fn formats(&self) -> Vec<TaskFormat> {
                match self {
                    Self::V1(adapter) => adapter.formats(),
                    Self::V2(adapter) => adapter.formats(),
                }
            }
            async fn invoke(&self, ctx: &TaskContext) -> TaskResult {
                match self {
                    Self::V1(adapter) => adapter.invoke(ctx).await,
                    Self::V2(adapter) => adapter.invoke(ctx).await,
                }
            }
        }
        let calls = Arc::new(Mutex::new(Vec::new()));
        let first = TaskFormat::of::<Payload<1>>();
        let second = TaskFormat::of::<Payload<2>>();
        let dispatcher = TaskDispatcher::try_new(vec![
            VersionedAdapter::V1(JsonTaskAdapter::<Payload<1>, _>::new(Handler(calls.clone()))),
            VersionedAdapter::V2(JsonTaskAdapter::<Payload<2>, _>::new(Handler(calls.clone()))),
        ])
        .unwrap();
        assert_eq!(dispatcher.formats(), vec![first.clone(), second.clone()]);
        let base = claimed();
        for format in [second, first.clone()] {
            let task = TaskAttempt::try_new(
                base.attempt().task_id(),
                format,
                json!({"value":"decoded"}),
                base.attempt().metadata().clone(),
                1,
            )
            .unwrap();
            assert_eq!(dispatcher.dispatch(Arc::new(task), Duration::ZERO).await, TaskResult::Done);
        }
        assert_eq!(*calls.lock().unwrap(), vec![(2, "decoded".into()), (1, "decoded".into())]);
        assert!(matches!(TaskDispatcher::try_new(vec![
            JsonTaskAdapter::<Payload<1>, _>::new(Handler(calls.clone())),
            JsonTaskAdapter::<Payload<1>, _>::new(Handler(calls.clone())),
        ]), Err(TaskDispatcherError::DuplicateFormat(format)) if format == first));
        assert_eq!(calls.lock().unwrap().len(), 2);
    }

    trait Clock: Send + Sync {
        fn now(&self) -> u64;
    }
    struct Fixed(u64);
    impl Clock for Fixed {
        fn now(&self) -> u64 {
            self.0
        }
    }

    /// The pause, the attempt number and the clock's time a handler saw.
    type Seen = (Duration, i32, Option<u64>);

    /// Records what its context holds, and needs a clock.
    struct Inspecting(Arc<Mutex<Vec<Seen>>>);

    impl TaskAdapter for Inspecting {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![crate::worker::test_support::format()]
        }
        fn dependencies(&self) -> Vec<DependencyId> {
            vec![DependencyId::of::<dyn Clock>()]
        }
        async fn invoke(&self, ctx: &TaskContext) -> TaskResult {
            let clock = ctx.dependency::<dyn Clock>().ok().map(Clock::now);
            self.0
                .lock()
                .unwrap()
                .push((ctx.retry_pause(), ctx.attempt().number(), clock));
            TaskResult::retry("later")
        }
    }

    #[tokio::test]
    async fn every_handler_gets_a_context_with_the_attempt_the_pause_the_dependencies_and_the_executors() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let clock: Arc<dyn Clock> = Arc::new(Fixed(42));
        let dispatcher = TaskDispatcher::assemble(
            vec![Inspecting(seen.clone())],
            Dependencies::default().provide(clock),
            Executors::default(),
        )
        .unwrap();
        let task = claimed();
        assert_eq!(
            dispatcher
                .dispatch(Arc::new(task.attempt().clone()), Duration::from_secs(4))
                .await,
            TaskResult::retry("later")
        );
        assert_eq!(*seen.lock().unwrap(), [(Duration::from_secs(4), 2, Some(42))]);
    }

    #[test]
    fn a_missing_dependency_is_reported_without_the_private_task_type() {
        let refused = TaskDispatcher::try_new(vec![Inspecting(Arc::default())]).err().unwrap();
        let TaskDispatcherError::MissingDependency { format, dependency } = &refused else {
            panic!("{refused:?}");
        };
        assert_eq!(
            (format, *dependency),
            (&crate::worker::test_support::format(), DependencyId::of::<dyn Clock>().name())
        );
        assert!(
            refused.to_string().starts_with("the handler of dummy_task v1 needs `dyn "),
            "{refused}"
        );
        assert!(refused.to_string().ends_with("Clock`, which nothing provides"), "{refused}");
        let private = TaskDispatcherError::MissingDependency {
            format: TaskFormat::try_new("private_type\nmail@example.com", 3).unwrap(),
            dependency: "Clock",
        };
        let rendered = format!("{private} {private:?}");
        assert!(
            rendered.contains("unknown") && rendered.contains('3') && rendered.contains("Clock"),
            "{rendered}"
        );
        assert!(
            !rendered.contains("private_") && !rendered.contains("mail@example.com"),
            "{rendered}"
        );
        let twice = TaskDispatcherError::from(crate::worker::context::ExecutorTwice("Ledger"));
        assert_eq!(
            twice.to_string(),
            "the command executor of the aggregate type `Ledger` is registered twice"
        );
        assert_eq!(format!("{twice:?}"), "ExecutorTwice(ExecutorTwice(\"Ledger\"))");
        assert_eq!(
            MissingDependency("Clock").to_string(),
            "the dependency `Clock` is not provided"
        );
    }
}
