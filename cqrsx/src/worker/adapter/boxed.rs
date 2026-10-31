//! One adapter type over adapters of different types, so one dispatcher can hold them all.

use crate::task::{Task, format::TaskFormat};
use crate::worker::{
    TaskHandler, TaskResult,
    adapter::{TaskAdapter, json::JsonTaskAdapter},
    context::TaskContext,
    dependencies::DependencyId,
};
use serde::de::DeserializeOwned;
use std::{fmt, future::Future, pin::Pin};

type InvokeFuture<'a> = Pin<Box<dyn Future<Output = TaskResult> + Send + 'a>>;

/// Object-safe shadow of [`TaskAdapter`]: the same contract with a boxed future. Method names
/// differ from the original so the blanket implementation never makes a call ambiguous.
trait DynTaskAdapter: Send + Sync {
    fn dyn_formats(&self) -> Vec<TaskFormat>;
    fn dyn_dependencies(&self) -> Vec<DependencyId>;
    fn dyn_invoke<'a>(&'a self, ctx: &'a TaskContext) -> InvokeFuture<'a>;
}

impl<T: TaskAdapter> DynTaskAdapter for T {
    fn dyn_formats(&self) -> Vec<TaskFormat> {
        self.formats()
    }

    fn dyn_dependencies(&self) -> Vec<DependencyId> {
        self.dependencies()
    }

    fn dyn_invoke<'a>(&'a self, ctx: &'a TaskContext) -> InvokeFuture<'a> {
        Box::pin(self.invoke(ctx))
    }
}

/// Any [`TaskAdapter`] behind one type, so one
/// [`TaskDispatcher`](crate::worker::dispatcher::TaskDispatcher) can route to handlers of different
/// types without an application-owned enum.
///
/// Formats, dependencies and results pass through unchanged; the wrapped adapter is shared like any
/// other. The cost is one allocation per invocation. Wrapped adapters must be `'static`.
pub struct BoxedTaskAdapter(Box<dyn DynTaskAdapter>);

// By hand: the wrapped adapter is a trait object; it prints as the formats it handles and what it depends on.
impl fmt::Debug for BoxedTaskAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoxedTaskAdapter")
            .field("formats", &self.0.dyn_formats())
            .field("dependencies", &self.0.dyn_dependencies())
            .finish_non_exhaustive()
    }
}

impl BoxedTaskAdapter {
    /// Erase the type of `adapter`.
    pub fn new(adapter: impl TaskAdapter + 'static) -> Self {
        Self(Box::new(adapter))
    }

    /// A [`JsonTaskAdapter`] for `handler`.
    pub fn json<T, H>(handler: H) -> Self
    where
        T: Task + DeserializeOwned + Send + 'static,
        H: TaskHandler<T> + 'static,
    {
        Self::new(JsonTaskAdapter::<T, H>::new(handler))
    }
}

impl TaskAdapter for BoxedTaskAdapter {
    fn formats(&self) -> Vec<TaskFormat> {
        self.0.dyn_formats()
    }

    fn dependencies(&self) -> Vec<DependencyId> {
        self.0.dyn_dependencies()
    }

    async fn invoke(&self, ctx: &TaskContext) -> TaskResult {
        self.0.dyn_invoke(ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::attempt::TaskAttempt;
    use crate::worker::dispatcher::{TaskDispatcher, TaskDispatcherError};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicU32, Ordering},
        },
        time::Duration,
    };
    use uuid::Uuid;

    #[derive(Serialize, Deserialize)]
    struct Greeting {
        name: String,
    }
    impl Task for Greeting {
        const TASK_TYPE: &'static str = "greeting";
        const TASK_VERSION: i32 = 1;
    }

    #[derive(Serialize, Deserialize)]
    struct Invoice {
        amount: u32,
    }
    impl Task for Invoice {
        const TASK_TYPE: &'static str = "invoice";
        const TASK_VERSION: i32 = 2;
    }

    type Calls = Arc<Mutex<Vec<String>>>;

    /// Counts its own invocations and records them with the decoded payload.
    struct Recorder {
        calls: Calls,
        count: AtomicU32,
        result: TaskResult,
    }

    impl Recorder {
        fn new(calls: &Calls) -> Self {
            Self::returning(calls, TaskResult::Done)
        }

        fn returning(calls: &Calls, result: TaskResult) -> Self {
            Self {
                calls: calls.clone(),
                count: AtomicU32::new(0),
                result,
            }
        }

        fn next(&self) -> u32 {
            self.count.fetch_add(1, Ordering::SeqCst) + 1
        }
    }

    impl TaskHandler<Greeting> for Recorder {
        async fn handle(&self, payload: Greeting, _: &TaskContext) -> TaskResult {
            tokio::task::yield_now().await;
            let count = self.next();
            self.calls.lock().unwrap().push(format!("greeting {} #{count}", payload.name));
            self.result.clone()
        }
    }

    impl TaskHandler<Invoice> for Recorder {
        async fn handle(&self, payload: Invoice, _: &TaskContext) -> TaskResult {
            let count = self.next();
            self.calls
                .lock()
                .unwrap()
                .push(format!("invoice {} #{count}", payload.amount));
            self.result.clone()
        }
    }

    fn attempt<T: Task>(payload: serde_json::Value) -> TaskAttempt {
        TaskAttempt::try_new(Uuid::new_v4(), TaskFormat::of::<T>(), payload, json!({}), 1).unwrap()
    }

    #[tokio::test]
    async fn one_dispatcher_routes_differently_typed_adapters_by_format_in_registration_order() {
        let calls = Calls::default();
        let dispatcher = TaskDispatcher::try_new(vec![
            BoxedTaskAdapter::json::<Invoice, _>(Recorder::new(&calls)),
            BoxedTaskAdapter::json::<Greeting, _>(Recorder::new(&calls)),
        ])
        .unwrap();
        assert_eq!(
            dispatcher.formats(),
            [TaskFormat::of::<Invoice>(), TaskFormat::of::<Greeting>()]
        );
        let invoice = r#"TaskFormat { task_type: "invoice", task_version: 2 }"#;
        let greeting = r#"TaskFormat { task_type: "greeting", task_version: 1 }"#;
        assert_eq!(
            format!("{dispatcher:?}"),
            format!(
                "TaskDispatcher {{ formats: [{invoice}, {greeting}], dependencies: Dependencies {{ count: 0 }}, \
                 executors: Executors {{ count: 0 }}, .. }}"
            )
        );
        assert_eq!(
            format!("{:?}", BoxedTaskAdapter::json::<Invoice, _>(Recorder::new(&calls))),
            format!("BoxedTaskAdapter {{ formats: [{invoice}], dependencies: [], .. }}")
        );
        for task in [
            attempt::<Greeting>(json!({ "name": "fox" })),
            attempt::<Invoice>(json!({ "amount": 80 })),
            attempt::<Greeting>(json!({ "name": "panda" })),
        ] {
            assert_eq!(dispatcher.dispatch(Arc::new(task), Duration::ZERO).await, TaskResult::Done);
        }
        assert_eq!(
            *calls.lock().unwrap(),
            ["greeting fox #1", "invoice 80 #1", "greeting panda #2"],
            "each adapter keeps its own mutable handler state"
        );
    }

    #[test]
    fn a_duplicate_format_is_rejected_before_registration() {
        let calls = Calls::default();
        assert!(matches!(
            TaskDispatcher::try_new(vec![
                BoxedTaskAdapter::json::<Greeting, _>(Recorder::new(&calls)),
                BoxedTaskAdapter::json::<Greeting, _>(Recorder::new(&calls)),
            ]),
            Err(TaskDispatcherError::DuplicateFormat(format)) if format == TaskFormat::of::<Greeting>()
        ));
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn handler_results_pass_through_unchanged() {
        for result in [TaskResult::Done, TaskResult::retry("later"), TaskResult::fail("rejected")] {
            let calls = Calls::default();
            let adapter = BoxedTaskAdapter::json::<Greeting, _>(Recorder::returning(&calls, result.clone()));
            let actual = adapter
                .invoke(&TaskContext::new(
                    attempt::<Greeting>(json!({ "name": "fox" })),
                    Duration::ZERO,
                ))
                .await;
            assert_eq!(format!("{actual:?}"), format!("{result:?}"));
            assert_eq!(calls.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn a_custom_adapter_keeps_its_formats_and_decoding() {
        /// Accepts two versions of one operation and decodes neither through serde.
        struct MultiVersion {
            seen: Calls,
        }
        impl TaskAdapter for MultiVersion {
            fn formats(&self) -> Vec<TaskFormat> {
                vec![
                    TaskFormat::try_new("legacy", 1).unwrap(),
                    TaskFormat::try_new("legacy", 2).unwrap(),
                ]
            }
            async fn invoke(&self, ctx: &TaskContext) -> TaskResult {
                self.seen.lock().unwrap().push(ctx.attempt().format().to_string());
                TaskResult::Done
            }
        }
        let seen = Calls::default();
        let dispatcher = TaskDispatcher::try_new(vec![
            BoxedTaskAdapter::new(MultiVersion { seen: seen.clone() }),
            BoxedTaskAdapter::json::<Greeting, _>(Recorder::new(&seen)),
        ])
        .unwrap();
        assert_eq!(dispatcher.formats().len(), 3);
        for version in [2, 1] {
            let task = TaskAttempt::try_new(
                Uuid::new_v4(),
                TaskFormat::try_new("legacy", version).unwrap(),
                json!({}),
                json!({}),
                1,
            )
            .unwrap();
            assert_eq!(dispatcher.dispatch(Arc::new(task), Duration::ZERO).await, TaskResult::Done);
        }
        assert_eq!(*seen.lock().unwrap(), ["legacy v2", "legacy v1"]);
    }
}
