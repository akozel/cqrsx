//! Adapter contracts work without worker/postgres features or erased futures.
use cqrsx::task::{
    adapter::{TaskAdapter, json::JsonTaskAdapter},
    attempt::TaskAttempt,
    context::TaskContext,
    dispatcher::TaskDispatcher,
    executor::{TaskExecutor, default_executor::DefaultTaskExecutor},
    format::{TaskFormat, TaskPayload},
    handler::TaskHandler,
    queue::{
        TaskProcessingError, TaskQueue,
        lease::{LeasedTask, TaskLease},
        settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult},
        storage_config::TaskStorageConfig,
    },
    result::TaskResult,
};
use serde::{Deserialize, Serialize};
use std::{
    cell::Cell,
    future::Future,
    pin::pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Waker},
    time::Duration,
};

async fn pending<F: Future + Send>(future: F) -> F::Output {
    let mut future = pin!(future);
    assert!(future.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    future.await
}

#[derive(Serialize, Deserialize)]
struct Payload {
    value: String,
    #[serde(skip)]
    mutable: Cell<u8>,
}
impl TaskPayload for Payload {
    const TASK_TYPE: &'static str = "native_adapter";
    const TASK_VERSION: i32 = 1;
}
// The payload's Cell rules out Sync/Clone requirements on payloads; the handler borrows shared state.
struct BorrowedHandler<'a> {
    calls: &'a AtomicUsize,
    original: &'a TaskAttempt,
}
impl TaskHandler<Payload> for BorrowedHandler<'_> {
    async fn handle(&self, payload: Payload, ctx: &TaskContext) -> TaskResult {
        tokio::task::yield_now().await;
        assert!(std::ptr::eq(ctx.attempt(), self.original));
        assert_eq!(payload.value, "owned");
        payload.mutable.set(1);
        self.calls.fetch_add(1, Ordering::SeqCst);
        TaskResult::Done
    }
}
struct BorrowedAdapter<'a> {
    calls: &'a AtomicUsize,
    original: &'a TaskAttempt,
    state: AtomicUsize,
}
impl TaskAdapter for BorrowedAdapter<'_> {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![self.original.format().clone()]
    }
    async fn invoke(&self, ctx: &TaskContext) -> TaskResult {
        tokio::task::yield_now().await;
        assert!(std::ptr::eq(ctx.attempt(), self.original));
        self.state.fetch_add(1, Ordering::SeqCst);
        self.calls.fetch_add(1, Ordering::SeqCst);
        TaskResult::Done
    }
}
fn attempt() -> Arc<TaskAttempt> {
    Arc::new(
        TaskAttempt::new(
            uuid::Uuid::nil(),
            TaskFormat::of::<Payload>().unwrap(),
            serde_json::json!({"value":"owned"}),
            serde_json::json!({"original":"metadata"}),
            1,
        )
        .unwrap(),
    )
}
#[tokio::test]
async fn native_invoke_and_dispatch_borrow_original_attempt_and_shared_state_through_pending() {
    let original = attempt();
    let ctx = TaskContext::new(original.clone(), Duration::ZERO);
    let calls = AtomicUsize::new(0);
    {
        let adapter = BorrowedAdapter {
            calls: &calls,
            original: &original,
            state: AtomicUsize::new(0),
        };
        for _ in 0..2 {
            assert_eq!(pending(TaskAdapter::invoke(&adapter, &ctx)).await, TaskResult::Done);
        }
        assert_eq!(adapter.state.load(Ordering::SeqCst), 2);
        let dispatcher = TaskDispatcher::try_new(vec![adapter]).unwrap();
        assert_eq!(
            pending(dispatcher.dispatch(original.clone(), Duration::ZERO)).await,
            TaskResult::Done
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    {
        let adapter = JsonTaskAdapter::<Payload, _>::new(BorrowedHandler {
            calls: &calls,
            original: &original,
        })
        .unwrap();
        assert_eq!(pending(TaskAdapter::invoke(&adapter, &ctx)).await, TaskResult::Done);
        let dispatcher = TaskDispatcher::try_new(vec![adapter]).unwrap();
        assert_eq!(
            pending(dispatcher.dispatch(original.clone(), Duration::ZERO)).await,
            TaskResult::Done
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 5);
}

struct Queue;
impl TaskQueue for Queue {
    async fn claim(&self, _: &TaskFormat, _: &str, _: usize, _: Duration) -> Result<Vec<LeasedTask>, TaskProcessingError> {
        unreachable!()
    }
    async fn settle(&self, _: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
        assert_eq!(request, SettlementRequest::Completed);
        Ok(SettlementResult::Completed)
    }
    async fn recover_expired(&self, _: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
        unreachable!()
    }
}
struct Adapter;
impl TaskAdapter for Adapter {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![TaskFormat::of::<Payload>().unwrap()]
    }
    async fn invoke(&self, _: &TaskContext) -> TaskResult {
        tokio::task::yield_now().await;
        TaskResult::Done
    }
}
#[tokio::test]
async fn one_standard_executor_accepts_different_concrete_adapter_types() {
    let executor = DefaultTaskExecutor::new(Arc::new(Queue), TaskStorageConfig::default());
    let first = TaskDispatcher::try_new(vec![Adapter]).unwrap();
    let second = TaskDispatcher::try_new(vec![JsonTaskAdapter::<Payload, _>::new(OwnedHandler).unwrap()]).unwrap();
    macro_rules! execute {
        ($dispatcher:expr) => {{
            let task = LeasedTask::new(
                (*attempt()).clone(),
                TaskLease::new(uuid::Uuid::nil(), tokio::time::Instant::now() + Duration::from_secs(60)),
            );
            assert_eq!(
                pending(TaskExecutor::execute_and_settle(
                    &executor,
                    $dispatcher,
                    task,
                    Duration::from_secs(15)
                ))
                .await
                .unwrap(),
                SettlementResult::Completed
            );
        }};
    }
    execute!(&first);
    execute!(&second);
}
struct OwnedHandler;
impl TaskHandler<Payload> for OwnedHandler {
    async fn handle(&self, payload: Payload, _: &TaskContext) -> TaskResult {
        tokio::task::yield_now().await;
        payload.mutable.set(1);
        TaskResult::Done
    }
}

#[tokio::test]
async fn custom_adapter_converts_two_wire_versions_without_changing_routing() {
    struct Versions<'a>(&'a Mutex<Vec<String>>);
    impl TaskAdapter for Versions<'_> {
        fn formats(&self) -> Vec<TaskFormat> {
            (1..=2)
                .map(|version| TaskFormat::new("custom_versioned", version).unwrap())
                .collect()
        }
        async fn invoke(&self, ctx: &TaskContext) -> TaskResult {
            let task = ctx.attempt();
            let key = match task.format().task_version() {
                1 => "old_value",
                2 => "value",
                _ => unreachable!(),
            };
            let current = task.payload()[key].as_str().unwrap().to_owned();
            tokio::task::yield_now().await;
            assert_eq!(task.metadata()["original"], "metadata");
            self.0.lock().unwrap().push(current);
            TaskResult::Done
        }
    }
    let calls = Mutex::new(Vec::new());
    {
        let dispatcher = TaskDispatcher::try_new(vec![Versions(&calls)]).unwrap();
        assert_eq!(
            dispatcher.formats().iter().map(TaskFormat::task_version).collect::<Vec<_>>(),
            [1, 2]
        );
        for (version, payload) in [
            (1, serde_json::json!({"old_value":"first"})),
            (2, serde_json::json!({"value":"second"})),
        ] {
            let task = TaskAttempt::new(
                uuid::Uuid::nil(),
                TaskFormat::new("custom_versioned", version).unwrap(),
                payload,
                serde_json::json!({"original":"metadata"}),
                1,
            )
            .unwrap();
            assert_eq!(pending(dispatcher.dispatch(task, Duration::ZERO)).await, TaskResult::Done);
        }
    }
    assert_eq!(*calls.lock().unwrap(), ["first", "second"]);
}
