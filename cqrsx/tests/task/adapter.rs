//! Adapter contracts work without worker/postgres features or erased futures.
use cqrsx::task::{
    adapter::{TaskAdapter, json::JsonTaskAdapter},
    attempt::TaskAttempt,
    dispatcher::TaskDispatcher,
    executor::{TaskExecutor, default_executor::DefaultTaskExecutor},
    format::{TaskFormat, TaskPayload},
    handler::{TaskHandler, TaskHandlerError},
    queue::{
        TaskProcessingError, TaskQueue,
        lease::{LeasedTask, TaskLease},
        settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult},
        storage_config::TaskStorageConfig,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    cell::Cell,
    future::Future,
    pin::pin,
    sync::Arc,
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
// Cell deliberately rules out Sync/Clone requirements on a borrowed handler.
struct BorrowedHandler<'a> {
    calls: &'a mut usize,
    original: &'a TaskAttempt,
}
impl TaskHandler<Payload> for BorrowedHandler<'_> {
    async fn handle(&mut self, payload: Payload, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        tokio::task::yield_now().await;
        assert!(std::ptr::eq(attempt, self.original));
        assert_eq!(payload.value, "owned");
        payload.mutable.set(1);
        *self.calls += 1;
        Ok(())
    }
}
struct BorrowedAdapter<'a> {
    calls: &'a mut usize,
    original: &'a TaskAttempt,
    state: Cell<usize>,
}
impl TaskAdapter for BorrowedAdapter<'_> {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![self.original.format().clone()]
    }
    async fn invoke(&mut self, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        tokio::task::yield_now().await;
        assert!(std::ptr::eq(attempt, self.original));
        self.state.set(self.state.get() + 1);
        *self.calls += 1;
        Ok(())
    }
}
fn attempt() -> TaskAttempt {
    TaskAttempt::new(
        uuid::Uuid::nil(),
        TaskFormat::of::<Payload>().unwrap(),
        serde_json::json!({"value":"owned"}),
        serde_json::json!({"original":"metadata"}),
        1,
    )
    .unwrap()
}
#[tokio::test]
async fn native_invoke_and_dispatch_borrow_original_attempt_and_send_only_state_through_pending() {
    let original = attempt();
    let mut calls = 0;
    {
        let mut adapter = BorrowedAdapter {
            calls: &mut calls,
            original: &original,
            state: Cell::new(0),
        };
        for _ in 0..2 {
            pending(TaskAdapter::invoke(&mut adapter, &original)).await.unwrap();
        }
        assert_eq!(adapter.state.get(), 2);
        let mut dispatcher = TaskDispatcher::try_new(vec![adapter]).unwrap();
        pending(dispatcher.dispatch(&original)).await.unwrap();
    }
    assert_eq!(calls, 3);
    {
        let mut adapter = JsonTaskAdapter::<Payload, _>::new(BorrowedHandler {
            calls: &mut calls,
            original: &original,
        })
        .unwrap();
        pending(TaskAdapter::invoke(&mut adapter, &original)).await.unwrap();
        let mut dispatcher = TaskDispatcher::try_new(vec![adapter]).unwrap();
        pending(dispatcher.dispatch(&original)).await.unwrap();
    }
    assert_eq!(calls, 5);
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
    async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
        tokio::task::yield_now().await;
        Ok(())
    }
}
#[tokio::test]
async fn one_standard_executor_accepts_different_concrete_adapter_types() {
    let executor = DefaultTaskExecutor::new(Arc::new(Queue), TaskStorageConfig::default());
    let mut first = TaskDispatcher::try_new(vec![Adapter]).unwrap();
    let mut second = TaskDispatcher::try_new(vec![JsonTaskAdapter::<Payload, _>::new(OwnedHandler).unwrap()]).unwrap();
    macro_rules! execute {
        ($dispatcher:expr) => {{
            let task = LeasedTask::new(
                attempt(),
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
    execute!(&mut first);
    execute!(&mut second);
}
struct OwnedHandler;
impl TaskHandler<Payload> for OwnedHandler {
    async fn handle(&mut self, payload: Payload, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
        tokio::task::yield_now().await;
        payload.mutable.set(1);
        Ok(())
    }
}

#[tokio::test]
async fn custom_adapter_converts_two_wire_versions_without_changing_routing() {
    struct Versions<'a>(&'a mut Vec<String>);
    impl TaskAdapter for Versions<'_> {
        fn formats(&self) -> Vec<TaskFormat> {
            (1..=2)
                .map(|version| TaskFormat::new("custom_versioned", version).unwrap())
                .collect()
        }
        async fn invoke(&mut self, task: &TaskAttempt) -> Result<(), TaskHandlerError> {
            let key = match task.format().task_version() {
                1 => "old_value",
                2 => "value",
                _ => unreachable!(),
            };
            let current = task.payload()[key].as_str().unwrap().to_owned();
            tokio::task::yield_now().await;
            assert_eq!(task.metadata()["original"], "metadata");
            self.0.push(current);
            Ok(())
        }
    }
    let mut calls = Vec::new();
    {
        let mut dispatcher = TaskDispatcher::try_new(vec![Versions(&mut calls)]).unwrap();
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
            pending(dispatcher.dispatch(&task)).await.unwrap();
        }
    }
    assert_eq!(calls, ["first", "second"]);
}
