//! Public native ports must preserve borrowing, ownership and Send across Pending.
use cqrsx::task::{
    adapter::{TaskAdapter, json::JsonTaskAdapter},
    attempt::TaskAttempt,
    context::TaskContext,
    dispatcher::TaskDispatcher,
    executor::{TaskExecutor, default_executor::DefaultTaskExecutor},
    format::TaskFormat,
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
    future::{Future, poll_fn},
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::time::Instant;
use uuid::Uuid;

async fn suspend_once() {
    let mut suspended = false;
    poll_fn(|cx| {
        if suspended {
            Poll::Ready(())
        } else {
            suspended = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

async fn through_pending<F: Future + Send>(future: F) -> F::Output {
    let mut future = pin!(future);
    assert!(future.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    future.await
}

struct BorrowedQueue<'a> {
    format: &'a TaskFormat,
    owner: &'a str,
    leased: &'a LeasedTask,
    calls: &'a AtomicUsize,
    request_address: usize,
}
impl TaskQueue for BorrowedQueue<'_> {
    async fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
        suspend_once().await;
        assert!(std::ptr::eq(format, self.format));
        assert!(std::ptr::eq(owner, self.owner));
        assert_eq!(limit, 1);
        assert_eq!(lease, Duration::from_secs(60));
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(vec![self.leased.clone()])
    }
    async fn settle(&self, task: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
        suspend_once().await;
        assert!(std::ptr::eq(task, self.leased));
        let SettlementRequest::Failed { reason } = &request else {
            panic!("expected owned failure request")
        };
        assert_eq!(reason.as_ptr() as usize, self.request_address);
        assert_eq!(
            request,
            SettlementRequest::Failed {
                reason: "owned request".into()
            }
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(SettlementResult::OwnershipLost)
    }
    async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
        suspend_once().await;
        assert_eq!(limit, 7);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(LeaseRecoveryReport::new(7, vec![]))
    }
}

#[derive(Serialize, Deserialize)]
struct Payload {}
impl cqrsx::task::format::TaskPayload for Payload {
    const TASK_TYPE: &'static str = "native_runtime";
    const TASK_VERSION: i32 = 1;
}
struct Handler;
impl TaskHandler<Payload> for Handler {
    async fn handle(&self, _: Payload, _: &TaskContext) -> TaskResult {
        TaskResult::Done
    }
}
fn dispatcher() -> TaskDispatcher<JsonTaskAdapter<Payload, Handler>> {
    TaskDispatcher::try_new(vec![JsonTaskAdapter::<Payload, _>::new(Handler)]).unwrap()
}
fn leased() -> LeasedTask {
    LeasedTask::new(
        TaskAttempt::try_new(
            Uuid::nil(),
            TaskFormat::of::<Payload>(),
            serde_json::json!({}),
            serde_json::json!({"owned":"payload"}),
            1,
        )
        .unwrap(),
        TaskLease::new(Uuid::from_u128(1), Instant::now() + Duration::from_secs(60)),
    )
}

struct BorrowedExecutor<'a> {
    calls: &'a AtomicUsize,
    dispatcher_address: usize,
    payload_address: usize,
    deadline: Instant,
}
impl TaskExecutor for BorrowedExecutor<'_> {
    fn storage_timeout(&self) -> Duration {
        Duration::from_secs(6)
    }
    async fn execute_and_settle<A: TaskAdapter>(
        &self,
        dispatcher: &TaskDispatcher<A>,
        task: LeasedTask,
        timeout: Duration,
    ) -> Result<SettlementResult, TaskProcessingError> {
        suspend_once().await;
        assert_eq!(dispatcher as *const TaskDispatcher<A> as usize, self.dispatcher_address);
        assert_eq!(
            task.attempt().metadata()["owned"].as_str().unwrap().as_ptr() as usize,
            self.payload_address
        );
        assert_eq!(task.lease().deadline(), self.deadline);
        assert_eq!(timeout, Duration::from_secs(15));
        assert_eq!(
            dispatcher.dispatch(Arc::new(task.attempt().clone()), Duration::ZERO).await,
            TaskResult::Done
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(SettlementResult::Completed)
    }
}

#[tokio::test]
async fn all_four_port_futures_are_send_and_keep_original_inputs_across_pending() {
    let calls = AtomicUsize::new(0);
    let format = TaskFormat::of::<Payload>();
    let owner = String::from("borrowed owner");
    let task = leased();
    let reason = String::from("owned request");
    let request_address = reason.as_ptr() as usize;
    let queue = BorrowedQueue {
        format: &format,
        owner: &owner,
        leased: &task,
        calls: &calls,
        request_address,
    };
    let claimed = through_pending(TaskQueue::claim(&queue, &format, &owner, 1, Duration::from_secs(60)))
        .await
        .unwrap();
    assert_eq!(claimed[0].lease().deadline(), task.lease().deadline());
    let request = SettlementRequest::Failed { reason };
    assert_eq!(
        through_pending(TaskQueue::settle(&queue, &task, request)).await.unwrap(),
        SettlementResult::OwnershipLost
    );
    assert_eq!(
        through_pending(TaskQueue::recover_expired(&queue, 7))
            .await
            .unwrap()
            .retry_scheduled_count(),
        7
    );
    let dispatcher = dispatcher();
    let executor = BorrowedExecutor {
        calls: &calls,
        dispatcher_address: &dispatcher as *const _ as usize,
        payload_address: task.attempt().metadata()["owned"].as_str().unwrap().as_ptr() as usize,
        deadline: task.lease().deadline(),
    };
    assert_eq!(TaskExecutor::storage_timeout(&executor), Duration::from_secs(6));
    assert_eq!(
        through_pending(TaskExecutor::execute_and_settle(
            &executor,
            &dispatcher,
            task,
            Duration::from_secs(15)
        ))
        .await
        .unwrap(),
        SettlementResult::Completed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

// The standard constructor must accept a non-'static queue and remain inert.
struct SettlementQueue<'a>(&'a AtomicUsize);
impl TaskQueue for SettlementQueue<'_> {
    async fn claim(&self, _: &TaskFormat, _: &str, _: usize, _: Duration) -> Result<Vec<LeasedTask>, TaskProcessingError> {
        unreachable!()
    }
    async fn recover_expired(&self, _: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
        unreachable!()
    }
    async fn settle(&self, task: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
        suspend_once().await;
        assert_eq!(task.attempt().task_id(), Uuid::nil());
        assert_eq!(request, SettlementRequest::Completed);
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(SettlementResult::Completed)
    }
}
#[tokio::test]
async fn standard_constructor_supports_borrowed_queue_without_io() {
    let calls = AtomicUsize::new(0);
    let executor = DefaultTaskExecutor::new(Arc::new(SettlementQueue(&calls)), TaskStorageConfig::default());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        through_pending(TaskExecutor::execute_and_settle(
            &executor,
            &dispatcher(),
            leased(),
            Duration::from_secs(15)
        ))
        .await
        .unwrap(),
        SettlementResult::Completed
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
