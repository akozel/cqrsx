//! A permanent Tokio task with the shared dispatcher and a cloned async-channel receiver.
use crate::task::{
    adapter::TaskAdapter,
    dispatcher::TaskDispatcher,
    executor::TaskExecutor,
    queue::{lease::LeasedTask, settlement::SettlementResult},
};
use async_channel::Receiver;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::error;

pub(super) async fn run_task_worker<E: TaskExecutor, A: TaskAdapter>(
    index: usize,
    dispatcher: Arc<TaskDispatcher<A>>,
    executor: Arc<E>,
    receiver: Receiver<LeasedTask>,
    capacity_available: Arc<Notify>,
    handler_timeout: Duration,
    stop: CancellationToken,
) -> usize {
    loop {
        let task = tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            task = receiver.recv() => task,
        };
        let Ok(task) = task else {
            break;
        };
        capacity_available.notify_one();
        if stop.is_cancelled() {
            break;
        }
        let id = task.attempt().task_id();
        // Graceful stop does not cancel the current invocation or settlement.
        match executor.execute_and_settle(&dispatcher, task, handler_timeout).await {
            Ok(SettlementResult::Failed(failed)) => crate::diagnostics::log_task_failure(&failed),
            Ok(SettlementResult::Completed | SettlementResult::RetryScheduled) => {}
            Ok(SettlementResult::OwnershipLost) => error!(task_id = %id, "task ownership lost"),
            Err(error) => error!(task_id = %id, error = %error, "task processing failed"),
        }
    }
    index
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_closed_channel_drains_its_ready_delivery_and_then_stops_the_worker() {
        use crate::task::{executor::default_executor::DefaultTaskExecutor, test_support::*};
        let queue = Arc::new(Queue::default());
        let (sender, receiver) = async_channel::bounded(1);
        let task = claimed();
        let id = task.attempt().task_id();
        sender.send(task).await.unwrap();
        drop(sender);
        let stop = CancellationToken::new();
        let index = run_task_worker(
            7,
            Arc::new(dispatcher(adapter())),
            Arc::new(DefaultTaskExecutor::new(
                queue.clone(),
                crate::task::queue::storage_config::TaskStorageConfig::default(),
            )),
            receiver,
            Arc::new(Notify::new()),
            Duration::from_secs(1),
            stop.clone(),
        )
        .await;
        assert_eq!(index, 7);
        assert!(!stop.is_cancelled());
        assert_eq!(
            *queue.outcomes.lock().unwrap(),
            vec![(id, crate::task::queue::settlement::SettlementRequest::Completed)]
        );
    }

    #[tokio::test]
    async fn cancellation_after_dequeue_prevents_handler_execution_and_settlement() {
        use crate::task::{executor::default_executor::DefaultTaskExecutor, test_support::*};
        use std::{
            future::Future,
            sync::atomic::{AtomicUsize, Ordering},
            task::{Context, Poll, Wake, Waker},
        };
        struct CancelOnWake(CancellationToken);
        impl Wake for CancelOnWake {
            fn wake(self: Arc<Self>) {
                self.0.cancel();
            }
        }
        let stop = CancellationToken::new();
        let capacity = Arc::new(Notify::new());
        let mut notification = Box::pin(capacity.notified());
        let waker = Waker::from(Arc::new(CancelOnWake(stop.clone())));
        assert!(matches!(
            notification.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        let queue = Arc::new(Queue::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let (sender, receiver) = async_channel::bounded(1);
        sender.send(claimed()).await.unwrap();
        let index = run_task_worker(
            3,
            Arc::new(dispatcher(CountingAdapter(calls.clone()))),
            Arc::new(DefaultTaskExecutor::new(
                queue.clone(),
                crate::task::queue::storage_config::TaskStorageConfig::default(),
            )),
            receiver.clone(),
            capacity.clone(),
            Duration::from_secs(1),
            stop.clone(),
        )
        .await;
        assert_eq!(index, 3);
        assert!(stop.is_cancelled());
        assert!(receiver.is_empty(), "cancellation must happen after recv consumed the lease");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            queue.outcomes.lock().unwrap().is_empty(),
            "the lease must remain available for durable recovery"
        );
    }
    #[test]
    fn cancelling_pending_receive_after_send_preserves_delivery_and_lease() {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        let (sender, receiver) = async_channel::bounded(1);
        let other = receiver.clone();
        let mut receive = Box::pin(receiver.recv());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(receive.as_mut().poll(&mut context), Poll::Pending));
        let expected = crate::task::test_support::claimed();
        assert!(sender.try_send(expected.clone()).is_ok());
        // Cancel after the waiter is notified, without polling it to Ready.
        drop(receive);
        let received = other.try_recv().unwrap();
        assert_eq!(received.attempt().task_id(), expected.attempt().task_id());
        assert_eq!(received.attempt().payload(), expected.attempt().payload());
        assert_eq!(received.lease().lock_token(), expected.lease().lock_token());
        assert_eq!(received.lease().deadline(), expected.lease().deadline());
        assert!(receiver.is_empty());
    }

    #[tokio::test]
    async fn idle_worker_cancels_pending_receive_while_sender_is_open() {
        use crate::task::{
            executor::default_executor::DefaultTaskExecutor,
            test_support::{Queue, adapter},
        };
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };

        let queue = Arc::new(Queue::default());
        let (sender, receiver) = async_channel::bounded(1);
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let mut worker = Box::pin(run_task_worker(
            0,
            Arc::new(TaskDispatcher::try_new(vec![adapter()]).unwrap()),
            Arc::new(DefaultTaskExecutor::new(
                queue.clone(),
                crate::task::queue::storage_config::TaskStorageConfig::default(),
            )),
            receiver.clone(),
            Arc::new(Notify::new()),
            Duration::from_secs(30),
            shutdown,
        ));
        // Poll explicitly so shutdown exercises a pending recv, without sleeps.
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(worker.as_mut().poll(&mut context), Poll::Pending));
        assert!(!sender.is_closed());
        stop.cancel();
        assert_eq!(tokio::time::timeout(Duration::from_secs(1), worker).await.unwrap(), 0);
        assert!(!sender.is_closed(), "shutdown must not depend on channel closure");
        assert!(receiver.is_empty());
        assert!(queue.outcomes.lock().unwrap().is_empty());
    }
}
