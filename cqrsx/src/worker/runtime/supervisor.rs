//! Bounded admission and coordination of polling, worker exits and shutdown.
use super::{poller::Poller, worker_pool::WorkerPool};
use crate::worker::{
    adapter::TaskAdapter,
    dispatcher::TaskDispatcher,
    executor::TaskExecutor,
    queue::{TaskQueue, lease::LeasedTask},
    runtime::{TaskRuntimeError, worker_pool_config::TaskWorkerPoolConfig},
};
use async_channel::Sender;
use std::sync::Arc;
use tokio::{sync::Notify, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

// Expired-lease recovery returns at most this many rows per poller tick, in one statement.
const RECOVERY_BATCH_LIMIT: usize = 32;
/// One producer claims once fewer deliveries than the refill level wait, up to the free capacity of the
/// bounded channel; at most C deliveries run and Q wait.
/// Every worker shares the one dispatcher, and a worker restarted after it died (a panic outside every handler, e.g. in
/// an executor of one's own: the executor catches a handler's panic) gets it again. Token cancellation
/// stops admission/dequeue and drains only started invocations for a common H+S budget.
/// A configuration error cancels the supplied token before returning. Pass a
/// child token to isolate a pool; clones share cancellation with the host. Dropping a
/// token handle does not request shutdown: lifecycle owners should use a drop guard.
/// Dropping/aborting this supervisor cancels the token and aborts its owned workers;
/// asynchronous graceful draining requires cancelling the token and awaiting `run`.
/// Original lease deadlines and database fencing remain authoritative.
pub async fn run<Q: TaskQueue + 'static, E: TaskExecutor + 'static, A: TaskAdapter + 'static>(
    queue: Arc<Q>,
    executor: Arc<E>,
    dispatcher: TaskDispatcher<A>,
    config: TaskWorkerPoolConfig,
    shutdown: CancellationToken,
) -> Result<(), TaskRuntimeError> {
    let _cancel_on_exit = shutdown.clone().drop_guard();
    config.validate_for_storage_timeout(executor.storage_timeout())?;
    run_prepared(
        super::RuntimeContext::new(queue, executor),
        config,
        Arc::new(dispatcher),
        shutdown,
    )
    .await
}

/// Shared execution engine for a ready dispatcher and the low-level entry point.
pub(super) async fn run_prepared<Q: TaskQueue + 'static, E: TaskExecutor + 'static, A: TaskAdapter + 'static>(
    context: super::RuntimeContext<Q, E>,
    config: TaskWorkerPoolConfig,
    dispatcher: Arc<TaskDispatcher<A>>,
    shutdown: CancellationToken,
) -> Result<(), TaskRuntimeError> {
    let _cancel_on_exit = shutdown.clone().drop_guard();
    if shutdown.is_cancelled() {
        return Ok(());
    }
    let (queue, executor) = context.into_parts();
    let formats = dispatcher.formats().to_vec();
    let mut poller = Poller::new(queue, formats, executor.storage_timeout());
    let owner = Uuid::new_v4().to_string();
    let (sender, receiver) = async_channel::bounded(config.buffer_capacity());
    let capacity_available = Arc::new(Notify::new());
    let mut pool = WorkerPool::new(
        executor.clone(),
        receiver,
        capacity_available.clone(),
        config.handler_timeout(),
        shutdown.clone(),
    );
    for index in 0..config.concurrency() {
        pool.spawn(index, dispatcher.clone());
    }
    info!(concurrency = config.concurrency(), buffer_capacity = config.buffer_capacity(),
        buffer_refill_percent = config.buffer_refill_percent(),
        lease_seconds = config.lease_duration().as_secs(), owner = %owner, "task runtime started");
    let refill_level = config.buffer_refill_level();
    let mut interval = tokio::time::interval(config.poll_interval());
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            Some(exit) = pool.next_exit() => {
                let index = exit.index;
                error!(worker = index, tokio_task_id = %exit.tokio_task_id,
                    panicked = exit.panicked, cancelled = exit.cancelled,
                    owner = %owner, "task worker stopped; restarting");
                if shutdown.is_cancelled() {
                    break;
                }
                pool.spawn(index, dispatcher.clone());
            }
            _ = interval.tick() => {
                let recovery = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    result = poller.recover_expired(config.concurrency().min(RECOVERY_BATCH_LIMIT)) => result,
                };
                match recovery {
                    Ok(report) => for failure in report.failures() { crate::diagnostics::log_task_failure(failure); },
                    Err(error) => error!(error = %error, "task lease recovery failed"),
                }
            }
            _ = capacity_available.notified() => {},
        }
        // Claim once fewer than the refill level wait: one claim then fills the buffer instead
        // of one claim per freed slot. The level is at most the capacity, so a full buffer waits too.
        if sender.len() >= refill_level {
            continue;
        }
        let limit = config.buffer_capacity() - sender.len();
        // This is the only producer. While claim awaits, consumers can only
        // reduce occupancy, so the returned batch fits without reservations.
        let claimed = tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            result = poller.claim(&owner, limit, config.lease_duration()) => result,
        };
        match claimed {
            Ok(tasks) => {
                if shutdown.is_cancelled() {
                    break;
                }
                admit(&sender, tasks, limit);
            }
            Err(error) => error!(error = %error, "task claim failed"),
        }
    }
    // Notify the host before waiting for healthy workers.
    shutdown.cancel();
    pool.drain().await;
    drop(sender);
    Ok(())
}

/// Puts a claimed batch into the bounded channel of the workers. `TaskQueue::claim` returns at most `limit` tasks, and
/// this is the only producer, so the batch fits: while the claim awaited, the workers could only free slots. A queue that
/// breaks the contract (or a broken invariant here) does not stop the runtime: the tasks that do not fit are dropped with
/// an `error`, still leased, and the queue's lease recovery returns them.
fn admit(sender: &Sender<LeasedTask>, tasks: Vec<LeasedTask>, limit: usize) {
    if tasks.len() > limit {
        error!(
            claimed = tasks.len(),
            limit, "TaskQueue::claim broke its contract: more tasks than the limit; the extra tasks wait for lease recovery"
        );
    }
    let admitted = tasks.into_iter().take(limit);
    for task in admitted {
        // The error holds the LeasedTask: never format it, its payload and lease token are private.
        if sender.try_send(task).is_err() {
            error!("a claimed task did not fit the workers' channel of its single producer; it waits for lease recovery");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::worker::{executor::default_executor::DefaultTaskExecutor, test_support::*};
    use std::{sync::atomic::Ordering, time::Duration};
    async fn wait_until(condition: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
    }
    fn worker_config(c: usize, q: usize, poll: Duration) -> TaskWorkerPoolConfig {
        TaskWorkerPoolConfig::try_new(c, q, poll, Duration::from_secs(30), Duration::from_secs(120)).unwrap()
    }

    #[tokio::test]
    async fn startup_diagnostics_report_effective_worker_limits_and_owner() {
        let queue = Arc::new(Queue::default());
        let stop = CancellationToken::new();
        let config = worker_config(2, 3, Duration::from_secs(3600));
        let (_, log) = crate::diagnostics::test_support::capture_at(tracing::Level::INFO, async {
            let stopping = async {
                wait_until(|| queue.recovery_calls.load(Ordering::SeqCst) > 0).await;
                stop.cancel();
            };
            let (result, ()) = tokio::join!(
                super::run(
                    queue.clone(),
                    Arc::new(DefaultTaskExecutor::new(
                        queue.clone(),
                        crate::worker::queue::storage_config::TaskStorageConfig::default()
                    )),
                    TaskDispatcher::try_new(vec![adapter()]).unwrap(),
                    config.clone(),
                    stop.clone()
                ),
                stopping
            );
            result.unwrap();
        })
        .await;
        assert_eq!(log.matches("task runtime started").count(), 1);
        assert!(log.contains("concurrency=2 buffer_capacity=3 buffer_refill_percent=25"));
        assert!(log.contains(&format!("lease_seconds={}", config.lease_duration().as_secs())));
        let owner = log.split("owner=").nth(1).unwrap().split_whitespace().next().unwrap();
        assert!(Uuid::parse_str(owner).is_ok());
        assert!(queue.outcomes.lock().unwrap().is_empty());
    }

    /// A queue that claims more than the limit breaks the `TaskQueue` contract: the runtime logs it, admits the limit,
    /// leaves the rest to lease recovery and goes on.
    #[tokio::test]
    async fn a_queue_that_claims_beyond_the_limit_is_logged_and_the_runtime_goes_on() {
        let queue = Arc::new(Queue::default());
        queue.claim_beyond_limit.store(true, Ordering::SeqCst);
        queue.incoming.lock().unwrap().extend([claimed(), claimed(), claimed()]);
        let stop = CancellationToken::new();
        let (_, log) = crate::diagnostics::test_support::capture(async {
            let stopping = async {
                wait_until(|| queue.outcomes.lock().unwrap().len() == 1).await;
                // Another poll round after the broken claim: the supervisor is alive.
                let claims = queue.claim_limits.lock().unwrap().len();
                wait_until(|| queue.claim_limits.lock().unwrap().len() > claims).await;
                stop.cancel();
            };
            let (result, ()) = tokio::join!(
                super::run(
                    queue.clone(),
                    Arc::new(DefaultTaskExecutor::new(
                        queue.clone(),
                        crate::worker::queue::storage_config::TaskStorageConfig::default()
                    )),
                    TaskDispatcher::try_new(vec![adapter()]).unwrap(),
                    worker_config(1, 1, Duration::from_millis(2)),
                    stop.clone()
                ),
                stopping
            );
            result.unwrap();
        })
        .await;
        assert_eq!(queue.claim_limits.lock().unwrap()[0], 1);
        assert_eq!(
            queue.outcomes.lock().unwrap().len(),
            1,
            "the extra tasks wait for lease recovery"
        );
        assert!(
            log.contains("TaskQueue::claim broke its contract") && log.contains("claimed=3 limit=1"),
            "{log}"
        );
    }

    /// A batch that does not fit the channel (a broken invariant of the single producer) is dropped with an error, not a
    /// panic.
    #[tokio::test]
    async fn a_task_that_does_not_fit_the_channel_is_logged_and_dropped() {
        let (sender, receiver) = async_channel::bounded(1);
        let (_, log) = crate::diagnostics::test_support::capture(async {
            admit(&sender, vec![claimed(), claimed()], 2);
        })
        .await;
        assert_eq!(receiver.len(), 1);
        assert!(log.contains("did not fit the workers' channel"), "{log}");
        assert!(!log.contains("broke its contract"), "{log}");
    }
}
