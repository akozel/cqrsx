use super::*;
use cqrsx::task::{
    executor::{TaskExecutor, default_executor::HANDLER_PANICKED},
    policy::{Policies, PolicyName, Retry, TaskPolicy},
};

/// An executor of one's own that panics on its first attempt, outside every handler, so that its worker dies: what the
/// supervisor's restart still guards against. Records the Tokio task of every attempt; may ask for shutdown first.
struct PanicOnce {
    inner: DefaultTaskExecutor<Queue>,
    attempts: AtomicUsize,
    workers: std::sync::Mutex<Vec<Id>>,
    stop: Option<CancellationToken>,
}

impl PanicOnce {
    fn new(queue: Arc<Queue>, stop: Option<CancellationToken>) -> Self {
        Self {
            inner: DefaultTaskExecutor::new(queue, TaskStorageConfig::default()),
            attempts: AtomicUsize::new(0),
            workers: std::sync::Mutex::default(),
            stop,
        }
    }
}

impl TaskExecutor for PanicOnce {
    fn storage_timeout(&self) -> Duration {
        self.inner.storage_timeout()
    }

    async fn execute_and_settle<A: TaskAdapter>(
        &self,
        dispatcher: &TaskDispatcher<A>,
        task: LeasedTask,
        handler_timeout: Duration,
    ) -> Result<SettlementResult, TaskProcessingError> {
        self.workers.lock().unwrap().push(tokio::task::id());
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            if let Some(stop) = &self.stop {
                stop.cancel();
            }
            panic!("injected executor panic");
        }
        self.inner.execute_and_settle(dispatcher, task, handler_timeout).await
    }
}

#[tokio::test]
async fn cancellation_before_a_restart_does_not_start_a_replacement_worker() {
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
    let stop = CancellationToken::new();
    let replacement_calls = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(PanicOnce::new(queue.clone(), Some(stop.clone())));
    tokio::time::timeout(
        Duration::from_secs(5),
        run(
            queue.clone(),
            executor.clone(),
            TaskDispatcher::try_new(vec![CountingAdapter(replacement_calls.clone())]).unwrap(),
            worker_config(1, 1, Duration::from_secs(3600)),
            stop.clone(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(stop.is_cancelled());
    assert_eq!(executor.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(replacement_calls.load(Ordering::SeqCst), 0);
    assert!(queue.outcomes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn supervisor_survives_a_handler_panic_storage_errors_and_lost_acknowledgements() {
    let queue = Arc::new(Queue::default());
    queue.claim_error.store(true, Ordering::SeqCst);
    queue.recovery_error.store(true, Ordering::SeqCst);
    let executor = Arc::new(DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default()));
    let calls = Arc::new(AtomicUsize::new(0));
    struct PanicOnce {
        calls: Arc<AtomicUsize>,
    }

    impl TaskAdapter for PanicOnce {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }
        async fn invoke(&self, _: &TaskContext) -> TaskResult {
            assert_ne!(self.calls.fetch_add(1, Ordering::SeqCst), 0, "injected worker failure");
            TaskResult::Done
        }
    }
    let dispatcher = TaskDispatcher::try_new(vec![PanicOnce { calls: calls.clone() }]).unwrap();
    let config = TaskWorkerPoolConfig::try_new(
        1,
        1,
        Duration::from_millis(2),
        TaskWorkerPoolConfig::default().handler_timeout(),
        TaskWorkerPoolConfig::default().lease_duration(),
    )
    .unwrap();
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let supervisor_task = tokio::spawn(run(queue.clone(), executor, dispatcher, config, shutdown));
    wait_until(|| queue.recovery_calls.load(Ordering::SeqCst) >= 2).await;
    queue.claim_error.store(false, Ordering::SeqCst);
    queue.recovery_error.store(false, Ordering::SeqCst);
    queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
    // The panic of the first delivery is a retry; the same worker takes the second.
    wait_until(|| calls.load(Ordering::SeqCst) == 2 && queue.outcomes.lock().unwrap().len() == 2).await;
    assert!(
        matches!(&queue.outcomes.lock().unwrap()[..], [(_, SettlementRequest::Retry { reason, .. }), (_, SettlementRequest::Completed)] if reason == HANDLER_PANICKED)
    );
    // An acknowledgement failure leaves durable recovery to the queue's lease mechanism.
    queue.settle_error.store(true, Ordering::SeqCst);
    queue.incoming.lock().unwrap().push_back(claimed());
    wait_until(|| queue.outcomes.lock().unwrap().len() == 3).await;
    queue.settle_error.store(false, Ordering::SeqCst);
    queue.lost_owner.store(true, Ordering::SeqCst);
    queue.incoming.lock().unwrap().push_back(claimed());
    wait_until(|| queue.outcomes.lock().unwrap().len() == 4).await;
    stop.cancel();
    supervisor_task.await.unwrap().unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn storage_timeouts_allow_polling_and_free_worker_capacity_without_shutdown() {
    for stage in ["claim", "recovery", "settle"] {
        let queue = Arc::new(Queue::default());
        queue.hang_claim.store(stage == "claim", Ordering::SeqCst);
        queue.hang_recovery.store(stage == "recovery", Ordering::SeqCst);
        queue.hang_settle.store(stage == "settle", Ordering::SeqCst);
        queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
        let executor = Arc::new(DefaultTaskExecutor::new(
            queue.clone(),
            TaskStorageConfig::try_new(Duration::from_millis(5)).unwrap(),
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let supervisor_task = tokio::spawn(run(
            queue.clone(),
            executor,
            TaskDispatcher::try_new(vec![CountingAdapter(calls.clone())]).unwrap(),
            TaskWorkerPoolConfig::try_new(
                1,
                1,
                Duration::from_millis(2),
                TaskWorkerPoolConfig::default().handler_timeout(),
                TaskWorkerPoolConfig::default().lease_duration(),
            )
            .unwrap(),
            shutdown,
        ));
        match stage {
            "claim" => {
                wait_until(|| queue.requested.lock().unwrap().len() >= 2).await;
                assert_eq!(calls.load(Ordering::SeqCst), 0);
                queue.hang_claim.store(false, Ordering::SeqCst);
            }
            "recovery" => {
                wait_until(|| queue.recovery_calls.load(Ordering::SeqCst) >= 2).await;
                queue.hang_recovery.store(false, Ordering::SeqCst);
            }
            _ => {}
        }
        // Both tasks are executed even when the first acknowledgement never returns.
        wait_until(|| calls.load(Ordering::SeqCst) == 2 && queue.outcomes.lock().unwrap().len() == 2).await;
        assert!(!supervisor_task.is_finished());
        stop.cancel();
        supervisor_task.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn sequential_deliveries_preserve_dispatcher_state_and_tokio_task_identity() {
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
    let probe = Arc::new(IoProbe::default());
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let supervisor_task = tokio::spawn(
        prepared_runtime(
            queue.clone(),
            io_dispatcher(probe.clone()),
            worker_config(1, 1, Duration::from_millis(2)),
        )
        .run(shutdown),
    );
    wait_until(|| probe.calls.lock().unwrap().len() == 1).await;
    probe.gate.add_permits(1);
    wait_until(|| probe.calls.lock().unwrap().len() == 2).await;
    let calls = probe.calls.lock().unwrap().clone();
    assert_eq!(calls[0].0, 0, "the first prepared dispatcher must execute");
    assert_eq!(calls[0].0, calls[1].0);
    assert_eq!(calls[0].1, calls[1].1);
    assert_eq!((calls[0].2, calls[1].2), (1, 2));
    assert_eq!(probe.built.load(Ordering::SeqCst), 1);
    stop.cancel();
    tokio::time::sleep(Duration::from_millis(2)).await;
    probe.gate.add_permits(1);
    supervisor_task.await.unwrap().unwrap();
}

/// Panics on its first delivery; records the dispatcher and the Tokio task of every delivery.
struct PanicFirstRegistry {
    dispatcher: usize,
    deliveries: AtomicUsize,
    records: Arc<std::sync::Mutex<Vec<(usize, Id)>>>,
}

impl TaskAdapter for PanicFirstRegistry {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }
    async fn invoke(&self, _: &TaskContext) -> TaskResult {
        self.records.lock().unwrap().push((self.dispatcher, tokio::task::id()));
        assert_ne!(self.deliveries.fetch_add(1, Ordering::SeqCst), 0, "injected failure");
        TaskResult::Done
    }
}

/// A dispatcher over a `PanicFirstRegistry`; `built` counts the dispatchers built.
fn panic_first_dispatcher(
    built: &AtomicUsize,
    records: &Arc<std::sync::Mutex<Vec<(usize, Id)>>>,
) -> TaskDispatcher<PanicFirstRegistry> {
    TaskDispatcher::try_new(vec![PanicFirstRegistry {
        dispatcher: built.fetch_add(1, Ordering::SeqCst),
        deliveries: AtomicUsize::new(0),
        records: records.clone(),
    }])
    .unwrap()
}

/// A handler's panic is caught at the dispatch: its task is settled as a retry after the pause of its type's policy,
/// and the worker that ran it takes the next task, in the same Tokio task.
#[tokio::test]
async fn a_handler_panic_retries_its_task_after_the_policy_pause_and_the_worker_lives_on() {
    const DUMMY: PolicyName = PolicyName::new("dummy").unwrap();
    let policies = Policies::default()
        .with_policy(DUMMY, TaskPolicy::new(Retry::fixed(Duration::from_secs(7)), 3).unwrap())
        .unwrap()
        .with_task::<DummyTask>(DUMMY)
        .unwrap();
    let queue = Arc::new(Queue::default());
    let (first, second) = (claimed(), claimed());
    let ids = (first.attempt().task_id(), second.attempt().task_id());
    queue.incoming.lock().unwrap().extend([first, second]);
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let built = AtomicUsize::new(0);
    let runtime = cqrsx::task::runtime::prepare(
        queue.clone(),
        Arc::new(panic_first_dispatcher(&built, &records)),
        cqrsx::task::runtime::config::TaskRuntimeConfig::try_new(
            TaskStorageConfig::default(),
            worker_config(1, 1, Duration::from_secs(3600)),
        )
        .unwrap(),
        policies,
    );
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let supervisor_task = tokio::spawn(runtime.run(shutdown));
    wait_until(|| queue.outcomes.lock().unwrap().len() == 2).await;
    assert_eq!(
        queue.outcomes.lock().unwrap().as_slice(),
        [
            (
                ids.0,
                SettlementRequest::Retry {
                    delay: Duration::from_secs(7),
                    reason: HANDLER_PANICKED.into()
                }
            ),
            (ids.1, SettlementRequest::Completed)
        ]
    );
    let records = records.lock().unwrap().clone();
    assert_eq!(built.load(Ordering::SeqCst), 1);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0], records[1], "one dispatcher, one worker: the panic did not end it");
    assert!(!stop.is_cancelled(), "a handler panic must not cancel the host");
    stop.cancel();
    supervisor_task.await.unwrap().unwrap();
}

/// The last line of defence: a worker that dies outside every handler (here in an executor of one's own) is replaced
/// at once, without waiting for a poll tick, by a new Tokio task with the shared dispatcher.
#[tokio::test]
async fn a_worker_that_dies_restarts_without_poll_tick_with_the_shared_dispatcher_and_a_new_task() {
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let built = AtomicUsize::new(0);
    let executor = Arc::new(PanicOnce::new(queue.clone(), None));
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let supervisor_task = tokio::spawn(run(
        queue.clone(),
        executor.clone(),
        panic_first_dispatcher(&built, &records),
        worker_config(1, 1, Duration::from_secs(3600)),
        shutdown,
    ));
    wait_until(|| queue.outcomes.lock().unwrap().len() == 1).await;
    let workers = executor.workers.lock().unwrap().clone();
    assert_eq!(workers.len(), 2);
    assert_ne!(workers[0], workers[1], "the replacement is a new Tokio task");
    let records = records.lock().unwrap().clone();
    assert_eq!(built.load(Ordering::SeqCst), 1);
    assert_eq!(
        records,
        [(0, workers[1])],
        "the replacement delivers with the shared dispatcher"
    );
    assert_eq!(queue.recovery_calls.load(Ordering::SeqCst), 1);
    assert!(!stop.is_cancelled(), "a worker panic must not cancel the host");
    stop.cancel();
    supervisor_task.await.unwrap().unwrap();
}
