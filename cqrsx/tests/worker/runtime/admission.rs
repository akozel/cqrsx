use super::*;

#[tokio::test]
async fn bounded_admission_is_independent_of_concurrency_and_shutdown_leaves_queue_for_recovery() {
    for (c, q) in [(2, 3), (2, 1), (3, 1)] {
        let queue = Arc::new(Queue::default());
        queue.incoming.lock().unwrap().extend((0..20).map(|_| claimed()));
        let probe = Arc::new(IoProbe::default());
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let supervisor_task = tokio::spawn(
            prepared_runtime(
                queue.clone(),
                io_dispatcher(probe.clone()),
                // A claim for every freed slot, so that the buffer fills: this test is about the bound and
                // shutdown, not the refill level.
                eager_worker_config(c, q, Duration::from_millis(2)),
            )
            .run(shutdown),
        );
        wait_until(|| probe.active.load(Ordering::SeqCst) == c && queue.incoming.lock().unwrap().len() == 20 - c - q).await;
        assert_eq!(probe.built.load(Ordering::SeqCst), 1, "the workers share one dispatcher");
        assert_eq!(probe.peak.load(Ordering::SeqCst), c);
        assert!(queue.claim_limits.lock().unwrap().iter().all(|&limit| limit <= q));
        let calls = queue.claim_limits.lock().unwrap().len();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            queue.claim_limits.lock().unwrap().len(),
            calls,
            "full queue cannot claim more work"
        );
        stop.cancel();
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(!supervisor_task.is_finished(), "started operations must drain");
        probe.gate.add_permits(c);
        supervisor_task.await.unwrap().unwrap();
        assert_eq!(
            probe.calls.lock().unwrap().len(),
            c,
            "queued operations do not start on shutdown"
        );
        assert_eq!(queue.outcomes.lock().unwrap().len(), c);
        assert_eq!(queue.incoming.lock().unwrap().len(), 20 - c - q);
        assert_eq!(probe.active.load(Ordering::SeqCst), 0);
    }
}

/// One worker over a buffer of four: the first claim fills the buffer, then each finished delivery lets
/// the worker take the next one, and `claims` is the claim limits seen after each of `steps` dequeues.
async fn claim_limits_while_one_worker_drains_a_buffer_of_four(percent: u8, steps: &[Vec<usize>]) {
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend((0..20).map(|_| claimed()));
    let probe = Arc::new(IoProbe::default());
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    // Four queued waves need a lease above 2 * 6 + 30 + 5 + 4 * 36 = 191 s.
    let config = TaskWorkerPoolConfig::builder()
        .concurrency(1)
        .buffer_capacity(4)
        .poll_interval(Duration::from_millis(2))
        .handler_timeout(Duration::from_secs(30))
        .lease(Duration::from_secs(200))
        .buffer_refill_percent(percent)
        .build()
        .unwrap();
    let supervisor_task = tokio::spawn(prepared_runtime(queue.clone(), io_dispatcher(probe.clone()), config).run(shutdown));
    for (step, expected) in steps.iter().enumerate() {
        if step > 0 {
            probe.gate.add_permits(1);
        }
        let claimed: usize = expected.iter().sum();
        // The delivery of this step runs, and the buffer holds what was claimed and not yet taken.
        wait_until(|| probe.calls.lock().unwrap().len() == step + 1 && queue.incoming.lock().unwrap().len() == 20 - claimed)
            .await;
        // Poll ticks keep coming (recovery runs every 2 ms), and none of them claims beyond the expectation.
        let ticks = queue.recovery_calls.load(Ordering::SeqCst);
        wait_until(|| queue.recovery_calls.load(Ordering::SeqCst) >= ticks + 3).await;
        assert_eq!(
            *queue.claim_limits.lock().unwrap(),
            *expected,
            "percent {percent}, step {step}"
        );
    }
    stop.cancel();
    probe.gate.add_permits(1);
    supervisor_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_claim_waits_until_fewer_than_half_the_buffer_waits_and_then_fills_it() {
    // Waiting after each dequeue: 3, 2, 1 → claim 3 (back to 4), then 3, 2, 1 → claim 3.
    let steps = [vec![4], vec![4], vec![4, 3], vec![4, 3], vec![4, 3], vec![4, 3, 3]];
    claim_limits_while_one_worker_drains_a_buffer_of_four(50, &steps).await;
}

#[tokio::test]
async fn a_refill_percent_of_100_claims_for_every_freed_slot() {
    let steps = [vec![4, 1], vec![4, 1, 1], vec![4, 1, 1, 1]];
    claim_limits_while_one_worker_drains_a_buffer_of_four(100, &steps).await;
}

#[tokio::test]
async fn consumers_free_capacity_during_claim_without_parallel_claims_or_overflow() {
    struct PausedClaimQueue {
        inner: Arc<Queue>,
        calls: AtomicUsize,
        gate: tokio::sync::Semaphore,
    }
    impl TaskQueue for PausedClaimQueue {
        async fn claim(
            &self,
            format: &TaskFormat,
            owner: &str,
            limit: usize,
            lease: Duration,
        ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 2 {
                self.gate.acquire().await.unwrap().forget();
            }
            self.inner.claim(format, owner, limit, lease).await
        }
        async fn settle(&self, task: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
            self.inner.settle(task, request).await
        }
        async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
            self.inner.recover_expired(limit).await
        }
    }
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend((0..7).map(|_| claimed()));
    let paused_queue = Arc::new(PausedClaimQueue {
        inner: queue.clone(),
        calls: AtomicUsize::new(0),
        gate: tokio::sync::Semaphore::new(0),
    });
    let probe = Arc::new(IoProbe::default());
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let supervisor_task = tokio::spawn(run(
        paused_queue.clone(),
        Arc::new(DefaultTaskExecutor::new(paused_queue.clone(), TaskStorageConfig::default())),
        io_dispatcher(probe.clone()),
        // A claim for every freed slot: this test is about a dequeue racing a claim, not the refill level.
        eager_worker_config(1, 2, Duration::from_secs(3600)),
        shutdown,
    ));
    // One active delivery and two waiting; the next claim starts on dequeue.
    wait_until(|| probe.active.load(Ordering::SeqCst) == 1 && queue.incoming.lock().unwrap().len() == 4).await;
    probe.gate.add_permits(1);
    wait_until(|| paused_queue.calls.load(Ordering::SeqCst) == 3).await;
    // Finish the second delivery and dequeue the third while claim is pending.
    probe.gate.add_permits(1);
    wait_until(|| probe.calls.lock().unwrap().len() == 3).await;
    assert_eq!(queue.outcomes.lock().unwrap().len(), 2);
    assert_eq!(queue.incoming.lock().unwrap().len(), 4);
    assert_eq!(
        paused_queue.calls.load(Ordering::SeqCst),
        3,
        "dequeue notifications cannot start a parallel claim"
    );
    paused_queue.gate.add_permits(1);
    // The captured limit still fits; a subsequent claim refills the remaining slot.
    wait_until(|| queue.incoming.lock().unwrap().len() == 2).await;
    assert_eq!(*queue.claim_limits.lock().unwrap(), vec![2, 1, 1, 1]);
    assert_eq!(probe.peak.load(Ordering::SeqCst), 1);
    assert_eq!(probe.calls.lock().unwrap().len(), 3);
    stop.cancel();
    // Observe internal stop before releasing the active invocation.
    tokio::task::yield_now().await;
    probe.gate.add_permits(1);
    supervisor_task.await.unwrap().unwrap();
    assert_eq!(queue.outcomes.lock().unwrap().len(), 3);
    assert_eq!(
        probe.calls.lock().unwrap().len(),
        3,
        "the two buffered deliveries stay for recovery"
    );
}

#[tokio::test]
async fn thousand_tasks_are_each_executed_and_settled_successfully() {
    struct RecordingAdapter(Arc<std::sync::Mutex<Vec<Uuid>>>);

    impl TaskAdapter for RecordingAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }
        async fn invoke(&self, ctx: &TaskContext) -> TaskResult {
            self.0.lock().unwrap().push(ctx.task_id());
            tokio::task::yield_now().await;
            TaskResult::Done
        }
    }
    let tasks: Vec<_> = (0..1000).map(|_| claimed()).collect();
    let mut expected: Vec<_> = tasks.iter().map(|task| task.attempt().task_id()).collect();
    expected.sort_unstable();
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend(tasks);
    let executed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let dispatcher = TaskDispatcher::try_new(vec![RecordingAdapter(executed.clone())]).unwrap();
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let supervisor_task = tokio::spawn(
        prepared_runtime(
            queue.clone(),
            dispatcher,
            // More deliveries than C+Q exercises batch admission and Notify refill.
            worker_config(16, 32, Duration::from_secs(3600)),
        )
        .run(shutdown),
    );
    wait_until(|| queue.outcomes.lock().unwrap().len() >= expected.len()).await;
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), supervisor_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let mut actual = executed.lock().unwrap().clone();
    actual.sort_unstable();
    assert_eq!(
        actual, expected,
        "each delivery must execute exactly once in this successful run"
    );
    let outcomes = queue.outcomes.lock().unwrap();
    assert!(outcomes.iter().all(|(_, outcome)| *outcome == SettlementRequest::Completed));
    let mut settled: Vec<_> = outcomes.iter().map(|(id, _)| *id).collect();
    settled.sort_unstable();
    assert_eq!(settled, expected, "each delivered ID must have one successful settlement");
    assert!(queue.incoming.lock().unwrap().is_empty());
    assert!(queue.claim_limits.lock().unwrap().iter().all(|&limit| limit <= 32));
}

#[tokio::test(flavor = "current_thread")]
async fn thousand_io_workers_wait_concurrently_on_one_runtime_thread() {
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend((0..2100).map(|_| claimed()));
    let probe = Arc::new(IoProbe::default());
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let supervisor_task = tokio::spawn(
        prepared_runtime(
            queue.clone(),
            io_dispatcher(probe.clone()),
            worker_config(1000, 1000, Duration::from_millis(2)),
        )
        .run(shutdown),
    );
    wait_until(|| probe.active.load(Ordering::SeqCst) == 1000 && queue.incoming.lock().unwrap().len() == 100).await;
    assert_eq!(probe.peak.load(Ordering::SeqCst), 1000);
    let task_ids: std::collections::HashSet<_> = probe.calls.lock().unwrap().iter().map(|call| call.1).collect();
    assert_eq!(task_ids.len(), 1000);
    assert!(queue.recovery_limits.lock().unwrap().iter().all(|&limit| limit == 32));
    stop.cancel();
    tokio::time::sleep(Duration::from_millis(2)).await;
    probe.gate.add_permits(1000);
    supervisor_task.await.unwrap().unwrap();
    assert_eq!(queue.outcomes.lock().unwrap().len(), 1000);
}
