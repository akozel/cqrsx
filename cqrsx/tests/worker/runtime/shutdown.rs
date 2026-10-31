use super::*;

#[tokio::test(start_paused = true)]
async fn generic_custom_executor_uses_actual_budget_and_stalled_invocation_drains_for_h_plus_s() {
    struct Executor {
        budget: Duration,
        active: AtomicUsize,
    }
    impl cqrsx::worker::executor::TaskExecutor for Executor {
        fn storage_timeout(&self) -> Duration {
            self.budget
        }
        async fn execute_and_settle<A: TaskAdapter>(
            &self,
            _: &TaskDispatcher<A>,
            _: LeasedTask,
            _: Duration,
        ) -> Result<SettlementResult, TaskProcessingError> {
            struct Active<'a>(&'a AtomicUsize);
            impl Drop for Active<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            self.active.fetch_add(1, Ordering::SeqCst);
            let _active = Active(&self.active);
            std::future::pending().await
        }
    }
    let queue = Arc::new(Queue::default());
    let config = TaskWorkerPoolConfig::try_new(
        1,
        1,
        Duration::from_secs(1),
        Duration::from_millis(20),
        Duration::from_secs(60),
    )
    .unwrap();
    let stop = CancellationToken::new();
    let invalid = Arc::new(Executor {
        budget: Duration::from_secs(60),
        active: AtomicUsize::new(0),
    });
    /// A handler budget validation keeps from running.
    struct Unreachable;
    impl TaskAdapter for Unreachable {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }
        async fn invoke(&self, _: &TaskContext) -> TaskResult {
            panic!("budget validation precedes any delivery")
        }
    }
    queue.incoming.lock().unwrap().push_back(claimed());
    let result = run(
        queue.clone(),
        invalid.clone(),
        TaskDispatcher::try_new(vec![Unreachable]).unwrap(),
        config.clone(),
        stop.clone(),
    )
    .await;
    assert!(matches!(result, Err(TaskRuntimeError::Configuration(_))));
    assert!(stop.is_cancelled());
    assert_eq!(invalid.active.load(Ordering::SeqCst), 0);
    assert!(queue.requested.lock().unwrap().is_empty());
    assert_eq!(queue.recovery_calls.load(Ordering::SeqCst), 0);

    let executor = Arc::new(Executor {
        budget: Duration::from_millis(10),
        active: AtomicUsize::new(0),
    });
    let stop = CancellationToken::new();
    let supervisor_task = tokio::spawn(run(
        queue,
        executor.clone(),
        TaskDispatcher::try_new(vec![CountingAdapter(Arc::new(AtomicUsize::new(0)))]).unwrap(),
        config,
        stop.clone(),
    ));
    wait_until(|| executor.active.load(Ordering::SeqCst) == 1).await;
    let started = tokio::time::Instant::now();
    stop.cancel();
    supervisor_task.await.unwrap().unwrap();
    assert_eq!(started.elapsed(), Duration::from_millis(30));
    assert_eq!(executor.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn shutdown_drains_in_flight_work_and_invalid_worker_wiring_is_rejected() {
    let queue = Arc::new(Queue::default());
    let executor = Arc::new(DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default()));
    let seen = Arc::new(AtomicUsize::new(0));
    let dispatcher = || TaskDispatcher::try_new(vec![CountingAdapter(seen.clone())]).unwrap();
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let invalid =
        TaskWorkerPoolConfig::try_new(1, 1, Duration::from_secs(1), Duration::from_secs(30), Duration::from_secs(45)).unwrap();
    assert!(
        run(queue.clone(), executor.clone(), dispatcher(), invalid, shutdown.child_token())
            .await
            .is_err()
    );
    assert!(TaskDispatcher::<Adapter>::try_new(vec![]).is_err());
    queue.incoming.lock().unwrap().push_back(claimed());
    let supervisor_task = tokio::spawn(run(
        queue.clone(),
        executor,
        dispatcher(),
        TaskWorkerPoolConfig::default(),
        shutdown,
    ));
    wait_until(|| seen.load(Ordering::SeqCst) > 0).await;
    stop.cancel();
    supervisor_task.await.unwrap().unwrap();
    assert_eq!(queue.outcomes.lock().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn shutdown_cancels_stalled_polling_and_bounds_stalled_acknowledgements() {
    for stage in ["claim", "recovery", "settle"] {
        let queue = Arc::new(Queue::default());
        queue.hang_claim.store(stage == "claim", Ordering::SeqCst);
        queue.hang_recovery.store(stage == "recovery", Ordering::SeqCst);
        queue.hang_settle.store(stage == "settle", Ordering::SeqCst);
        queue.incoming.lock().unwrap().push_back(claimed());
        let executor = Arc::new(DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default()));
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let supervisor_task = tokio::spawn(run(
            queue.clone(),
            executor,
            TaskDispatcher::try_new(vec![adapter()]).unwrap(),
            TaskWorkerPoolConfig::default(),
            shutdown,
        ));
        wait_until(|| match stage {
            "claim" => !queue.requested.lock().unwrap().is_empty(),
            "recovery" => queue.recovery_calls.load(Ordering::SeqCst) > 0,
            "settle" => !queue.outcomes.lock().unwrap().is_empty(),
            _ => unreachable!(),
        })
        .await;
        let start = tokio::time::Instant::now();
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(22), supervisor_task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(start.elapsed() <= Duration::from_secs(21));
    }
}

#[tokio::test(start_paused = true)]
async fn shutdown_drain_uses_the_configured_settlement_budget() {
    let queue = Arc::new(Queue::default());
    queue.hang_settle.store(true, Ordering::SeqCst);
    queue.incoming.lock().unwrap().push_back(claimed());
    let executor = Arc::new(DefaultTaskExecutor::new(
        queue.clone(),
        TaskStorageConfig::try_new(Duration::from_secs(10)).unwrap(),
    ));
    let mut task_adapter = adapter();
    task_adapter.hang = true;
    let dispatcher = TaskDispatcher::try_new(vec![task_adapter]).unwrap();
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let started = tokio::time::Instant::now();
    let supervisor_task = tokio::spawn(run(
        queue.clone(),
        executor,
        dispatcher,
        worker_config(128, 128, Duration::from_secs(1)),
        shutdown,
    ));
    wait_until(|| queue.incoming.lock().unwrap().is_empty()).await;
    stop.cancel();
    supervisor_task.await.unwrap().unwrap();
    assert!(started.elapsed() >= Duration::from_secs(40));
    assert!(matches!(queue.outcomes.lock().unwrap()[0].1, SettlementRequest::Retry { .. }));
}

#[tokio::test]
async fn cancellation_of_supervisor_aborts_owned_workers_without_detaching_them() {
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
    let probe = Arc::new(IoProbe::default());
    let shutdown = CancellationToken::new();
    let supervisor_task = tokio::spawn(
        prepared_runtime(
            queue.clone(),
            io_dispatcher(probe.clone()),
            worker_config(1, 1, Duration::from_secs(3600)),
        )
        .run(shutdown.clone()),
    );
    wait_until(|| probe.active.load(Ordering::SeqCst) == 1 && queue.incoming.lock().unwrap().is_empty()).await;
    supervisor_task.abort();
    assert!(supervisor_task.await.unwrap_err().is_cancelled());
    assert!(shutdown.is_cancelled(), "supervisor abort must notify the host");
    wait_until(|| probe.active.load(Ordering::SeqCst) == 0).await;
    assert!(queue.outcomes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn shared_cancellation_stops_dequeue_while_supervisor_is_claiming() {
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend((0..5).map(|_| claimed()));
    let probe = Arc::new(IoProbe::default());
    let shutdown = CancellationToken::new();
    let supervisor_task = tokio::spawn(run(
        queue.clone(),
        Arc::new(DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default())),
        io_dispatcher(probe.clone()),
        // A claim for every freed slot, so that a claim is in flight after the second dequeue: this test is
        // about cancellation during a claim, not the refill level.
        worker_config(1, 2, Duration::from_secs(3600))
            .with_buffer_refill_percent(100)
            .unwrap(),
        shutdown.clone(),
    ));
    wait_until(|| probe.active.load(Ordering::SeqCst) == 1 && queue.incoming.lock().unwrap().len() == 2).await;
    queue.hang_claim.store(true, Ordering::SeqCst);
    probe.gate.add_permits(1);
    wait_until(|| probe.calls.lock().unwrap().len() == 2 && queue.requested.lock().unwrap().len() == 3).await;
    shutdown.cancel();
    // Finish the active operation immediately, even before supervisor resumes.
    probe.gate.add_permits(1);
    supervisor_task.await.unwrap().unwrap();
    assert_eq!(probe.calls.lock().unwrap().len(), 2);
    assert_eq!(queue.outcomes.lock().unwrap().len(), 2);
    assert_eq!(queue.incoming.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn cancellation_in_a_ready_claim_discards_the_batch_without_execution() {
    struct CancelAfterClaim {
        inner: Arc<Queue>,
        shutdown: CancellationToken,
    }
    impl TaskQueue for CancelAfterClaim {
        async fn claim(
            &self,
            format: &TaskFormat,
            owner: &str,
            limit: usize,
            lease: Duration,
        ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
            let tasks = self.inner.claim(format, owner, limit, lease).await?;
            self.shutdown.cancel();
            Ok(tasks)
        }
        async fn settle(&self, task: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
            self.inner.settle(task, request).await
        }
        async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
            self.inner.recover_expired(limit).await
        }
    }
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().push_back(claimed());
    let calls = Arc::new(AtomicUsize::new(0));
    let shutdown = CancellationToken::new();
    let dispatcher = TaskDispatcher::try_new(vec![CountingAdapter(calls.clone())]).unwrap();
    let cancelling_queue = Arc::new(CancelAfterClaim {
        inner: queue.clone(),
        shutdown: shutdown.clone(),
    });
    tokio::time::timeout(
        Duration::from_secs(1),
        run(
            cancelling_queue.clone(),
            Arc::new(DefaultTaskExecutor::new(cancelling_queue, TaskStorageConfig::default())),
            dispatcher,
            worker_config(1, 1, Duration::from_secs(3600)),
            shutdown,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(queue.incoming.lock().unwrap().is_empty(), "claim did complete");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(queue.outcomes.lock().unwrap().is_empty());
}
