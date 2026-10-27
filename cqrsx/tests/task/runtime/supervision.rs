use super::*;

#[tokio::test]
async fn cancellation_before_a_restart_does_not_start_a_replacement_worker() {
    /// Asks for shutdown and panics on its first delivery; counts the deliveries after it.
    struct CancelThenPanic {
        stop: CancellationToken,
        deliveries: AtomicUsize,
        after_panic: Arc<AtomicUsize>,
    }

    impl TaskAdapter for CancelThenPanic {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }
        async fn invoke(&self, _: &TaskContext) -> TaskResult {
            if self.deliveries.fetch_add(1, Ordering::SeqCst) == 0 {
                self.stop.cancel();
                panic!("injected worker panic");
            }
            self.after_panic.fetch_add(1, Ordering::SeqCst);
            TaskResult::Done
        }
    }
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
    let stop = CancellationToken::new();
    let replacement_calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = TaskDispatcher::try_new(vec![CancelThenPanic {
        stop: stop.clone(),
        deliveries: AtomicUsize::new(0),
        after_panic: replacement_calls.clone(),
    }])
    .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        run(
            queue.clone(),
            Arc::new(DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default())),
            dispatcher,
            worker_config(1, 1, Duration::from_secs(3600)),
            stop.clone(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(stop.is_cancelled());
    assert_eq!(replacement_calls.load(Ordering::SeqCst), 0);
    assert!(queue.outcomes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn supervisor_recovers_workers_and_survives_storage_errors_and_lost_acknowledgements() {
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
    let config = TaskWorkerPoolConfig::new(
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
    wait_until(|| calls.load(Ordering::SeqCst) >= 2 && !queue.outcomes.lock().unwrap().is_empty()).await;
    // An acknowledgement failure leaves durable recovery to the queue's lease mechanism.
    queue.settle_error.store(true, Ordering::SeqCst);
    queue.incoming.lock().unwrap().push_back(claimed());
    wait_until(|| queue.outcomes.lock().unwrap().len() >= 2).await;
    queue.settle_error.store(false, Ordering::SeqCst);
    queue.lost_owner.store(true, Ordering::SeqCst);
    queue.incoming.lock().unwrap().push_back(claimed());
    wait_until(|| queue.outcomes.lock().unwrap().len() >= 3).await;
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
            TaskStorageConfig::new(Duration::from_millis(5)).unwrap(),
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let supervisor_task = tokio::spawn(run(
            queue.clone(),
            executor,
            TaskDispatcher::try_new(vec![CountingAdapter(calls.clone())]).unwrap(),
            TaskWorkerPoolConfig::new(
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

#[tokio::test]
async fn panic_restarts_without_poll_tick_with_the_shared_dispatcher_and_a_new_task() {
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
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().extend([claimed(), claimed()]);
    let records = Arc::new(std::sync::Mutex::new(Vec::new()));
    let built = Arc::new(AtomicUsize::new(0));
    let dispatcher = TaskDispatcher::try_new(vec![PanicFirstRegistry {
        dispatcher: built.fetch_add(1, Ordering::SeqCst),
        deliveries: AtomicUsize::new(0),
        records: records.clone(),
    }])
    .unwrap();
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let supervisor_task =
        tokio::spawn(prepared_runtime(queue.clone(), dispatcher, worker_config(1, 1, Duration::from_secs(3600))).run(shutdown));
    wait_until(|| queue.outcomes.lock().unwrap().len() == 1).await;
    let records = records.lock().unwrap().clone();
    assert_eq!(built.load(Ordering::SeqCst), 1);
    assert_eq!((records[0].0, records[1].0), (0, 0));
    assert_ne!(records[0].1, records[1].1);
    assert_eq!(queue.recovery_calls.load(Ordering::SeqCst), 1);
    assert!(!stop.is_cancelled(), "a worker panic must not cancel the host");
    stop.cancel();
    supervisor_task.await.unwrap().unwrap();
}
