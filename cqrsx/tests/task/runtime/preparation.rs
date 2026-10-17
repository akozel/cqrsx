use super::*;

#[tokio::test]
async fn invalid_full_lease_budget_is_rejected_before_polling() {
    let queue = Arc::new(Queue::default());
    let executor = Arc::new(DefaultTaskExecutor::new(
        queue.clone(),
        TaskStorageConfig::new(Duration::from_secs(10)).unwrap(),
    ));
    let factory: TaskDispatcherFactory<_> = Arc::new(|| TaskDispatcher::try_new(vec![adapter()]));
    let shutdown = CancellationToken::new();
    assert!(
        run(
            queue.clone(),
            executor,
            factory,
            TaskWorkerPoolConfig::new(
                TaskWorkerPoolConfig::default().concurrency(),
                TaskWorkerPoolConfig::default().concurrency(),
                TaskWorkerPoolConfig::default().poll_interval(),
                TaskWorkerPoolConfig::default().handler_timeout(),
                Duration::from_secs(50)
            )
            .unwrap(),
            shutdown
        )
        .await
        .is_err()
    );
    assert!(queue.requested.lock().unwrap().is_empty());
    assert_eq!(queue.recovery_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn validates_every_workers_dispatcher_before_polling() {
    struct OtherAdapter;

    impl TaskAdapter for OtherAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![TaskFormat::new("other", 1).unwrap()]
        }

        async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
            Ok(())
        }
    }

    enum RegistryAdapter {
        Normal(Adapter),
        Other(OtherAdapter),
    }
    impl TaskAdapter for RegistryAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            match self {
                Self::Normal(adapter) => adapter.formats(),
                Self::Other(adapter) => adapter.formats(),
            }
        }
        async fn invoke(&mut self, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
            match self {
                Self::Normal(adapter) => adapter.invoke(attempt).await,
                Self::Other(adapter) => adapter.invoke(attempt).await,
            }
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let factory: TaskDispatcherFactory<_> = Arc::new({
        let calls = calls.clone();
        move || {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                TaskDispatcher::try_new(vec![RegistryAdapter::Normal(adapter())])
            } else {
                TaskDispatcher::try_new(vec![RegistryAdapter::Other(OtherAdapter)])
            }
        }
    });
    let queue = Arc::new(Queue::default());
    let executor = Arc::new(DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default()));
    let shutdown = CancellationToken::new();
    let result = run(
        queue.clone(),
        executor,
        factory,
        TaskWorkerPoolConfig::new(
            2,
            2,
            TaskWorkerPoolConfig::default().poll_interval(),
            TaskWorkerPoolConfig::default().handler_timeout(),
            TaskWorkerPoolConfig::default().lease_duration(),
        )
        .unwrap(),
        shutdown,
    )
    .await;
    assert_eq!(
        result,
        Err(TaskRuntimeError::WorkerSetup(WorkerSetupError::FormatsMismatch {
            worker_index: 1
        }))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn initially_cancelled_shutdown_does_not_build_workers_or_claim() {
    let queue = Arc::new(Queue::default());
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    run::<_, _, Adapter>(
        queue.clone(),
        Arc::new(DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default())),
        Arc::new(|| panic!("cancelled startup must not build a dispatcher")),
        worker_config(2, 1, Duration::from_secs(1)),
        shutdown,
    )
    .await
    .unwrap();
    assert!(queue.requested.lock().unwrap().is_empty());
    assert_eq!(queue.recovery_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn startup_factory_panic_cancels_the_host_without_polling_storage() {
    let queue = Arc::new(Queue::default());
    let shutdown = CancellationToken::new();
    let supervisor = tokio::spawn(run::<_, _, Adapter>(
        queue.clone(),
        Arc::new(DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default())),
        Arc::new(|| panic!("injected startup factory panic")),
        worker_config(1, 1, Duration::from_secs(1)),
        shutdown.clone(),
    ));
    assert!(supervisor.await.unwrap_err().is_panic());
    assert!(shutdown.is_cancelled());
    assert!(queue.requested.lock().unwrap().is_empty());
}

#[tokio::test]
async fn standard_assembly_polls_and_settles_through_the_original_queue_instance() {
    use cqrsx::task::runtime::{config::TaskRuntimeConfig, prepare};
    let queue = Arc::new(Queue::default());
    let task = claimed();
    let id = task.attempt().task_id();
    queue.incoming.lock().unwrap().push_back(task);
    let factory: TaskDispatcherFactory<_> = Arc::new(|| TaskDispatcher::try_new(vec![adapter()]));
    let config = TaskRuntimeConfig::new(TaskStorageConfig::default(), worker_config(1, 1, Duration::from_millis(5))).unwrap();
    let prepared = prepare(queue.clone(), factory, config).unwrap();
    assert!(queue.requested.lock().unwrap().is_empty());
    assert!(queue.outcomes.lock().unwrap().is_empty());
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(prepared.run(shutdown.clone()));
    let settled = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !queue.outcomes.lock().unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    settled.unwrap();
    assert!(queue.requested.lock().unwrap().contains(&format()));
    assert_eq!(
        *queue.outcomes.lock().unwrap(),
        vec![(id, cqrsx::task::queue::settlement::SettlementRequest::Completed)]
    );
}

#[tokio::test]
async fn a_boxed_adapter_dispatcher_runs_through_the_standard_runtime() {
    use cqrsx::task::{
        adapter::boxed::BoxedTaskAdapter,
        runtime::{config::TaskRuntimeConfig, prepare},
    };
    let queue = Arc::new(Queue::default());
    let task = claimed();
    let id = task.attempt().task_id();
    queue.incoming.lock().unwrap().push_back(task);
    let builds = Arc::new(AtomicUsize::new(0));
    let factory: TaskDispatcherFactory<BoxedTaskAdapter> = Arc::new({
        let builds = builds.clone();
        move || {
            builds.fetch_add(1, Ordering::SeqCst);
            TaskDispatcher::try_new(vec![BoxedTaskAdapter::new(adapter())])
        }
    });
    let config = TaskRuntimeConfig::new(TaskStorageConfig::default(), worker_config(2, 1, Duration::from_millis(5))).unwrap();
    let prepared = prepare(queue.clone(), factory, config).unwrap();
    assert_eq!(
        builds.load(Ordering::SeqCst),
        2,
        "preparation builds one dispatcher per worker"
    );
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(prepared.run(shutdown.clone()));
    let settled = tokio::time::timeout(Duration::from_secs(2), async {
        while queue.outcomes.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(2), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    settled.unwrap();
    assert_eq!(
        *queue.outcomes.lock().unwrap(),
        vec![(id, cqrsx::task::queue::settlement::SettlementRequest::Completed)]
    );
}
