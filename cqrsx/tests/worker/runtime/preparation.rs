use super::*;
use cqrsx::task::{Task, policy::Policies};
use cqrsx::worker::{TaskHandler, dependencies::DependencyId, handlers::TaskHandlers};

#[tokio::test]
async fn invalid_full_lease_budget_is_rejected_before_polling() {
    let queue = Arc::new(Queue::default());
    let executor = Arc::new(DefaultTaskExecutor::new(
        queue.clone(),
        TaskStorageConfig::try_new(Duration::from_secs(10)).unwrap(),
    ));
    let dispatcher = TaskDispatcher::try_new(vec![adapter()]).unwrap();
    let shutdown = CancellationToken::new();
    assert!(
        run(
            queue.clone(),
            executor,
            dispatcher,
            TaskWorkerPoolConfig::builder()
                .concurrency(TaskWorkerPoolConfig::default().concurrency())
                .buffer_capacity(TaskWorkerPoolConfig::default().concurrency())
                .poll_interval(TaskWorkerPoolConfig::default().poll_interval())
                .handler_timeout(TaskWorkerPoolConfig::default().handler_timeout())
                .lease(Duration::from_secs(50))
                .build()
                .unwrap(),
            shutdown
        )
        .await
        .is_err()
    );
    assert!(queue.requested.lock().unwrap().is_empty());
    assert_eq!(queue.recovery_calls.load(Ordering::SeqCst), 0);
}

/// The dummy task, decoded.
#[derive(serde::Serialize, serde::Deserialize)]
struct Greeting {
    value: String,
}

impl Task for Greeting {
    const TYPE: &'static str = "dummy_task";
    const VERSION: i32 = 1;
}

/// Greets with the salutation the assembly provides.
struct Greeter(Arc<std::sync::Mutex<Vec<String>>>);

impl TaskHandler<Greeting> for Greeter {
    const DEPENDENCIES: &'static [DependencyId] = &[DependencyId::of::<String>()];

    async fn handle(&self, task: Greeting, ctx: &TaskContext) -> TaskResult {
        match ctx.dependency::<String>() {
            Ok(salutation) => {
                self.0.lock().unwrap().push(format!("{salutation}, {}", task.value));
                TaskResult::Done
            }
            Err(missing) => TaskResult::fail(missing.to_string()),
        }
    }
}

#[tokio::test]
async fn a_handler_without_its_dependency_is_refused_before_the_runtime_polls() {
    let greeted = Arc::new(std::sync::Mutex::new(Vec::new()));
    assert_eq!(
        TaskHandlers::new().handler(Greeter(greeted.clone())).build().err(),
        Some(TaskDispatcherError::MissingDependency {
            format: format(),
            dependency: DependencyId::of::<String>().name(),
        })
    );
    // Provided, the same handler runs through the runtime.
    let dispatcher = TaskHandlers::new()
        .handler(Greeter(greeted.clone()))
        .provide(Arc::new(String::from("hello")))
        .build()
        .unwrap();
    let queue = Arc::new(Queue::default());
    let task = claimed();
    let id = task.attempt().task_id();
    queue.incoming.lock().unwrap().push_back(task);
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(run(
        queue.clone(),
        Arc::new(DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default())),
        dispatcher,
        worker_config(2, 2, Duration::from_millis(5)),
        shutdown.clone(),
    ));
    wait_until(|| !queue.outcomes.lock().unwrap().is_empty()).await;
    shutdown.cancel();
    running.await.unwrap().unwrap();
    assert_eq!(
        *queue.outcomes.lock().unwrap(),
        vec![(id, cqrsx::worker::queue::settlement::SettlementRequest::Completed)]
    );
    assert_eq!(*greeted.lock().unwrap(), ["hello, runtime"]);
}

/// A handler no test may run.
struct Untouchable;

impl TaskAdapter for Untouchable {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn invoke(&self, _: &TaskContext) -> TaskResult {
        panic!("cancelled startup must not run a handler")
    }
}

#[tokio::test]
async fn initially_cancelled_shutdown_does_not_build_workers_or_claim() {
    let queue = Arc::new(Queue::default());
    queue.incoming.lock().unwrap().push_back(claimed());
    let shutdown = CancellationToken::new();
    shutdown.cancel();
    run(
        queue.clone(),
        Arc::new(DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default())),
        TaskDispatcher::try_new(vec![Untouchable]).unwrap(),
        worker_config(2, 1, Duration::from_secs(1)),
        shutdown,
    )
    .await
    .unwrap();
    assert!(queue.requested.lock().unwrap().is_empty());
    assert_eq!(queue.recovery_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn standard_assembly_polls_and_settles_through_the_original_queue_instance() {
    use cqrsx::worker::runtime::{config::TaskRuntimeConfig, prepare};
    let queue = Arc::new(Queue::default());
    let task = claimed();
    let id = task.attempt().task_id();
    queue.incoming.lock().unwrap().push_back(task);
    let dispatcher = TaskDispatcher::try_new(vec![adapter()]).unwrap();
    let config = TaskRuntimeConfig::try_new(TaskStorageConfig::default(), worker_config(1, 1, Duration::from_millis(5))).unwrap();
    let prepared = prepare(queue.clone(), Arc::new(dispatcher), config, Policies::default());
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
        vec![(id, cqrsx::worker::queue::settlement::SettlementRequest::Completed)]
    );
}

#[tokio::test]
async fn a_boxed_adapter_dispatcher_runs_through_the_standard_runtime() {
    use cqrsx::worker::{
        adapter::boxed::BoxedTaskAdapter,
        runtime::{config::TaskRuntimeConfig, prepare},
    };
    let queue = Arc::new(Queue::default());
    let task = claimed();
    let id = task.attempt().task_id();
    queue.incoming.lock().unwrap().push_back(task);
    // Two workers share the one dispatcher.
    let dispatcher = TaskDispatcher::try_new(vec![BoxedTaskAdapter::new(adapter())]).unwrap();
    let config = TaskRuntimeConfig::try_new(TaskStorageConfig::default(), worker_config(2, 1, Duration::from_millis(5))).unwrap();
    let prepared = prepare(queue.clone(), Arc::new(dispatcher), config, Policies::default());
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
        vec![(id, cqrsx::worker::queue::settlement::SettlementRequest::Completed)]
    );
}
