//! Prepared worker runtime: one dispatcher shared by every worker, polling of durable work and supervision.
//! Its settings are [`config`] and [`worker_pool_config`].
pub mod config;
mod poller;
pub mod supervisor;
mod worker;
mod worker_pool;
pub mod worker_pool_config;

use crate::task::policy::Policies;
use crate::worker::{
    adapter::TaskAdapter,
    dispatcher::TaskDispatcher,
    executor::{TaskExecutor, default_executor::DefaultTaskExecutor},
    queue::TaskQueue,
    runtime::{
        config::TaskRuntimeConfig,
        supervisor::run_prepared,
        worker_pool_config::{TaskWorkerPoolConfig, TaskWorkerPoolConfigError},
    },
};
use std::{fmt, sync::Arc};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

/// Owns the one dispatcher every worker shares, the queue and the executor.
/// Preparation neither starts tasks nor owns a cancellation token. The run
/// future binds a token; its DropGuard starts at first poll.
/// Dropping this object simply releases its dispatcher. It cannot be cloned.
/// A is the concrete adapter type, often [`BoxedTaskAdapter`](crate::worker::adapter::boxed::BoxedTaskAdapter).
pub struct TaskRuntime<Q: TaskQueue, A> {
    context: RuntimeContext<Q, DefaultTaskExecutor<Q>>,
    config: TaskWorkerPoolConfig,
    dispatcher: Arc<TaskDispatcher<A>>,
}

// By hand: the queue need not be `Debug`.
impl<Q: TaskQueue, A> fmt::Debug for TaskRuntime<Q, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskRuntime")
            .field("config", &self.config)
            .field("executor", &self.context.executor)
            .field("dispatcher", &self.dispatcher)
            .finish_non_exhaustive()
    }
}

impl<Q: TaskQueue + 'static, A: TaskAdapter + 'static> TaskRuntime<Q, A> {
    /// Run the workers with the prepared dispatcher.
    /// At first poll a DropGuard owns cancellation; a pre-cancelled token skips
    /// spawn/claims/recovery. Active work retains the existing H+S drain contract.
    pub async fn run(self, shutdown: CancellationToken) -> Result<(), TaskRuntimeError> {
        run_prepared(self.context, self.config, self.dispatcher, shutdown).await
    }
}

/// Assemble the runtime synchronously, before opening inbound listeners: the executor pauses every retry by the
/// policy of its task's type (`policies`). Does not perform storage I/O or spawn Tokio tasks. The dispatcher is shared
/// by every worker and may be shared with the application.
pub fn prepare<Q: TaskQueue + 'static, A: TaskAdapter + 'static>(
    queue: Arc<Q>,
    dispatcher: Arc<TaskDispatcher<A>>,
    config: TaskRuntimeConfig,
    policies: Policies,
) -> TaskRuntime<Q, A> {
    let (storage_config, worker_config) = config.into_parts();
    let executor = Arc::new(DefaultTaskExecutor::new(queue.clone(), storage_config).with_policies(policies));
    TaskRuntime {
        context: RuntimeContext::new(queue, executor),
        config: worker_config,
        dispatcher,
    }
}

struct RuntimeContext<Q: TaskQueue, E: TaskExecutor> {
    queue: Arc<Q>,
    executor: Arc<E>,
}

impl<Q: TaskQueue, E: TaskExecutor> RuntimeContext<Q, E> {
    fn new(queue: Arc<Q>, executor: Arc<E>) -> Self {
        Self { queue, executor }
    }

    fn into_parts(self) -> (Arc<Q>, Arc<E>) {
        (self.queue, self.executor)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TaskRuntimeError {
    #[error(transparent)]
    Configuration(#[from] TaskWorkerPoolConfigError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::format::TaskFormat;
    use crate::worker::{
        TaskResult,
        adapter::TaskAdapter,
        context::TaskContext,
        queue::storage_config::TaskStorageConfig,
        test_support::{Queue, format},
    };

    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    struct TrackedAdapter {
        formats: Vec<TaskFormat>,
        dropped: Arc<AtomicUsize>,
    }
    impl Drop for TrackedAdapter {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl TaskAdapter for TrackedAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            self.formats.clone()
        }
        async fn invoke(&self, _: &TaskContext) -> TaskResult {
            TaskResult::Done
        }
    }
    fn dispatcher(formats: Vec<TaskFormat>, dropped: Arc<AtomicUsize>) -> TaskDispatcher<TrackedAdapter> {
        TaskDispatcher::try_new(vec![TrackedAdapter { formats, dropped }]).unwrap()
    }
    fn config(c: usize) -> TaskRuntimeConfig {
        TaskRuntimeConfig::try_new(
            TaskStorageConfig::default(),
            TaskWorkerPoolConfig::try_new(c, c, Duration::from_secs(1), Duration::from_secs(15), Duration::from_secs(60))
                .unwrap(),
        )
        .unwrap()
    }
    fn assert_no_storage(queue: &Queue) {
        assert!(queue.requested.lock().unwrap().is_empty());
        assert!(queue.outcomes.lock().unwrap().is_empty());
        assert_eq!(queue.recovery_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn synchronous_preparation_shares_one_dispatcher_without_a_tokio_runtime() {
        let queue = Arc::new(Queue::default());
        let dropped = Arc::new(AtomicUsize::new(0));
        let prepared = prepare(
            queue.clone(),
            Arc::new(dispatcher(vec![format()], dropped.clone())),
            config(3),
            Policies::default(),
        );
        assert_eq!(Arc::strong_count(&prepared.dispatcher), 1);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        assert_no_storage(&queue);
        let printed = format!("{prepared:?}");
        let executor = format!(
            "executor: DefaultTaskExecutor {{ storage_timeout: 6s, policies: {:?}, .. }}",
            Policies::default()
        );
        assert!(
            printed.starts_with("TaskRuntime { config: TaskWorkerPoolConfig {"),
            "{printed}"
        );
        assert!(
            printed.contains(&executor) && printed.contains("dispatcher: TaskDispatcher {"),
            "{printed}"
        );
        drop(prepared);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_no_storage(&queue);
    }

    #[tokio::test]
    async fn cancelled_prepared_run_releases_handlers_without_spawn_or_storage() {
        let queue = Arc::new(Queue::default());
        let dropped = Arc::new(AtomicUsize::new(0));
        let prepared = prepare(
            queue.clone(),
            Arc::new(dispatcher(vec![format()], dropped.clone())),
            config(2),
            Policies::default(),
        );
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        tokio::spawn(prepared.run(shutdown)).await.unwrap().unwrap();
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_no_storage(&queue);
    }
}
