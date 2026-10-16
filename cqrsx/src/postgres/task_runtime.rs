use crate::{
    postgres::task_queue::{PostgresTaskQueue, PostgresTaskQueueConfig},
    task::{
        adapter::TaskAdapter,
        runtime::{
            TaskRuntime,
            config::TaskRuntimeConfig,
            dispatcher_factory::{TaskDispatcherFactory, WorkerSetupError},
            prepare,
        },
    },
};
use sqlx::PgPool;
use std::sync::Arc;

/// Assemble the task queue, executor and all startup dispatchers before opening
/// listeners. Does not connect, execute SQL, read env or start background tasks.
/// The host chooses handlers; the returned object starts only when `run` is polled.
pub fn prepare_task_runtime<A: TaskAdapter + 'static>(
    pool: PgPool,
    factory: TaskDispatcherFactory<A>,
    queue_config: PostgresTaskQueueConfig,
    runtime_config: TaskRuntimeConfig,
) -> Result<TaskRuntime<PostgresTaskQueue, A>, WorkerSetupError> {
    let queue = Arc::new(PostgresTaskQueue::with_config(pool, queue_config));
    prepare(queue, factory, runtime_config)
}
