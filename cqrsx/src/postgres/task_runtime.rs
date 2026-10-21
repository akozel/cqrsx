use crate::{
    postgres::task_queue::PostgresTaskQueue,
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
/// The queue shares `pool` with the command executors; its statements are bounded by the
/// pool's `StatementTimeout`.
pub fn prepare_task_runtime<A: TaskAdapter + 'static>(
    pool: PgPool,
    factory: TaskDispatcherFactory<A>,
    runtime_config: TaskRuntimeConfig,
) -> Result<TaskRuntime<PostgresTaskQueue, A>, WorkerSetupError> {
    prepare(Arc::new(PostgresTaskQueue::new(pool)), factory, runtime_config)
}
