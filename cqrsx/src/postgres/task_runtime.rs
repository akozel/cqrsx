use crate::{
    postgres::task_queue::PostgresTaskQueue,
    task::policy::Policies,
    worker::{
        adapter::TaskAdapter,
        dispatcher::TaskDispatcher,
        runtime::{TaskRuntime, config::TaskRuntimeConfig, prepare},
    },
};
use sqlx::PgPool;
use std::sync::Arc;

/// Assemble the task queue, the executor and the dispatcher every worker shares before opening listeners: a retry
/// waits the pause of its task type's policy (`policies`). Does not connect, execute SQL, read env or start
/// background tasks. The host chooses handlers; the returned object starts only when `run` is polled.
/// The queue shares `pool` with the command executors; its statements are bounded by the
/// pool's `StatementTimeout`.
pub fn prepare_task_runtime<A: TaskAdapter + 'static>(
    pool: PgPool,
    dispatcher: Arc<TaskDispatcher<A>>,
    runtime_config: TaskRuntimeConfig,
    policies: Policies,
) -> TaskRuntime<PostgresTaskQueue, A> {
    prepare(Arc::new(PostgresTaskQueue::new(pool)), dispatcher, runtime_config, policies)
}
