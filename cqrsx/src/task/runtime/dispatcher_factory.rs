//! Checked per-worker dispatcher construction for preparation and restart.
use crate::task::{
    adapter::TaskAdapter,
    dispatcher::{TaskDispatcher, TaskDispatcherError},
    format::TaskFormat,
};
use std::sync::Arc;
use thiserror::Error;
/// Fresh handler dispatcher per worker/restart; every instance must expose the
/// same ordered format list as the first retained dispatcher. Sender state
/// belongs to a worker. The closure is Send + Sync; its returned adapter only
/// needs Send and may contain mutable non-Sync state.
pub type TaskDispatcherFactory<A> = Arc<dyn Fn() -> Result<TaskDispatcher<A>, TaskDispatcherError> + Send + Sync>;

/// Dispatcher construction or format mismatch, shared by preparation and restart.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkerSetupError {
    #[error("worker {worker_index} dispatcher construction failed: {source}")]
    Build {
        worker_index: usize,
        #[source]
        source: TaskDispatcherError,
    },
    #[error("worker {worker_index} dispatcher formats differ from the initial dispatcher")]
    FormatsMismatch { worker_index: usize },
}

pub(super) fn build_checked_dispatcher<A: TaskAdapter>(
    factory: &TaskDispatcherFactory<A>,
    expected_formats: &[TaskFormat],
    worker_index: usize,
) -> Result<TaskDispatcher<A>, WorkerSetupError> {
    let dispatcher = factory().map_err(|source| WorkerSetupError::Build { worker_index, source })?;
    if dispatcher.formats() == expected_formats {
        Ok(dispatcher)
    } else {
        Err(WorkerSetupError::FormatsMismatch { worker_index })
    }
}
