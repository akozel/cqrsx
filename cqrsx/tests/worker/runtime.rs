//! Task runtime behavior through the public queue, executor and runtime modules.

use cqrsx::task::format::TaskFormat;
use cqrsx::worker::{
    TaskResult,
    adapter::TaskAdapter,
    context::TaskContext,
    dispatcher::{TaskDispatcher, TaskDispatcherError},
    executor::default_executor::DefaultTaskExecutor,
    queue::{
        TaskProcessingError, TaskQueue,
        lease::LeasedTask,
        settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult},
        storage_config::TaskStorageConfig,
    },
    runtime::{TaskRuntimeError, supervisor::run, worker_pool_config::TaskWorkerPoolConfig},
};
use std::{
    num::NonZeroU16,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::task::Id;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[path = "runtime/admission.rs"]
mod admission;
#[path = "runtime/preparation.rs"]
mod preparation;
#[path = "runtime/shutdown.rs"]
mod shutdown;
#[path = "runtime/supervision.rs"]
mod supervision;
#[path = "runtime/support.rs"]
mod support;
use support::*;
