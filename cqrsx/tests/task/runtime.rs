//! Task runtime behavior through the public queue, executor and runtime modules.

use cqrsx::task::{
    adapter::TaskAdapter,
    attempt::TaskAttempt,
    dispatcher::{TaskDispatcher, TaskDispatcherError},
    executor::default_executor::DefaultTaskExecutor,
    format::TaskFormat,
    handler::TaskHandlerError,
    queue::{
        TaskProcessingError, TaskQueue,
        lease::LeasedTask,
        settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult},
        storage_config::TaskStorageConfig,
    },
    runtime::{
        TaskRuntimeError,
        dispatcher_factory::{TaskDispatcherFactory, WorkerSetupError},
        supervisor::run,
        worker_pool_config::TaskWorkerPoolConfig,
    },
};
use std::{
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
