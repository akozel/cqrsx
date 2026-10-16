//! Durable ownership transitions, lease fencing, settlement and recovery.
pub mod lease;
pub mod retry;
pub mod settlement;
pub mod storage_config;

use crate::task::{
    format::TaskFormat,
    queue::{
        lease::LeasedTask,
        settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult},
    },
};
use std::{future::Future, time::Duration};
use thiserror::Error;
/// Adapters atomically claim ready work, increment attempts and issue fresh tokens.
/// Anchor local lease time before requesting ownership. Fence settlement by token
/// AND database expiry; cancellation may leave an unknown committed result.
/// Durable recovery must retain abandoned work. No FIFO or exactly-once effects.
/// Implement with ordinary `async fn`. Futures borrow the original inputs and
/// must be Send even when the queue itself borrows non-'static state. Runtime
/// assembly requires 'static only where workers are spawned. Holding Rc across
/// an await is rejected at this port:
/// ```compile_fail
/// use cqrsx::task::{format::TaskFormat, queue::{TaskQueue, TaskProcessingError, lease::LeasedTask, settlement::{SettlementRequest, SettlementResult, LeaseRecoveryReport}}};
/// use std::time::Duration;
/// struct Queue;
/// impl TaskQueue for Queue {
///     async fn claim(&self, _: &TaskFormat, _: &str, _: usize, _: Duration)
///         -> Result<Vec<LeasedTask>, TaskProcessingError> {
///         let state = std::rc::Rc::new(());
///         std::future::pending::<()>().await;
///         drop(state);
///         Ok(vec![])
///     }
///     async fn settle(&self, _: &LeasedTask, _: SettlementRequest)
///         -> Result<SettlementResult, TaskProcessingError> { unimplemented!() }
///     async fn recover_expired(&self, _: usize)
///         -> Result<LeaseRecoveryReport, TaskProcessingError> { unimplemented!() }
/// }
/// ```
pub trait TaskQueue: Send + Sync {
    fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> impl Future<Output = Result<Vec<LeasedTask>, TaskProcessingError>> + Send;
    fn settle(
        &self,
        task: &LeasedTask,
        request: SettlementRequest,
    ) -> impl Future<Output = Result<SettlementResult, TaskProcessingError>> + Send;
    fn recover_expired(&self, limit: usize) -> impl Future<Output = Result<LeaseRecoveryReport, TaskProcessingError>> + Send;
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum QueueRequestError {
    #[error("queue batch limit is not representable")]
    InvalidLimit,
    #[error("lease duration must be positive and representable")]
    InvalidLeaseDuration,
    #[error("retry delay is not representable")]
    InvalidRetryDelay,
}
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TaskProcessingError {
    #[error(transparent)]
    InvalidRequest(#[from] QueueRequestError),
    #[error("queue returned structurally invalid delivery data")]
    InvalidDelivery,
    #[error("execution budget must be positive and representable")]
    InvalidExecutionBudget,
    #[error("task queue storage is unavailable")]
    Unavailable,
    #[error("task queue storage operation timed out; its result is unknown")]
    TimedOut,
    #[error("task lease budget exhausted; task requires durable recovery")]
    LeaseBudgetExhausted,
}
