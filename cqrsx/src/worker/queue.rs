//! Durable ownership transitions, lease fencing, settlement and recovery.
pub mod lease;
pub mod settlement;
pub mod storage_config;

use crate::task::format::TaskFormat;
use crate::worker::queue::{
    lease::LeasedTask,
    settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult},
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
/// use cqrsx::task::format::TaskFormat;
/// use cqrsx::worker::queue::{TaskQueue, TaskProcessingError, lease::LeasedTask, settlement::{SettlementRequest, SettlementResult, LeaseRecoveryReport}};
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
    /// Takes up to `limit` ready tasks of `format` (pending and due) for `owner` in one atomic step: each one's
    /// attempt counter grows by one, it gets a fresh lock token and a lease of `lease_duration`, measured locally
    /// from before the request ([`LeasedTask`]). No order is promised. A row that is not a valid task is not
    /// delivered: the PostgreSQL queue logs it and leaves it leased for [`recover_expired`](Self::recover_expired),
    /// so it cannot block its format. A storage failure is [`Unavailable`](TaskProcessingError::Unavailable).
    fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> impl Future<Output = Result<Vec<LeasedTask>, TaskProcessingError>> + Send;
    /// Applies `request` to a claimed task if its lease still holds: the token matches and the lease has not
    /// expired in the database. Otherwise the answer is
    /// [`OwnershipLost`](crate::worker::queue::settlement::SettlementResult::OwnershipLost) and nothing changes. A
    /// retry beyond the task's attempt budget fails it instead. Returns only a committed result.
    fn settle(
        &self,
        task: &LeasedTask,
        request: SettlementRequest,
    ) -> impl Future<Output = Result<SettlementResult, TaskProcessingError>> + Send;
    /// Returns up to `limit` tasks whose lease expired to the queue, after a redelivery pause, or fails them on their
    /// last attempt ([`LeaseExpired`](crate::worker::queue::settlement::TaskFailureCause::LeaseExpired)); the report
    /// counts both, after commit.
    fn recover_expired(&self, limit: usize) -> impl Future<Output = Result<LeaseRecoveryReport, TaskProcessingError>> + Send;
}

/// A request the queue cannot express in its storage: a limit or a duration out of range.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum QueueRequestError {
    #[error("queue batch limit is not representable")]
    InvalidLimit,
    #[error("lease duration must be positive and representable")]
    InvalidLeaseDuration,
    #[error("retry delay is not representable")]
    InvalidRetryDelay,
}
/// Why claiming, running or settling a task failed. A queue returns `InvalidRequest`, `InvalidDelivery` (data read
/// back from storage that is not a valid task, a settlement or a failure) and `Unavailable`; the runtime adds `TimedOut` (a storage call longer than the storage timeout) and the executor
/// `InvalidExecutionBudget` and `LeaseBudgetExhausted`.
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
