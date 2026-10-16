//! Execution and settlement of one leased attempt within its original budget.
pub mod default_executor;
mod lease_budget;

use crate::task::{
    adapter::TaskAdapter,
    dispatcher::TaskDispatcher,
    queue::{TaskProcessingError, lease::LeasedTask, settlement::SettlementResult},
};
use std::{future::Future, time::Duration};
/// Native Send execution boundary. Implement with ordinary `async fn`, without
/// async_trait. The dispatcher is borrowed mutably and the leased task is owned;
/// neither a Clone nor a Sync dispatcher is required. Implementations may borrow
/// non-'static state; runtime spawn boundaries require 'static. Non-Send state
/// cannot survive an await:
/// ```compile_fail
/// use cqrsx::task::{executor::TaskExecutor, adapter::TaskAdapter, dispatcher::TaskDispatcher, queue::{TaskProcessingError, lease::LeasedTask, settlement::SettlementResult}};
/// use std::time::Duration;
/// struct Executor;
/// impl TaskExecutor for Executor {
///     fn storage_timeout(&self) -> Duration { Duration::from_secs(6) }
///     async fn execute_and_settle<A: TaskAdapter>(&self, _: &mut TaskDispatcher<A>, _: LeasedTask, _: Duration)
///         -> Result<SettlementResult, TaskProcessingError> {
///         let state = std::rc::Rc::new(());
///         std::future::pending::<()>().await;
///         drop(state);
///         Ok(SettlementResult::Completed)
///     }
/// }
/// ```
pub trait TaskExecutor: Send + Sync {
    /// Effective storage budget used by worker startup to validate the full lease.
    fn storage_timeout(&self) -> Duration;
    /// Apply the invocation and remaining-lease budgets, then settle its outcome.
    /// A settled result can be retry/pending; it is not necessarily completion.
    /// LeaseBudgetExhausted skips unsafe execution/settlement and leaves recovery
    /// to the durable queue. The handler never owns or renews platform leases.
    fn execute_and_settle<A: TaskAdapter>(
        &self,
        dispatcher: &mut TaskDispatcher<A>,
        task: LeasedTask,
        handler_timeout: Duration,
    ) -> impl Future<Output = Result<SettlementResult, TaskProcessingError>> + Send;
}
