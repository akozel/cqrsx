//! Task adaptation before invoking a business handler.
pub mod boxed;
pub mod json;

use crate::task::{context::TaskContext, dependencies::DependencyId, format::TaskFormat, result::TaskResult};
use std::future::Future;

/// An adapter advertises supported formats and invokes their business handler.
/// JSON adapters decode one DTO version; custom adapters can convert several
/// versions to the same business input. They pass the attempt's context on and
/// add no retry, settlement or workflow sequencing.
/// Implement with a native Send future. One adapter serves every worker, so it is `Send + Sync`;
/// it may borrow non-'static state and needs no `Clone`.
/// [`BoxedTaskAdapter`](boxed::BoxedTaskAdapter) erases the adapter type, so one dispatcher can
/// hold adapters of different types at the cost of one allocation per invocation.
/// Non-Send state across await is rejected independently of runtime spawning:
/// ```compile_fail
/// use cqrsx::task::{adapter::TaskAdapter, context::TaskContext, format::TaskFormat, result::TaskResult};
/// struct Adapter;
/// impl TaskAdapter for Adapter {
///     fn formats(&self) -> Vec<TaskFormat> { vec![] }
///     async fn invoke(&self, _: &TaskContext) -> TaskResult {
///         let state = std::rc::Rc::new(());
///         std::future::pending::<()>().await;
///         drop(state);
///         TaskResult::done()
///     }
/// }
/// ```
pub trait TaskAdapter: Send + Sync {
    /// The formats this adapter handles; the dispatcher routes every attempt of these formats here. Called when the
    /// dispatcher is assembled, not per task: a format two adapters claim fails the assembly
    /// ([`DuplicateFormat`](crate::task::dispatcher::TaskDispatcherError::DuplicateFormat)), and so does a dispatcher
    /// with no format at all (`Empty`). The workers claim the formats in this order.
    fn formats(&self) -> Vec<TaskFormat>;

    /// What the handlers behind this adapter need from the assembly; none by default.
    fn dependencies(&self) -> Vec<DependencyId> {
        Vec::new()
    }

    /// Handles one attempt of a task in one of [`formats`](Self::formats): `ctx` holds the attempt (payload, metadata,
    /// attempt number), the pause a retry would get, the dependencies and the command executors. The result settles
    /// the attempt; a panic of the future is caught by the worker and the attempt is retried.
    fn invoke(&self, ctx: &TaskContext) -> impl Future<Output = TaskResult> + Send;
}
