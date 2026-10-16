//! Task adaptation before invoking a business handler.
pub mod json;

use crate::task::{attempt::TaskAttempt, format::TaskFormat, handler::TaskHandlerError};
use std::future::Future;

/// An adapter advertises supported formats and invokes their business handler.
/// JSON adapters decode one DTO version; custom adapters can convert several
/// versions to the same business input. They preserve the attempt context and
/// add no retry, settlement or workflow sequencing.
/// Implement with a native Send future. The original attempt stays borrowed;
/// adapters may borrow non-'static state and need neither Sync nor Clone.
/// Non-Send state across await is rejected independently of runtime spawning:
/// ```compile_fail
/// use cqrsx::task::{attempt::TaskAttempt, format::TaskFormat, adapter::TaskAdapter, handler::TaskHandlerError};
/// struct Adapter;
/// impl TaskAdapter for Adapter {
///     fn formats(&self) -> Vec<TaskFormat> { vec![] }
///     async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
///         let state = std::rc::Rc::new(());
///         std::future::pending::<()>().await;
///         drop(state);
///         Ok(())
///     }
/// }
/// ```
pub trait TaskAdapter: Send {
    fn formats(&self) -> Vec<TaskFormat>;
    fn invoke(&mut self, attempt: &TaskAttempt) -> impl Future<Output = Result<(), TaskHandlerError>> + Send;
}
