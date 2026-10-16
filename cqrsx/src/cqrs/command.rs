//! Command gateways with bounded optimistic-conflict retry.
use crate::{cqrs::aggregate::Aggregate, task::NewTask};
use std::future::Future;
use thiserror::Error;
pub mod executor;
pub mod retry;

/// Technical command boundary; the application gateway selects the aggregate ID.
///
/// Implementations own persistence and retry policy without interpreting domain
/// rejections. Tasks belong to this invocation and must not leak into other calls.
/// Implement with an ordinary `async fn`, without `async_trait`. The future must
/// be `Send` and may borrow the aggregate ID through awaits. Commands require
/// `Send`; the CQRS retry engine additionally needs `Clone`, never `Sync`.
/// A future holding non-Send state across await violates the contract:
/// ```compile_fail
/// use cqrsx::cqrs::aggregate::Aggregate;
/// use cqrsx::{cqrs::command::{CommandExecutor, CommandGatewayError}, task::NewTask};
/// struct NonSendExecutor;
/// impl<A: Aggregate> CommandExecutor<A> for NonSendExecutor where A::Command: Send {
///     async fn execute(&self, _: &str, _: A::Command, _: Vec<NewTask>) -> Result<(), CommandGatewayError<A::Error>> {
///         let state = std::rc::Rc::new(());
///         std::future::pending::<()>().await;
///         drop(state);
///         Ok(())
///     }
/// }
/// ```
pub trait CommandExecutor<A>: Send + Sync
where
    A: Aggregate,
    A::Command: Send,
{
    fn execute(
        &self,
        aggregate_id: &str,
        command: A::Command,
        tasks: Vec<NewTask>,
    ) -> impl Future<Output = Result<(), CommandGatewayError<A::Error>>> + Send;
}

/// Domain rejection, exhausted conflict, unavailable storage, or a confirmed commit with event-handler failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CommandGatewayError<D> {
    #[error("domain command failed a domain rule: {0}")]
    Domain(D),
    #[error("domain command conflicted with a concurrent change")]
    Conflict,
    #[error("command gateway is temporarily unavailable")]
    Unavailable,
    /// The event commit succeeded. Repair the event-handler failure without repeating the command.
    #[error("command committed through sequence {last_sequence}; event handlers failed")]
    CommittedWithEventHandlerFailure {
        last_sequence: usize,
        handler_indices: Vec<usize>,
    },
}
