//! Command gateways: the executor contract and its outcomes, the aggregate locks that order the commands of
//! one aggregate, bounded optimistic-conflict retry and the causation metadata convention.
use crate::{cqrs::aggregate::Aggregate, task::NewTask};
use std::{collections::HashMap, future::Future};
use thiserror::Error;
pub mod causation;
pub mod executor;
pub mod locks;
pub mod retry;
pub mod wakeup;

/// Technical command boundary; the application gateway selects the aggregate ID.
///
/// Implementations own persistence and retry policy without interpreting domain
/// rejections. Tasks belong to this invocation and must not leak into other calls.
/// Event metadata travels with the invocation as well: every event the command
/// commits stores the same map, and [`execute`](Self::execute) is the
/// metadata-free form that delegates with an empty map.
/// Implement [`execute_with_metadata`](Self::execute_with_metadata) with an
/// ordinary `async fn`, without `async_trait`. The future must be `Send` and may
/// borrow the aggregate ID through awaits. Commands require `Send`; the CQRS
/// retry engine additionally needs `Clone`, never `Sync`.
/// A future holding non-Send state across await violates the contract:
/// ```compile_fail
/// use cqrsx::cqrs::aggregate::Aggregate;
/// use cqrsx::{cqrs::command::{CommandExecutor, CommandGatewayError}, task::NewTask};
/// use std::collections::HashMap;
/// struct NonSendExecutor;
/// impl<A: Aggregate> CommandExecutor<A> for NonSendExecutor where A::Command: Send {
///     async fn execute_with_metadata(
///         &self,
///         _: &str,
///         _: A::Command,
///         _: HashMap<String, String>,
///         _: Vec<NewTask>,
///     ) -> Result<(), CommandGatewayError<A::Error>> {
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
    /// Executes without event metadata. The default delegates to
    /// [`execute_with_metadata`](Self::execute_with_metadata) with an empty map.
    fn execute(
        &self,
        aggregate_id: &str,
        command: A::Command,
        tasks: Vec<NewTask>,
    ) -> impl Future<Output = Result<(), CommandGatewayError<A::Error>>> + Send {
        self.execute_with_metadata(aggregate_id, command, HashMap::new(), tasks)
    }

    /// Executes one command and stores `metadata` with every committed event.
    fn execute_with_metadata(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
        tasks: Vec<NewTask>,
    ) -> impl Future<Output = Result<(), CommandGatewayError<A::Error>>> + Send;
}

/// Domain rejection, exhausted conflict retry, or a command that was not run or whose outcome is unknown.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CommandGatewayError<D> {
    #[error("domain command failed a domain rule: {0}")]
    Domain(D),
    #[error("domain command conflicted with a concurrent change")]
    Conflict,
    /// Not run, or outcome unknown: the storage failed or timed out, or the aggregate's lock was not acquired
    /// within its `max_wait`. The reason is logged.
    #[error("command gateway is temporarily unavailable")]
    Unavailable,
}
