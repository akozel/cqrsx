//! Command gateways: the executor contract and its outcomes, the aggregate locks that order the commands of
//! one aggregate, bounded optimistic-conflict retry, the causation metadata convention and the planning of a
//! command's follow-up tasks from its decision.
use crate::cqrs::{CommitReport, aggregate::Aggregate};
use std::{collections::HashMap, future::Future};
use thiserror::Error;
pub mod causation;
pub mod executor;
pub mod locks;
pub mod planner;
pub mod retry;
pub mod wakeup;

/// Technical command boundary; the application gateway selects the aggregate ID.
///
/// Implementations own persistence and retry policy without interpreting domain
/// rejections. The caller passes only the command: the executor's
/// [task planner](planner::TaskPlanner) plans the follow-up tasks from the decision, and
/// the answer reports what committed ([`CommitReport`]). Event metadata travels with
/// the invocation: every event the command commits stores the same map, and
/// [`execute`](Self::execute) is the metadata-free form that delegates with an empty map.
/// Implement [`execute_with_metadata`](Self::execute_with_metadata) with an
/// ordinary `async fn`, without `async_trait`. The future must be `Send` and may
/// borrow the aggregate ID through awaits. Commands require `Send`; the CQRS
/// retry engine additionally needs `Clone`, never `Sync`.
/// A future holding non-Send state across await violates the contract:
/// ```compile_fail
/// use cqrsx::cqrs::aggregate::Aggregate;
/// use cqrsx::cqrs::{CommitReport, command::{CommandExecutor, CommandGatewayError}};
/// use std::collections::HashMap;
/// struct NonSendExecutor;
/// impl<A: Aggregate> CommandExecutor<A> for NonSendExecutor where A::Command: Send {
///     async fn execute_with_metadata(
///         &self,
///         _: &str,
///         _: A::Command,
///         _: HashMap<String, String>,
///     ) -> Result<CommitReport<A>, CommandGatewayError<A::Error>> {
///         let state = std::rc::Rc::new(());
///         std::future::pending::<()>().await;
///         drop(state);
///         Ok(CommitReport::default())
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
    ) -> impl Future<Output = Result<CommitReport<A>, CommandGatewayError<A::Error>>> + Send {
        self.execute_with_metadata(aggregate_id, command, HashMap::new())
    }

    /// Executes one command and stores `metadata` with every committed event; the answer reports the
    /// committed events and the tasks planned from them.
    fn execute_with_metadata(
        &self,
        aggregate_id: &str,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> impl Future<Output = Result<CommitReport<A>, CommandGatewayError<A::Error>>> + Send;
}

/// Domain rejection, exhausted conflict retry, or a command that was not run or whose outcome is unknown.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CommandGatewayError<D> {
    #[error("domain command failed a domain rule: {0}")]
    Domain(D),
    #[error("domain command conflicted with a concurrent change")]
    Conflict,
    /// Not run, or outcome unknown: the storage failed or timed out, the aggregate's lock was not acquired
    /// within its `max_wait`, or the decision's tasks could not be planned (nothing was written). The reason
    /// is logged.
    #[error("command gateway is temporarily unavailable")]
    Unavailable,
}
