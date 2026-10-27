//! Command gateways: the executor contract and its outcomes, how a command decides ([`decide`]) and what is known
//! of it ([`CommandInfo`]), the aggregate locks that order the commands of one aggregate, bounded
//! optimistic-conflict retry, the causation metadata convention and a command's [`metadata`], the planning of a
//! command's follow-up tasks from its decision and what an executor tells its [`observer`] about every command.
use crate::cqrs::{CommitReport, aggregate::Aggregate};
use std::{collections::HashMap, future::Future};
use thiserror::Error;
use uuid::Uuid;
pub mod causation;
pub mod decide;
pub mod executor;
pub mod locks;
pub mod metadata;
pub mod observer;
pub mod planner;
pub mod retry;
pub mod wakeup;

/// Technical command boundary; the application gateway selects the aggregate by its typed ID ([`Aggregate::Id`]).
///
/// Implementations own persistence and retry policy without interpreting domain
/// rejections. The caller passes only the command: the executor's
/// [task planner](planner::TaskPlanner) plans the follow-up tasks from the decision, and
/// the answer reports what committed ([`CommitReport`]). Event metadata travels with
/// the invocation: every event the command commits stores the same map, and
/// [`execute`](Self::execute) is the metadata-free form that delegates with an empty map.
/// Implement [`execute_with_metadata`](Self::execute_with_metadata) with an
/// ordinary `async fn`, without `async_trait`. The future must be `Send` and may
/// borrow the aggregate ID through awaits. Commands are `Send` ([`Aggregate::Command`]); the CQRS
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
///         _: &A::Id,
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
pub trait CommandExecutor<A: Aggregate>: Send + Sync {
    /// Executes without event metadata. The default delegates to
    /// [`execute_with_metadata`](Self::execute_with_metadata) with an empty map.
    fn execute(
        &self,
        id: &A::Id,
        command: A::Command,
    ) -> impl Future<Output = Result<CommitReport<A>, CommandGatewayError<A::Error>>> + Send {
        self.execute_with_metadata(id, command, HashMap::new())
    }

    /// Executes one command and stores `metadata` with every committed event; the answer reports the
    /// committed events and the tasks planned from them.
    fn execute_with_metadata(
        &self,
        id: &A::Id,
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

/// What is known of a command besides its decision: what an observer reports it by and what an assembly takes its id
/// from. `#[derive(cqrsx::Command)]` implements it on a command type, `#[derive(cqrsx::Commands)]` on the aggregate's
/// enum of commands, by the variant's type; the core does not read it.
///
/// - [`name`](Self::name): the command type's name, `"CancelBooking"`.
/// - [`command_id`](Self::command_id): the field marked `#[command_id]`, when the business id is the id of the command
///   (a booking, a cancellation); a `Uuid`, at most one field. Without it, `None`: the id is not the command's.
/// - [`labels`](Self::labels): the fields marked `#[label]`, in field order, each as its name and its `Display`; what
///   an activity feed shows and filters a command by.
///
/// ```
/// use cqrsx::{Command, Commands, cqrs::command::CommandInfo};
/// use uuid::Uuid;
///
/// #[derive(Command)]
/// struct CancelBooking {
///     seat: String,
///     #[label]
///     fan_id: String,
///     #[command_id]
///     cancellation_id: Uuid,
///     #[label]
///     reason: u8,
/// }
///
/// #[derive(Command)]
/// struct AnnounceConcert {
///     artist: String,
/// }
///
/// let cancellation_id = Uuid::now_v7();
/// let cancel = CancelBooking { seat: "B4".into(), fan_id: "fox".into(), cancellation_id, reason: 2 };
/// assert_eq!(cancel.name(), "CancelBooking");
/// assert_eq!(cancel.command_id(), Some(cancellation_id));
/// assert_eq!(cancel.labels(), [("fan_id", "fox".to_owned()), ("reason", "2".to_owned())]);
/// let announce = AnnounceConcert { artist: "Radiohead".into() };
/// assert_eq!((announce.name(), announce.command_id(), announce.labels()), ("AnnounceConcert", None, vec![]));
/// ```
///
/// One field at most is the command id:
///
/// ```compile_fail
/// #[derive(cqrsx::Command)]
/// struct BookSeat {
///     #[command_id]
///     booking_id: uuid::Uuid,
///     #[command_id]
///     request_id: uuid::Uuid,
/// }
/// ```
///
/// and it is a `Uuid`:
///
/// ```compile_fail
/// #[derive(cqrsx::Command)]
/// struct BookSeat {
///     #[command_id]
///     booking_id: String,
/// }
/// ```
pub trait CommandInfo {
    /// The name of the command's type.
    fn name(&self) -> &'static str;

    /// The id of the command, when a field of it is (`#[command_id]`); `None` by default.
    fn command_id(&self) -> Option<Uuid> {
        None
    }

    /// The labels of the command, `(field, value)` in field order (`#[label]`); none by default.
    fn labels(&self) -> Vec<(&'static str, String)> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Ping;

    impl CommandInfo for Ping {
        fn name(&self) -> &'static str {
            "Ping"
        }
    }

    #[test]
    fn a_command_info_written_by_hand_has_no_id_and_no_labels_unless_it_says_so() {
        assert_eq!((Ping.name(), Ping.command_id(), Ping.labels()), ("Ping", None, vec![]));
    }
}
