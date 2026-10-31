//! Executing a command: the executor port and its outcomes ([`CommandExecutor`], [`CommandGatewayError`],
//! [`CommitReport`], [`AggregateError`]), one attempt ([`framework`]), the executor ([`executor`]), the aggregate
//! [`locks`] that order the commands of one aggregate, bounded optimistic-conflict [`retry`], the [`causation`]
//! convention and a command's [`metadata`], the [`planner`] of a command's follow-up tasks from its decision and the
//! [`observer`] an executor tells about every command. How a command decides and what is known of it live in
//! [`domain::command`](crate::domain::command).
use crate::{
    command::planner::TaskPlanError,
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::PersistenceError,
    task::NewTask,
};
use std::{collections::HashMap, error, fmt, future::Future};
use thiserror::Error;
pub mod causation;
pub mod executor;
pub mod framework;
pub mod locks;
pub mod metadata;
pub mod observer;
pub mod planner;
pub mod retry;

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
/// use cqrsx::domain::aggregate::Aggregate;
/// use cqrsx::command::{CommitReport, CommandExecutor, CommandGatewayError};
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

/// What one command committed in one transaction: its events in commit order (the one event it decided, as
/// the store reports it) and the tasks planned from them, published atomically with them.
///
/// `Default` is derived: its bound `A: Default` holds for every aggregate.
#[derive(Default)]
pub struct CommitReport<A: Aggregate> {
    pub events: Vec<EventEnvelope<A>>,
    pub tasks: Vec<NewTask>,
}

// By hand: a derive would require `A` itself to be `Clone`, `Debug` and `PartialEq`, not only its events.
impl<A: Aggregate> Clone for CommitReport<A> {
    fn clone(&self) -> Self {
        Self {
            events: self.events.clone(),
            tasks: self.tasks.clone(),
        }
    }
}

impl<A: Aggregate> fmt::Debug for CommitReport<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitReport")
            .field("events", &self.events)
            .field("tasks", &self.tasks)
            .finish()
    }
}

impl<A: Aggregate> PartialEq for CommitReport<A> {
    fn eq(&self, other: &Self) -> bool {
        self.events == other.events && self.tasks == other.tasks
    }
}

/// The base error for the framework.
#[derive(Debug, thiserror::Error)]
pub enum AggregateError<T: error::Error> {
    /// A domain rule rejected the command; the payload is the aggregate domain error.
    #[error("{0}")]
    Domain(T),
    /// A command has been rejected due to a conflict with another command on the same aggregate
    /// instance. This is handled by optimistic locking in systems backed by an RDBMS.
    #[error("aggregate conflict")]
    AggregateConflict,
    /// A error occurred while attempting to read or write from a database.
    #[error("{0}")]
    DatabaseConnectionError(#[source] Box<dyn error::Error + Send + Sync + 'static>),
    /// A deserialization error occurred due to invalid JSON.
    #[error("{0}")]
    DeserializationError(#[source] Box<dyn error::Error + Send + Sync + 'static>),
    /// A technical error was encountered that prevented the command from being applied to the
    /// aggregate. In general the accompanying message should be logged for investigation rather
    /// than returned to the user.
    #[error("{0}")]
    UnexpectedError(#[source] Box<dyn error::Error + Send + Sync + 'static>),
    /// The aggregate decided, but the decision's tasks could not be planned; nothing was written.
    #[error("{0}")]
    TaskPlan(#[source] TaskPlanError),
}

impl<T: std::error::Error> From<PersistenceError> for AggregateError<T> {
    fn from(err: PersistenceError) -> Self {
        match err {
            PersistenceError::OptimisticLockError => Self::AggregateConflict,
            PersistenceError::ConnectionError(error) => Self::DatabaseConnectionError(error),
            PersistenceError::DeserializationError(error) => Self::DeserializationError(error),
            PersistenceError::UnknownError(error) => Self::UnexpectedError(error),
            gap @ PersistenceError::SequenceGap { .. } => Self::UnexpectedError(Box::new(gap)),
        }
    }
}

impl<T: error::Error> From<serde_json::error::Error> for AggregateError<T> {
    fn from(err: serde_json::error::Error) -> Self {
        match err.classify() {
            serde_json::error::Category::Data | serde_json::error::Category::Syntax => Self::DeserializationError(Box::new(err)),
            serde_json::error::Category::Io | serde_json::error::Category::Eof => Self::UnexpectedError(Box::new(err)),
        }
    }
}
