//! Event-store contracts, persistence errors and repository-backed implementations.
pub mod feed;
pub mod repository;
pub mod serialized_event;
pub mod snapshot;
pub mod store;
pub mod upcaster;
pub mod wakeup;

// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and cqrsx/licenses/.
use crate::{
    domain::{aggregate::Aggregate, event::EventEnvelope},
    task::NewTask,
};
use std::{collections::HashMap, future::Future};

/// The abstract central source for loading past events and committing new events.
pub trait EventStore<A>: Send + Sync
where
    A: Aggregate,
{
    /// Provides the current state of an aggregate along with surrounding context.
    /// This is used by the command executor (`StoreCommandExecutor`) when loading
    /// an aggregate in order to handle incoming commands.
    type Context: AggregateContext<A>;

    /// Load all events for a particular `aggregate_id`
    fn load_events(&self, aggregate_id: &str) -> impl Future<Output = Result<Vec<EventEnvelope<A>>, PersistenceError>> + Send;
    /// Load aggregate at current state
    fn load_aggregate(&self, aggregate_id: &str) -> impl Future<Output = Result<Self::Context, PersistenceError>> + Send;
    /// Commit new events and the tasks planned from them using the last committed aggregate context.
    /// The tasks commit atomically with the events or the commit fails before writing anything; a task
    /// never commits without an event. A store without an outbox refuses non-empty `tasks`.
    fn commit(
        &self,
        events: Vec<A::Event>,
        context: Self::Context,
        metadata: HashMap<String, String>,
        tasks: &[NewTask],
    ) -> impl Future<Output = Result<Vec<EventEnvelope<A>>, PersistenceError>> + Send;
}

/// Returns the aggregate as well as the context around it.
///
/// This is used internally within an `EventStore` to persist an aggregate instance and events
/// with its committed context. Command handling uses a separate working copy.
pub trait AggregateContext<A>
where
    A: Aggregate,
{
    /// The aggregate instance with all state loaded.
    fn aggregate(&self) -> &A;
}

/// Validate a complete batch before applying or serializing any of its events.
pub(crate) fn checked_counter_add(current: usize, count: usize) -> Result<usize, crate::journal::PersistenceError> {
    current
        .checked_add(count)
        .ok_or_else(|| crate::journal::PersistenceError::Unexpected("event sequence or snapshot version overflow".into()))
}

/// Errors for implementations of a persistent event store.
///
/// A store that can tell the category and the code of a failure wraps the error of its driver in
/// [`StorageFailure`](crate::diagnostics::StorageFailure) before it boxes it here; the logs of a failed command or
/// projection batch then carry them.
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    /// Optimistic locking conflict occurred while committing and aggregate.
    #[error("conflict with a concurrent write")]
    Conflict,
    /// An error occurred connecting to the database.
    #[error("{0}")]
    Connection(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Error occurred while attempting to deserialize data.
    #[error("{0}")]
    Deserialization(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// An unexpected error occurred while accessing the database.
    #[error("{0}")]
    Unexpected(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// A view document is behind the delivered events: it has applied `expected - 1` of its
    /// aggregate and the batch continues at `found`. Nothing of the batch is written; the document
    /// stays a correct prefix of its aggregate's history until a rebuild of the key fills the gap.
    #[error("sequence gap: expected event {expected}, found {found}")]
    SequenceGap {
        /// The next event the document can apply (`last_sequence + 1`).
        expected: usize,
        /// The first event of the batch above `expected`.
        found: usize,
    },
}

impl From<serde_json::Error> for PersistenceError {
    fn from(err: serde_json::Error) -> Self {
        match err.classify() {
            serde_json::error::Category::Data | serde_json::error::Category::Syntax => Self::Deserialization(Box::new(err)),
            serde_json::error::Category::Io | serde_json::error::Category::Eof => Self::Unexpected(Box::new(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    fn serde_errors() -> Vec<serde_json::Error> {
        struct BrokenReader;
        impl io::Read for BrokenReader {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("reader failed"))
            }
        }
        vec![
            serde_json::from_str::<u32>("false").unwrap_err(),
            serde_json::from_str::<u32>("?").unwrap_err(),
            serde_json::from_str::<u32>("").unwrap_err(),
            serde_json::from_reader::<_, u32>(BrokenReader).unwrap_err(),
        ]
    }
    #[test]
    fn serde_data_syntax_eof_and_io_keep_distinct_mappings_and_causes() {
        for error in serde_errors() {
            let deserialization = matches!(
                error.classify(),
                serde_json::error::Category::Data | serde_json::error::Category::Syntax
            );
            let original = error.to_string();
            let mapped = PersistenceError::from(error);
            assert_eq!(mapped.to_string(), original);
            match mapped {
                PersistenceError::Deserialization(e) if deserialization => assert!(e.is::<serde_json::Error>()),
                PersistenceError::Unexpected(e) if !deserialization => assert!(e.is::<serde_json::Error>()),
                _ => panic!("incorrect serde category"),
            }
        }
    }
}
