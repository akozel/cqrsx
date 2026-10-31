//! Event-store contracts, persistence errors and repository-backed implementations.
pub mod feed;
pub mod persisted;
pub mod repository;
pub mod serialized_event;
pub mod snapshot;
pub mod upcaster;
pub mod wakeup;

// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::{
    command::AggregateError,
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
    /// This is used by the [CqrsFramework](crate::command::framework::CqrsFramework) when loading
    /// an aggregate in order to handle incoming commands.
    type Context: AggregateContext<A>;

    /// Load all events for a particular `aggregate_id`
    fn load_events(
        &self,
        aggregate_id: &str,
    ) -> impl Future<Output = Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>>> + Send;
    /// Load aggregate at current state
    fn load_aggregate(&self, aggregate_id: &str) -> impl Future<Output = Result<Self::Context, AggregateError<A::Error>>> + Send;
    /// Commit new events and the tasks planned from them using the last committed aggregate context.
    /// The tasks commit atomically with the events or the commit fails before writing anything; a task
    /// never commits without an event. A store without an outbox refuses non-empty `tasks`.
    fn commit(
        &self,
        events: Vec<A::Event>,
        context: Self::Context,
        metadata: HashMap<String, String>,
        tasks: &[NewTask],
    ) -> impl Future<Output = Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>>> + Send;
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
        .ok_or_else(|| crate::journal::PersistenceError::UnknownError("event sequence or snapshot version overflow".into()))
}

/// Errors for implementations of a persistent event store.
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    /// Optimistic locking conflict occurred while committing and aggregate.
    #[error("optimistic lock error")]
    OptimisticLockError,
    /// An error occurred connecting to the database.
    #[error("{0}")]
    ConnectionError(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Error occurred while attempting to deserialize data.
    #[error("{0}")]
    DeserializationError(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    /// An unexpected error occurred while accessing the database.
    #[error("{0}")]
    UnknownError(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
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
            serde_json::error::Category::Data | serde_json::error::Category::Syntax => Self::DeserializationError(Box::new(err)),
            serde_json::error::Category::Io | serde_json::error::Category::Eof => Self::UnknownError(Box::new(err)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::io;
    #[test]
    fn persistence_categories_preserve_boxed_cause() {
        let cause = || Box::new(io::Error::other("original cause")) as Box<dyn std::error::Error + Send + Sync>;
        assert!(matches!(
            AggregateError::<Infallible>::from(PersistenceError::OptimisticLockError),
            AggregateError::AggregateConflict
        ));
        let cases = [
            PersistenceError::ConnectionError(cause()),
            PersistenceError::DeserializationError(cause()),
            PersistenceError::UnknownError(cause()),
        ];
        for (index, error) in cases.into_iter().enumerate() {
            let error = AggregateError::<Infallible>::from(error);
            let boxed = match (index, error) {
                (0, AggregateError::DatabaseConnectionError(e))
                | (1, AggregateError::DeserializationError(e))
                | (2, AggregateError::UnexpectedError(e)) => e,
                _ => panic!("technical categories must not become conflicts"),
            };
            assert_eq!(boxed.downcast_ref::<io::Error>().unwrap().to_string(), "original cause");
        }
        let AggregateError::UnexpectedError(boxed) =
            AggregateError::<Infallible>::from(PersistenceError::SequenceGap { expected: 2, found: 5 })
        else {
            panic!("a sequence gap is a technical category")
        };
        assert_eq!(boxed.to_string(), "sequence gap: expected event 2, found 5");
        assert!(matches!(
            boxed.downcast_ref::<PersistenceError>(),
            Some(PersistenceError::SequenceGap { expected: 2, found: 5 })
        ));
    }
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
                PersistenceError::DeserializationError(e) if deserialization => assert!(e.is::<serde_json::Error>()),
                PersistenceError::UnknownError(e) if !deserialization => assert!(e.is::<serde_json::Error>()),
                _ => panic!("incorrect serde category"),
            }
        }
        for error in serde_errors() {
            let deserialization = matches!(
                error.classify(),
                serde_json::error::Category::Data | serde_json::error::Category::Syntax
            );
            let original = error.to_string();
            let mapped = AggregateError::<Infallible>::from(error);
            assert_eq!(mapped.to_string(), original);
            assert!(
                matches!(mapped, AggregateError::DeserializationError(_) if deserialization)
                    || matches!(mapped, AggregateError::UnexpectedError(_) if !deserialization)
            );
        }
    }
}
