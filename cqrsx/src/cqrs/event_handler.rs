//! Reactions to committed domain events and replay of saved history.
pub mod replay;

// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::{aggregate::Aggregate, event::EventEnvelope, store::PersistenceError};
use async_trait::async_trait;

/// Reacts to events after they have been committed to the event store.
///
/// A handler may update a read-model projection or perform another event reaction.
/// Invocation is outside the event transaction and has no durable redelivery guarantee.
/// Use transactional outbox tasks when a reaction requires durable delivery.
#[async_trait]
pub trait EventHandler<A: Aggregate>: Send + Sync {
    /// Dispatch committed events. An error does not roll back the event commit.
    /// Framework dispatches every registered event handler and reports failures separately.
    async fn handle(&self, aggregate_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError>;
}
