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
    /// Identifies the handler in post-commit failures
    /// ([`FailedHandler`](crate::cqrs::command::FailedHandler)); defaults to the type name.
    /// Decorators should report the name of the handler they wrap.
    fn name(&self) -> String {
        std::any::type_name::<Self>().to_owned()
    }

    /// Dispatch committed events. An error does not roll back the event commit.
    /// Framework dispatches every registered event handler and reports failures separately.
    async fn handle(&self, aggregate_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cqrs::store::persisted::tests::TestAggregate;

    struct Unnamed;

    #[async_trait]
    impl EventHandler<TestAggregate> for Unnamed {
        async fn handle(&self, _: &str, _: &[EventEnvelope<TestAggregate>]) -> Result<(), PersistenceError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn the_default_name_is_the_concrete_type_name_also_behind_a_box() {
        let boxed: Box<dyn EventHandler<TestAggregate>> = Box::new(Unnamed);
        boxed.handle("id", &[]).await.unwrap();
        assert_eq!(Unnamed.name(), std::any::type_name::<Unnamed>());
        assert_eq!(boxed.name(), std::any::type_name::<Unnamed>());
        assert!(boxed.name().ends_with("::Unnamed"));
    }
}
