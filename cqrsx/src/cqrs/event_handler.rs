//! Reactions to committed domain events and replay of saved history.
pub mod replay;

// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::{aggregate::Aggregate, event::EventEnvelope, store::PersistenceError};
use async_trait::async_trait;
use std::sync::Arc;

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

/// A shared handler, e.g. one instance registered with every per-command framework.
/// Name and handling are those of the shared handler.
#[async_trait]
impl<A, H> EventHandler<A> for Arc<H>
where
    A: Aggregate,
    H: EventHandler<A> + ?Sized,
{
    fn name(&self) -> String {
        (**self).name()
    }

    async fn handle(&self, aggregate_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError> {
        (**self).handle(aggregate_id, events).await
    }
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

    /// Counts calls and fails on the second one.
    #[derive(Default)]
    struct Counting(std::sync::atomic::AtomicUsize);

    #[async_trait]
    impl EventHandler<TestAggregate> for Counting {
        fn name(&self) -> String {
            "counting".into()
        }

        async fn handle(&self, _: &str, _: &[EventEnvelope<TestAggregate>]) -> Result<(), PersistenceError> {
            match self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                0 => Ok(()),
                _ => Err(PersistenceError::OptimisticLockError),
            }
        }
    }

    #[tokio::test]
    async fn a_shared_handler_delegates_name_and_handling_to_the_one_instance() {
        let shared = Arc::new(Counting::default());
        let erased: Arc<dyn EventHandler<TestAggregate>> = shared.clone();
        let boxed: Vec<Box<dyn EventHandler<TestAggregate>>> = vec![Box::new(erased.clone()), Box::new(shared.clone())];
        assert_eq!(boxed.iter().map(|h| h.name()).collect::<Vec<_>>(), ["counting", "counting"]);
        boxed[0].handle("id", &[]).await.unwrap();
        assert!(matches!(
            boxed[1].handle("id", &[]).await,
            Err(PersistenceError::OptimisticLockError)
        ));
        assert_eq!(shared.0.load(std::sync::atomic::Ordering::SeqCst), 2);
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
