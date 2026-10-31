//! What the projection processor drives: the [`EventHandler`] it feeds from the journal, the
//! [`RebuildableProjection`] it can rebuild, and the submodules with the checkpoint of a projection, the
//! [`processor`] itself and its [`runtime`].
pub mod checkpoint;
pub mod processor;
pub mod runtime;

// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and cqrsx/licenses/.
use crate::{
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::PersistenceError,
};
use async_trait::async_trait;
use std::sync::Arc;

/// Reacts to events after they have been committed to the event store.
///
/// A projection is the handler a projection processor calls: the processor reads the journal and hands it
/// the events of one aggregate key at a time, in journal order, possibly the events of several commands in
/// one call. Invocation is outside the event transaction; the processor's checkpoint makes it durable. Use
/// transactional outbox tasks for any other reaction that must happen.
///
/// A handler never executes a command, not even from a task it spawns: it writes its read model and
/// nothing else, and a follow-up started there would be recorded nowhere, repeated by every replay of the
/// journal and lost by a crash. Plan the follow-up as an outbox task from the command's decision instead
/// ([`TaskPlanner`](crate::command::planner::TaskPlanner)); the framework does not detect the mistake
/// (see the naming and integration rules in `doc/components.md`).
#[async_trait]
pub trait EventHandler<A: Aggregate>: Send + Sync {
    /// Identifies the handler in the processor's failure reports; defaults to the type name. Decorators
    /// should report the name of the handler they wrap.
    fn name(&self) -> String {
        std::any::type_name::<Self>().to_owned()
    }

    /// Handle committed events. An error does not roll back the event commit: the processor retries the
    /// page.
    async fn handle(&self, aggregate_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError>;
}

/// A shared handler: `Arc<H>` handles like `H`, so one instance can serve a decorator and a processor alike.
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

/// A projection that can replace a document with the fold of its aggregate's whole history: what a
/// projection processor runs for one key on a rebuild of the journal, on a request for that key, and for a
/// document that refused a live event as a gap.
#[async_trait]
pub trait RebuildableProjection<A: Aggregate>: EventHandler<A> {
    /// Replaces the document of `aggregate_id` with the fold of `history[..prefix]`. `history` is the
    /// aggregate's complete history from its first event, as it was read; `0 < prefix <= history.len()`:
    /// the whole history for a rebuild in one write, `1` for a paced rebuild that then applies the rest
    /// event by event through `handle`. A document that is ahead of `history` (its `last_sequence` is above
    /// the last event) is newer than the read and is refused with `OptimisticLockError`, so that a reader
    /// whose history is stale never rolls a document back: the caller re-reads the history. An empty
    /// history writes nothing.
    async fn rebuild(&self, aggregate_id: &str, history: &[EventEnvelope<A>], prefix: usize) -> Result<(), PersistenceError>;
}

/// A shared projection rebuilds like the one instance, as it handles like it.
#[async_trait]
impl<A, P> RebuildableProjection<A> for Arc<P>
where
    A: Aggregate,
    P: RebuildableProjection<A> + ?Sized,
{
    async fn rebuild(&self, aggregate_id: &str, history: &[EventEnvelope<A>], prefix: usize) -> Result<(), PersistenceError> {
        (**self).rebuild(aggregate_id, history, prefix).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::persisted::tests::TestAggregate;

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
