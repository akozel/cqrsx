//! An event store that is never reachable.
use crate::mem_store::MemStoreAggregateContext;
use cqrsx::{
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::{EventStore, PersistenceError},
    task::NewTask,
};
use std::collections::HashMap;

/// Every call fails with [`PersistenceError::ConnectionError`], which the command boundary
/// reports as [`Unavailable`](cqrsx::command::CommandGatewayError::Unavailable). Drives the
/// "storage unavailable" branches of a use case and proves that a path never touches storage.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnavailableStore;

fn unavailable() -> PersistenceError {
    PersistenceError::ConnectionError("the event store is unavailable".into())
}

impl<A: Aggregate> EventStore<A> for UnavailableStore {
    type Context = MemStoreAggregateContext<A>;

    async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<A>>, PersistenceError> {
        Err(unavailable())
    }

    async fn load_aggregate(&self, _: &str) -> Result<Self::Context, PersistenceError> {
        Err(unavailable())
    }

    async fn commit(
        &self,
        _: Vec<A::Event>,
        _: Self::Context,
        _: HashMap<String, String>,
        _: &[NewTask],
    ) -> Result<Vec<EventEnvelope<A>>, PersistenceError> {
        Err(unavailable())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::Ledger as TestAggregate;

    #[tokio::test]
    async fn every_call_fails_as_a_connection_error() {
        let store = UnavailableStore;
        assert!(matches!(
            EventStore::<TestAggregate>::load_events(&store, "id").await,
            Err(PersistenceError::ConnectionError(error)) if error.to_string() == "the event store is unavailable"
        ));
        assert!(matches!(
            EventStore::<TestAggregate>::load_aggregate(&store, "id").await,
            Err(PersistenceError::ConnectionError(_))
        ));
        let context = MemStoreAggregateContext {
            aggregate_id: "id".into(),
            aggregate: TestAggregate::default(),
            current_sequence: 0,
        };
        assert!(matches!(
            store.commit(vec![], context, HashMap::new(), &[]).await,
            Err(PersistenceError::ConnectionError(_))
        ));
    }
}
