//! An event store that is never reachable.
use crate::{
    command::AggregateError,
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::EventStore,
    task::NewTask,
    testing::mem_store::MemStoreAggregateContext,
};
use std::{collections::HashMap, error::Error};

/// Every call fails with [`AggregateError::DatabaseConnectionError`], which the command boundary
/// reports as [`Unavailable`](crate::command::CommandGatewayError::Unavailable). Drives the
/// "storage unavailable" branches of a use case and proves that a path never touches storage.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnavailableStore;

fn unavailable<E: Error>() -> AggregateError<E> {
    AggregateError::DatabaseConnectionError("the event store is unavailable".into())
}

impl<A: Aggregate> EventStore<A> for UnavailableStore {
    type Context = MemStoreAggregateContext<A>;

    async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>> {
        Err(unavailable())
    }

    async fn load_aggregate(&self, _: &str) -> Result<Self::Context, AggregateError<A::Error>> {
        Err(unavailable())
    }

    async fn commit(
        &self,
        _: Vec<A::Event>,
        _: Self::Context,
        _: HashMap<String, String>,
        _: &[NewTask],
    ) -> Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>> {
        Err(unavailable())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::persisted::tests::TestAggregate;

    #[tokio::test]
    async fn every_call_fails_as_a_connection_error() {
        let store = UnavailableStore;
        assert!(matches!(
            EventStore::<TestAggregate>::load_events(&store, "id").await,
            Err(AggregateError::DatabaseConnectionError(error)) if error.to_string() == "the event store is unavailable"
        ));
        assert!(matches!(
            EventStore::<TestAggregate>::load_aggregate(&store, "id").await,
            Err(AggregateError::DatabaseConnectionError(_))
        ));
        let context = MemStoreAggregateContext {
            aggregate_id: "id".into(),
            aggregate: TestAggregate::default(),
            current_sequence: 0,
        };
        assert!(matches!(
            store.commit(vec![], context, HashMap::new(), &[]).await,
            Err(AggregateError::DatabaseConnectionError(_))
        ));
    }
}
