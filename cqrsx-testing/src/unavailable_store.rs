//! An aggregate store that is never reachable.
use cqrsx::{
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::{AggregateStore, PersistenceError, store::CommittedAggregate},
    task::NewTask,
};
use std::collections::HashMap;

/// Every call fails with [`PersistenceError::Connection`], which the command boundary
/// reports as [`Unavailable`](cqrsx::command::CommandError::Unavailable). Drives the
/// "storage unavailable" branches of a use case and proves that a path never touches storage.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnavailableStore;

fn unavailable() -> PersistenceError {
    PersistenceError::Connection("the event store is unavailable".into())
}

impl<A: Aggregate> AggregateStore<A> for UnavailableStore {
    type Context = CommittedAggregate<A>;

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
            AggregateStore::<TestAggregate>::load_events(&store, "id").await,
            Err(PersistenceError::Connection(error)) if error.to_string() == "the event store is unavailable"
        ));
        assert!(matches!(
            AggregateStore::<TestAggregate>::load_aggregate(&store, "id").await,
            Err(PersistenceError::Connection(_))
        ));
        let context = CommittedAggregate {
            aggregate_id: "id".into(),
            aggregate: TestAggregate::default(),
            last_sequence: 0,
            snapshot_revision: None,
        };
        assert!(matches!(
            store.commit(vec![], context, HashMap::new(), &[]).await,
            Err(PersistenceError::Connection(_))
        ));
    }
}
