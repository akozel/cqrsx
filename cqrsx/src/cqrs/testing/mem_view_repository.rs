//! An in-memory view repository with the version rules of the PostgreSQL one.
use crate::cqrs::{
    aggregate::Aggregate,
    projection::{
        View,
        repository::{ViewContext, ViewRepository},
    },
    store::PersistenceError,
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    marker::PhantomData,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

/// Views stored as JSON documents with optimistic versions, like
/// `postgres::view_repository::PostgresViewRepository`:
/// version 0 inserts and conflicts with an existing document, a later version updates only the
/// document stored with exactly that version, and every write stores the next version.
/// Conflicts are [`PersistenceError::OptimisticLockError`]. Because documents are JSON, `V`
/// needs no `Clone` and serde failures surface as in a real store.
///
/// Clones share the documents, so a test can seed or inspect the repository a projection uses.
pub struct MemViewRepository<V, A> {
    documents: Arc<Mutex<BTreeMap<String, (i64, Value)>>>,
    phantom: PhantomData<fn() -> (V, A)>,
}

impl<V, A> Default for MemViewRepository<V, A> {
    fn default() -> Self {
        Self {
            documents: Arc::default(),
            phantom: PhantomData,
        }
    }
}

impl<V, A> Clone for MemViewRepository<V, A> {
    fn clone(&self) -> Self {
        Self {
            documents: self.documents.clone(),
            phantom: PhantomData,
        }
    }
}

impl<V, A> MemViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    /// Store `view` as `view_id` at `version`, replacing any document; for seeding tests.
    pub fn put(&self, view_id: &str, version: i64, view: &V) -> Result<(), PersistenceError> {
        let payload = serde_json::to_value(view)?;
        self.documents().insert(view_id.to_owned(), (version, payload));
        Ok(())
    }

    /// Every stored view with its context, ordered by view ID.
    pub fn list_all(&self) -> Result<Vec<(ViewContext, V)>, PersistenceError> {
        self.documents()
            .iter()
            .map(|(id, (version, payload))| Ok((ViewContext::new(id.clone(), *version), V::deserialize(payload)?)))
            .collect()
    }

    /// Drop every document.
    pub fn clear(&self) {
        self.documents().clear();
    }

    fn documents(&self) -> MutexGuard<'_, BTreeMap<String, (i64, Value)>> {
        self.documents.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<V, A> ViewRepository<V, A> for MemViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError> {
        Ok(self.load_with_context(view_id).await?.map(|(view, _)| view))
    }

    async fn load_with_context(&self, view_id: &str) -> Result<Option<(V, ViewContext)>, PersistenceError> {
        let Some((version, payload)) = self.documents().get(view_id).cloned() else {
            return Ok(None);
        };
        Ok(Some((
            V::deserialize(payload)?,
            ViewContext::new(view_id.to_owned(), version),
        )))
    }

    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        let version = context
            .version
            .checked_add(1)
            .ok_or_else(|| PersistenceError::UnknownError("view version overflow".into()))?;
        let payload = serde_json::to_value(&view)?;
        let mut documents = self.documents();
        let stored = documents.get(&context.view_instance_id).map(|(version, _)| *version);
        match (context.version, stored) {
            (0, None) => {}
            (expected, Some(stored)) if expected != 0 && expected == stored => {}
            _ => return Err(PersistenceError::OptimisticLockError),
        }
        documents.insert(context.view_instance_id, (version, payload));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cqrs::event_handler::EventHandler;
    use crate::cqrs::{
        event::EventEnvelope,
        projection::ViewProjection,
        store::persisted::tests::{TestAggregate, TestEvents},
    };
    use serde::{Deserialize, Serialize, Serializer};

    #[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
    struct Sequences(Vec<usize>);

    impl View<TestAggregate> for Sequences {
        fn update(&mut self, event: &EventEnvelope<TestAggregate>) {
            self.0.push(event.sequence);
        }
    }

    type Repository = MemViewRepository<Sequences, TestAggregate>;

    fn ids_and_versions(repository: &Repository) -> Vec<(String, i64, Sequences)> {
        repository
            .list_all()
            .unwrap()
            .into_iter()
            .map(|(context, view)| (context.view_instance_id, context.version, view))
            .collect()
    }

    #[tokio::test]
    async fn inserts_at_version_zero_and_updates_only_the_stored_version() {
        let repository = Repository::default();
        assert_eq!(repository.load("a").await.unwrap(), None);
        repository
            .update_view(Sequences(vec![1]), ViewContext::new("a".into(), 0))
            .await
            .unwrap();
        let (view, context) = repository.load_with_context("a").await.unwrap().unwrap();
        assert_eq!((view, context.version), (Sequences(vec![1]), 1));
        for stale in [0, 2] {
            assert!(matches!(
                repository
                    .update_view(Sequences(vec![9]), ViewContext::new("a".into(), stale))
                    .await,
                Err(PersistenceError::OptimisticLockError)
            ));
        }
        assert!(matches!(
            repository
                .update_view(Sequences(vec![9]), ViewContext::new("b".into(), 1))
                .await,
            Err(PersistenceError::OptimisticLockError)
        ));
        repository
            .update_view(Sequences(vec![1, 2]), ViewContext::new("a".into(), 1))
            .await
            .unwrap();
        assert_eq!(repository.load("a").await.unwrap(), Some(Sequences(vec![1, 2])));
        assert_eq!(ids_and_versions(&repository), [("a".into(), 2, Sequences(vec![1, 2]))]);
    }

    #[tokio::test]
    async fn seeds_lists_by_id_and_clears_through_shared_clones() {
        let repository = Repository::default();
        let seen_by_projection = repository.clone();
        repository.put("b", 4, &Sequences(vec![2])).unwrap();
        repository.put("a", 7, &Sequences(vec![1])).unwrap();
        assert_eq!(
            ids_and_versions(&seen_by_projection),
            [("a".into(), 7, Sequences(vec![1])), ("b".into(), 4, Sequences(vec![2]))]
        );
        let projection = ViewProjection::new(Arc::new(seen_by_projection));
        let event = EventEnvelope {
            aggregate_id: "b".into(),
            sequence: 3,
            payload: TestEvents::SomethingWasDone,
            metadata: Default::default(),
        };
        projection.handle("b", &[event]).await.unwrap();
        assert_eq!(repository.load_with_context("b").await.unwrap().unwrap().1.version, 5);
        repository.clear();
        assert!(repository.list_all().unwrap().is_empty());
    }

    #[tokio::test]
    async fn version_overflow_and_serde_failures_are_errors() {
        let repository = Repository::default();
        repository.put("max", i64::MAX, &Sequences(vec![])).unwrap();
        assert!(matches!(
            repository.update_view(Sequences(vec![]), ViewContext::new("max".into(), i64::MAX)).await,
            Err(PersistenceError::UnknownError(error)) if error.to_string() == "view version overflow"
        ));
        repository
            .documents()
            .insert("broken".into(), (1, serde_json::json!("not a list")));
        assert!(matches!(
            repository.load("broken").await,
            Err(PersistenceError::DeserializationError(_))
        ));
        assert!(matches!(
            repository.list_all(),
            Err(PersistenceError::DeserializationError(_))
        ));

        #[derive(Debug, Default, Deserialize)]
        struct Unserializable;
        impl Serialize for Unserializable {
            fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("unserializable"))
            }
        }
        impl View<TestAggregate> for Unserializable {
            fn update(&mut self, _: &EventEnvelope<TestAggregate>) {}
        }
        let repository = MemViewRepository::<Unserializable, TestAggregate>::default();
        assert!(repository.put("a", 1, &Unserializable).is_err());
        assert!(
            repository
                .update_view(Unserializable, ViewContext::new("a".into(), 0))
                .await
                .is_err()
        );
        assert!(repository.list_all().unwrap().is_empty());
    }
}
