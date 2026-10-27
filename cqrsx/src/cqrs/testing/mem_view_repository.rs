//! An in-memory view repository with the version rules of the PostgreSQL one.
use crate::cqrs::{
    aggregate::Aggregate,
    projection::{
        View,
        repository::{StoredView, ViewContext, ViewRepository},
    },
    store::PersistenceError,
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    marker::PhantomData,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

/// One stored document: its optimistic version, the last applied event of its aggregate and the view as JSON.
#[derive(Clone, Debug)]
struct Document {
    version: i64,
    last_sequence: usize,
    payload: Value,
}

/// Views stored as JSON documents with optimistic versions, like
/// `postgres::view_repository::PostgresViewRepository`:
/// version 0 inserts and conflicts with an existing document, a later version updates only the
/// document stored with exactly that version, and every write stores the next version together with
/// the document's `last_sequence`. Conflicts are [`PersistenceError::OptimisticLockError`]. Because
/// documents are JSON, `V` needs no `Clone` and serde failures surface as in a real store. The queries
/// order the documents by the bytes of their ids, as the PostgreSQL and MongoDB repositories do.
///
/// Clones share the documents, so a test can seed (`put`), wipe (`clear`) or query the repository a projection uses.
pub struct MemViewRepository<V, A> {
    documents: Arc<Mutex<BTreeMap<String, Document>>>,
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
    /// Store `view` as `view_id` at `version` with `last_sequence` applied, replacing any document; for
    /// seeding tests.
    pub fn put(&self, view_id: &str, version: i64, last_sequence: usize, view: &V) -> Result<(), PersistenceError> {
        let payload = serde_json::to_value(view)?;
        self.documents().insert(
            view_id.to_owned(),
            Document {
                version,
                last_sequence,
                payload,
            },
        );
        Ok(())
    }

    /// Drop every document.
    pub fn clear(&self) {
        self.documents().clear();
    }

    fn documents(&self) -> MutexGuard<'_, BTreeMap<String, Document>> {
        self.documents.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The documents whose id `within` accepts, in the order of the ids, decoded.
    fn stored(&self, within: impl Fn(&str) -> bool) -> Result<Vec<StoredView<V>>, PersistenceError> {
        self.documents()
            .iter()
            .filter(|(id, _)| within(id))
            .map(|(id, document)| {
                Ok(StoredView {
                    id: id.clone(),
                    version: document.version,
                    last_sequence: document.last_sequence,
                    view: V::deserialize(&document.payload)?,
                })
            })
            .collect()
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
        let Some(document) = self.documents().get(view_id).cloned() else {
            return Ok(None);
        };
        Ok(Some((
            V::deserialize(document.payload)?,
            ViewContext::new(view_id.to_owned(), document.version, document.last_sequence),
        )))
    }

    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        let version = context
            .version
            .checked_add(1)
            .ok_or_else(|| PersistenceError::UnknownError("view version overflow".into()))?;
        let payload = serde_json::to_value(&view)?;
        let mut documents = self.documents();
        let stored = documents.get(&context.view_instance_id).map(|document| document.version);
        match (context.version, stored) {
            (0, None) => {}
            (expected, Some(stored)) if expected != 0 && expected == stored => {}
            _ => return Err(PersistenceError::OptimisticLockError),
        }
        documents.insert(
            context.view_instance_id,
            Document {
                version,
                last_sequence: context.last_sequence,
                payload,
            },
        );
        Ok(())
    }

    async fn list(&self) -> Result<Vec<StoredView<V>>, PersistenceError> {
        self.stored(|_| true)
    }

    async fn range(&self, lower: &str, upper: &str) -> Result<Vec<StoredView<V>>, PersistenceError> {
        self.stored(|id| lower <= id && id < upper)
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
        const NAME: &'static str = "sequences";
        fn update(&mut self, event: &EventEnvelope<TestAggregate>) {
            self.0.push(event.sequence);
        }
    }

    type Repository = MemViewRepository<Sequences, TestAggregate>;

    async fn listed(repository: &Repository) -> Vec<(String, i64, usize, Sequences)> {
        repository
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|stored| (stored.id, stored.version, stored.last_sequence, stored.view))
            .collect()
    }

    #[tokio::test]
    async fn inserts_at_version_zero_and_updates_only_the_stored_version() {
        let repository = Repository::default();
        assert_eq!(repository.load("a").await.unwrap(), None);
        repository
            .update_view(Sequences(vec![1]), ViewContext::new("a".into(), 0, 1))
            .await
            .unwrap();
        let (view, context) = repository.load_with_context("a").await.unwrap().unwrap();
        assert_eq!((view, context), (Sequences(vec![1]), ViewContext::new("a".into(), 1, 1)));
        for stale in [0, 2] {
            assert!(matches!(
                repository
                    .update_view(Sequences(vec![9]), ViewContext::new("a".into(), stale, 9))
                    .await,
                Err(PersistenceError::OptimisticLockError)
            ));
        }
        assert!(matches!(
            repository
                .update_view(Sequences(vec![9]), ViewContext::new("b".into(), 1, 9))
                .await,
            Err(PersistenceError::OptimisticLockError)
        ));
        repository
            .update_view(Sequences(vec![1, 2]), ViewContext::new("a".into(), 1, 2))
            .await
            .unwrap();
        assert_eq!(repository.load("a").await.unwrap(), Some(Sequences(vec![1, 2])));
        assert_eq!(listed(&repository).await, [("a".into(), 2, 2, Sequences(vec![1, 2]))]);
    }

    #[tokio::test]
    async fn seeds_lists_by_id_and_clears_through_shared_clones() {
        let repository = Repository::default();
        let seen_by_projection = repository.clone();
        repository.put("b", 4, 2, &Sequences(vec![2])).unwrap();
        repository.put("a", 7, 1, &Sequences(vec![1])).unwrap();
        assert_eq!(
            listed(&seen_by_projection).await,
            [("a".into(), 7, 1, Sequences(vec![1])), ("b".into(), 4, 2, Sequences(vec![2]))]
        );
        let projection = ViewProjection::new(Arc::new(seen_by_projection));
        let event = EventEnvelope {
            aggregate_id: "b".into(),
            sequence: 3,
            payload: TestEvents::SomethingWasDone,
            metadata: Default::default(),
        };
        projection.handle("b", &[event]).await.unwrap();
        let (view, context) = repository.load_with_context("b").await.unwrap().unwrap();
        assert_eq!((view, context), (Sequences(vec![2, 3]), ViewContext::new("b".into(), 5, 3)));
        repository.clear();
        assert!(repository.list().await.unwrap().is_empty());
    }

    /// The ids of every repository's ordering test: in the order of their bytes (`F` < `f`; `-` < `:` < `;` < `_` < `y`).
    const IDS: [&str; 8] = [
        "Fox:muse-2027-01-16",
        "fox-2:radiohead-2026-11-21",
        "fox:muse-2027-01-16",
        "fox:radiohead-2026-11-21",
        "fox;",
        "fox_1:c-1",
        "foxy:c-1",
        "panda:c-1",
    ];

    #[tokio::test]
    async fn lists_every_document_and_a_range_of_ids_in_the_byte_order_of_the_ids() {
        let repository = Repository::default();
        assert!(repository.list().await.unwrap().is_empty());
        assert!(repository.range("fox:", "fox;").await.unwrap().is_empty());
        for (n, id) in IDS.iter().rev().enumerate() {
            repository.put(id, n as i64 + 1, n, &Sequences((1..=n).collect())).unwrap();
        }
        let ids = |stored: Vec<StoredView<Sequences>>| stored.into_iter().map(|stored| stored.id).collect::<Vec<_>>();
        assert_eq!(ids(repository.list().await.unwrap()), IDS);
        assert_eq!(
            ids(repository.range("fox:", "fox;").await.unwrap()),
            ["fox:muse-2027-01-16", "fox:radiohead-2026-11-21"]
        );
        // The lower bound is in, the upper one out; an empty or inverted range holds nothing.
        assert_eq!(
            ids(repository.range("fox;", "foxy:c-1").await.unwrap()),
            ["fox;", "fox_1:c-1"]
        );
        for (lower, upper) in [("fox;", "fox:"), ("fox;", "fox;"), ("zebra", "a")] {
            assert!(repository.range(lower, upper).await.unwrap().is_empty(), "{lower}..{upper}");
        }
        let stored = repository.range("fox;", "fox_").await.unwrap();
        assert_eq!(
            stored,
            [StoredView {
                id: "fox;".into(),
                version: 4,
                last_sequence: 3,
                view: Sequences(vec![1, 2, 3]),
            }]
        );
    }

    #[tokio::test]
    async fn version_overflow_and_serde_failures_are_errors() {
        let repository = Repository::default();
        repository.put("max", i64::MAX, 0, &Sequences(vec![])).unwrap();
        assert!(matches!(
            repository.update_view(Sequences(vec![]), ViewContext::new("max".into(), i64::MAX, 1)).await,
            Err(PersistenceError::UnknownError(error)) if error.to_string() == "view version overflow"
        ));
        repository.documents().insert(
            "broken".into(),
            Document {
                version: 1,
                last_sequence: 1,
                payload: serde_json::json!("not a list"),
            },
        );
        assert!(matches!(
            repository.load("broken").await,
            Err(PersistenceError::DeserializationError(_))
        ));
        assert!(matches!(
            repository.list().await,
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
            const NAME: &'static str = "unserializable";
            fn update(&mut self, _: &EventEnvelope<TestAggregate>) {}
        }
        let repository = MemViewRepository::<Unserializable, TestAggregate>::default();
        assert!(repository.put("a", 1, 0, &Unserializable).is_err());
        assert!(
            repository
                .update_view(Unserializable, ViewContext::new("a".into(), 0, 1))
                .await
                .is_err()
        );
        assert!(repository.list().await.unwrap().is_empty());
    }
}
