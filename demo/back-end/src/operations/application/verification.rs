//! Verify (D-20): every document of a read model against the fold of the part of its aggregate's history the
//! document has applied. Under eventual consistency a document behind the journal is no error: it is counted
//! apart, with the events it still has to take. Only a document that is not the fold of its own prefix, is
//! ahead of its history or has no history at all is a difference.
//!
//! The documents are read first, the histories after them: a document only ever holds committed events, so a
//! history read later is at least as long, and a command committed in between shows as behind, never as a
//! false difference. Verify needs no lock and may run during Encore!: a document being rebuilt is a correct
//! prefix of its history.
use crate::shared::application::{
    StorageError,
    read_model::{ReadModelStore, Stored},
};
use async_trait::async_trait;
use cqrsx::cqrs::{aggregate::Aggregate, event::EventEnvelope, projection::View, store::repository::PersistedEventRepository};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    sync::Arc,
};

/// Keys of the journal read per page.
const KEY_PAGE: usize = 100;

/// A document that is wrong, not merely behind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Difference {
    pub collection: &'static str,
    pub id: String,
    /// `differs` (not the fold of its prefix, or ahead of its history) or `unexpected` (no history).
    pub kind: &'static str,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Verification {
    /// Keys of the journal checked.
    pub checked: usize,
    /// Documents behind their history, missing ones included, and the events they still have to apply.
    pub behind_documents: usize,
    pub behind_events: usize,
    pub differences: Vec<Difference>,
}

/// What verify needs from every read model, whatever its view type.
#[async_trait]
pub trait VerifiedReadModel: Send + Sync {
    /// The collection name the API reports.
    fn name(&self) -> &'static str;
    async fn verify(&self) -> Result<Verification, StorageError>;
}

/// [`VerifiedReadModel`] for a view `V` of aggregate `A` in documents `R`, against the journal `J`.
pub struct ProjectionVerification<J, R, V, A> {
    name: &'static str,
    journal: Arc<J>,
    documents: Arc<R>,
    phantom: PhantomData<fn() -> (V, A)>,
}

impl<J, R, V, A> ProjectionVerification<J, R, V, A> {
    pub fn new(name: &'static str, journal: Arc<J>, documents: Arc<R>) -> Self {
        Self {
            name,
            journal,
            documents,
            phantom: PhantomData,
        }
    }
}

enum Comparison {
    Same,
    Behind(usize),
    Differs,
}

fn compare<V: View<A> + PartialEq, A: Aggregate>(document: &Stored<V>, history: &[EventEnvelope<A>]) -> Comparison {
    let Some(applied) = history.get(..document.last_sequence) else {
        return Comparison::Differs;
    };
    let mut fold = V::default();
    for event in applied {
        fold.update(event);
    }
    if fold != document.view {
        Comparison::Differs
    } else if applied.len() < history.len() {
        Comparison::Behind(history.len() - applied.len())
    } else {
        Comparison::Same
    }
}

#[async_trait]
impl<J, R, V, A> VerifiedReadModel for ProjectionVerification<J, R, V, A>
where
    J: PersistedEventRepository + 'static,
    R: ReadModelStore<V> + 'static,
    V: View<A> + PartialEq + 'static,
    A: Aggregate + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    async fn verify(&self) -> Result<Verification, StorageError> {
        let documents: BTreeMap<String, Stored<V>> = self
            .documents
            .list()
            .await?
            .into_iter()
            .map(|document| (document.id.clone(), document))
            .collect();
        let mut result = Verification::default();
        let mut keys = BTreeSet::new();
        let mut after = String::new();
        loop {
            let page = self
                .journal
                .aggregate_ids_after::<A>(&after, KEY_PAGE)
                .await
                .map_err(StorageError::new)?;
            let Some(last) = page.last().cloned() else {
                break;
            };
            for key in page {
                let history = self
                    .journal
                    .get_events::<A>(&key)
                    .await
                    .map_err(StorageError::new)?
                    .into_iter()
                    .map(EventEnvelope::<A>::try_from)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(StorageError::new)?;
                result.checked += 1;
                let behind = match documents.get(&key).map(|document| compare(document, &history)) {
                    None => history.len(),
                    Some(Comparison::Same) => 0,
                    Some(Comparison::Behind(events)) => events,
                    Some(Comparison::Differs) => {
                        result.differences.push(self.difference(&key, "differs"));
                        0
                    }
                };
                if behind > 0 {
                    result.behind_documents += 1;
                    result.behind_events += behind;
                }
                keys.insert(key);
            }
            after = last;
        }
        for id in documents.keys().filter(|id| !keys.contains(*id)) {
            result.differences.push(self.difference(id, "unexpected"));
        }
        Ok(result)
    }
}

impl<J, R, V, A> ProjectionVerification<J, R, V, A> {
    fn difference(&self, id: &str, kind: &'static str) -> Difference {
        Difference {
            collection: self.name,
            id: id.to_owned(),
            kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::cqrs::{
        event::DomainEvent,
        event_sink::EventSink,
        store::serialized_event::SerializedEvent,
        testing::{mem_event_repository::MemEventRepository, mem_view_repository::MemViewRepository},
    };
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Added(i64);

    impl DomainEvent for Added {
        fn event_type(&self) -> String {
            "added".into()
        }
        fn event_version(&self) -> String {
            "1".into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("never")]
    struct Never;

    #[derive(Debug, Default, Clone, Serialize, Deserialize)]
    struct Counter;

    impl Aggregate for Counter {
        const TYPE: &'static str = "counter";
        type Command = ();
        type Event = Added;
        type Error = Never;
        type Services = ();
        async fn handle(&mut self, _: (), _: &(), _: &EventSink<Self>) -> Result<(), Never> {
            Ok(())
        }
        fn apply(&mut self, _: Added) {}
    }

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    struct Total {
        sum: i64,
    }

    impl View<Counter> for Total {
        fn update(&mut self, event: &EventEnvelope<Counter>) {
            self.sum += event.payload.0;
        }
    }

    fn event(id: &str, sequence: usize) -> SerializedEvent {
        SerializedEvent::try_from(&EventEnvelope::<Counter> {
            aggregate_id: id.into(),
            sequence,
            payload: Added(sequence as i64),
            metadata: Default::default(),
        })
        .unwrap()
    }

    /// The fold of events `1..=n` of `id`: 1 + 2 + … + n.
    fn total(n: usize) -> Total {
        Total {
            sum: (1..=n as i64).sum(),
        }
    }

    async fn journal(histories: &[(&str, usize)]) -> Arc<MemEventRepository> {
        let journal = MemEventRepository::default();
        for (id, events) in histories {
            for sequence in 1..=*events {
                journal.persist::<Counter>(&[event(id, sequence)], None).await.unwrap();
            }
        }
        Arc::new(journal)
    }

    fn documents(documents: &[(&str, usize, Total)]) -> Arc<MemViewRepository<Total, Counter>> {
        let repository = MemViewRepository::default();
        for (id, last_sequence, view) in documents {
            repository.put(id, 1, *last_sequence, view).unwrap();
        }
        Arc::new(repository)
    }

    async fn verify<R: ReadModelStore<Total> + 'static>(journal: Arc<MemEventRepository>, documents: Arc<R>) -> Verification {
        ProjectionVerification::<_, _, Total, Counter>::new("totals", journal, documents)
            .verify()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn documents_equal_to_the_fold_of_their_applied_prefix_pass_and_lagging_ones_are_counted_apart() {
        let journal = journal(&[("fresh", 3), ("lagging", 4), ("missing", 2)]).await;
        let documents = documents(&[("fresh", 3, total(3)), ("lagging", 1, total(1))]);
        let verification = verify(journal, documents).await;
        assert_eq!(
            verification,
            Verification {
                checked: 3,
                behind_documents: 2,
                behind_events: 3 + 2,
                differences: vec![]
            },
            "lagging: 3 events to go; missing: its whole history of 2"
        );
    }

    #[tokio::test]
    async fn a_wrong_document_one_ahead_of_its_history_and_one_without_history_are_differences() {
        let journal = journal(&[("wrong", 2), ("ahead", 2)]).await;
        let documents = documents(&[
            ("wrong", 2, Total { sum: 99 }),
            ("ahead", 3, total(3)),
            ("orphan", 1, total(1)),
        ]);
        let verification = verify(journal, documents).await;
        let kinds: Vec<(&str, &str)> = verification
            .differences
            .iter()
            .map(|difference| (difference.id.as_str(), difference.kind))
            .collect();
        assert_eq!(kinds, [("ahead", "differs"), ("wrong", "differs"), ("orphan", "unexpected")]);
        assert_eq!((verification.checked, verification.behind_documents), (2, 0));
        assert!(
            verification
                .differences
                .iter()
                .all(|difference| difference.collection == "totals")
        );
    }

    #[tokio::test]
    async fn an_empty_journal_and_an_empty_collection_verify_nothing() {
        let verification = verify(journal(&[]).await, documents(&[])).await;
        assert_eq!(verification, Verification::default());
    }

    #[tokio::test]
    async fn the_journal_is_read_page_by_page() {
        let histories: Vec<(String, usize)> = (0..KEY_PAGE + 5).map(|n| (format!("k{n:04}"), 1)).collect();
        let refs: Vec<(&str, usize)> = histories.iter().map(|(id, n)| (id.as_str(), *n)).collect();
        let verification = verify(journal(&refs).await, documents(&[])).await;
        assert_eq!(
            (verification.checked, verification.behind_documents),
            (KEY_PAGE + 5, KEY_PAGE + 5)
        );
    }

    /// A collection whose listing is followed by one more commit of `id`, as a command landing between the
    /// read of the documents and the read of the histories.
    struct CommitAfterListing {
        documents: Arc<MemViewRepository<Total, Counter>>,
        journal: Arc<MemEventRepository>,
        id: &'static str,
        next: usize,
    }

    #[async_trait]
    impl ReadModelStore<Total> for CommitAfterListing {
        async fn load(&self, id: &str) -> Result<Option<Stored<Total>>, StorageError> {
            self.documents.load(id).await
        }
        async fn list(&self) -> Result<Vec<Stored<Total>>, StorageError> {
            let listed = self.documents.list().await;
            self.journal
                .persist::<Counter>(&[event(self.id, self.next)], None)
                .await
                .unwrap();
            listed
        }
        async fn list_range(&self, lower: &str, upper: &str) -> Result<Vec<Stored<Total>>, StorageError> {
            self.documents.list_range(lower, upper).await
        }
    }

    #[tokio::test]
    async fn a_command_committed_between_the_two_reads_shows_as_behind() {
        let journal = journal(&[("c-1", 2)]).await;
        let documents = Arc::new(CommitAfterListing {
            documents: documents(&[("c-1", 2, total(2))]),
            journal: journal.clone(),
            id: "c-1",
            next: 3,
        });
        let verification = verify(journal, documents).await;
        assert_eq!(
            (
                verification.behind_documents,
                verification.behind_events,
                verification.differences
            ),
            (1, 1, vec![])
        );
    }

    #[tokio::test]
    async fn a_failing_store_or_an_unreadable_history_is_a_storage_error() {
        let journal = journal(&[]).await;
        let broken = SerializedEvent::new(
            "c-1".into(),
            1,
            Counter::TYPE.into(),
            "added".into(),
            "1".into(),
            serde_json::json!("not a number"),
            serde_json::json!({}),
        );
        journal.persist::<Counter>(&[broken], None).await.unwrap();
        let error = ProjectionVerification::<_, _, Total, Counter>::new("totals", journal, documents(&[]))
            .verify()
            .await
            .unwrap_err();
        assert!(!error.0.is_empty());
    }
}
