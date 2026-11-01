//! Verify (D-20): every document of a view against the fold of the part of its aggregate's history the document has
//! applied. Under eventual consistency a document behind the journal is no error: it is counted apart, with the events
//! it still has to take. Only a document that is not the fold of its own prefix, is ahead of its history or has no
//! history at all is a difference.
//!
//! The documents are read first, the histories after them: a document only ever holds committed events, so a
//! history read later is at least as long, and a command committed in between shows as behind, never as a
//! false difference. Verify needs no lock and may run during a rebuild: a document being rebuilt is a correct
//! prefix of its history.
//!
//! ```
//! # mod example {
//! # use cqrsx::{domain::{aggregate::Aggregate, command::Decide, event::{Events, EventEnvelope}}, view::View};
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
//! # pub struct Counted;
//! # impl Events for Counted {
//! #     fn event_type(&self) -> &'static str { "counted" }
//! #     fn event_version(&self) -> &'static str { "1" }
//! # }
//! # #[derive(Debug, thiserror::Error)]
//! # #[error("never")]
//! # pub struct Never;
//! # #[derive(Debug, Default, Clone, Serialize, Deserialize)]
//! # pub struct Counter;
//! # impl Aggregate for Counter {
//! #     const TYPE: &'static str = "counter";
//! #     type Id = String;
//! #     type Command = ();
//! #     type Event = Counted;
//! #     type Rejection = Never;
//! #     type Rules = ();
//! #     fn apply(&mut self, _: Counted) {}
//! # }
//! # impl Decide<Counter> for () {
//! #     fn decide(self, _: &Counter, _: &()) -> Result<Counted, Never> { Ok(Counted) }
//! # }
//! # #[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
//! # pub struct Total { pub count: usize }
//! # impl View<Counter> for Total {
//! #     const NAME: &'static str = "totals";
//! #     fn apply(&mut self, _: &EventEnvelope<Counter>) { self.count += 1; }
//! # }
//! # }
//! # use example::{Counted, Counter, Total};
//! # use cqrsx::domain::event::EventEnvelope;
//! use cqrsx::{
//!     journal::{repository::EventRepository, serialized_event::SerializedEvent},
//!     memory::{event_repository::MemoryEventRepository, view_repository::MemoryViewRepository},
//!     view::verify::verify,
//! };
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let event = |sequence| SerializedEvent::try_from(&EventEnvelope::<Counter> {
//! #     aggregate_id: "c-1".into(), sequence, payload: Counted, metadata: Default::default() }).unwrap();
//! let journal = MemoryEventRepository::default();
//! journal.persist::<Counter>(&[event(1), event(2)], None).await?;
//! let totals = MemoryViewRepository::<Total, Counter>::default();
//! totals.put("c-1", 1, 1, &Total { count: 1 })?; // one event applied, one to go
//!
//! let verification = verify::<Total, Counter>(&journal, &totals).await?;
//! assert_eq!((verification.checked, verification.behind_documents, verification.behind_events), (1, 1, 1));
//! assert!(verification.differences.is_empty(), "behind is no difference");
//! # Ok(()) }
//! # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(example()).unwrap();
//! ```
use crate::{
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::{PersistenceError, repository::EventRepository},
    view::{
        View,
        repository::{StoredView, ViewRepository},
    },
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// Keys of the journal read per page.
const KEY_PAGE: usize = 100;

/// A document that is wrong, not merely behind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Difference {
    /// The view's name, [`View::NAME`].
    pub view: &'static str,
    pub id: String,
    pub kind: DifferenceKind,
}

/// How a document is wrong; serialized in snake case (`differs`, `without_history`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DifferenceKind {
    /// Not the fold of a prefix of its history, or ahead of its history.
    Differs,
    /// A document without a history.
    WithoutHistory,
}

/// What verify found in the documents of one view.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Verification {
    /// Keys of the journal checked.
    pub checked: usize,
    /// Documents behind their history, missing ones included, and the events they still have to apply.
    pub behind_documents: usize,
    pub behind_events: usize,
    pub differences: Vec<Difference>,
}

enum Comparison {
    Same,
    Behind(usize),
    Differs,
}

fn compare<V: View<A> + PartialEq, A: Aggregate>(document: &StoredView<V>, history: &[EventEnvelope<A>]) -> Comparison {
    let Some(applied) = history.get(..document.last_sequence) else {
        return Comparison::Differs;
    };
    let mut fold = V::default();
    for event in applied {
        fold.apply(event);
    }
    if fold != document.view {
        Comparison::Differs
    } else if applied.len() < history.len() {
        Comparison::Behind(history.len() - applied.len())
    } else {
        Comparison::Same
    }
}

fn difference<V: View<A>, A: Aggregate>(id: &str, kind: DifferenceKind) -> Difference {
    Difference {
        view: V::NAME,
        id: id.to_owned(),
        kind,
    }
}

/// Verifies every document of the view `V` in `documents` against the journal of `A`: see the [module](self). The
/// documents are listed once, the journal is read key by key, a page of keys at a time; a failed read of either is
/// the error.
pub async fn verify<V, A>(
    journal: &impl EventRepository,
    documents: &impl ViewRepository<V, A>,
) -> Result<Verification, PersistenceError>
where
    V: View<A> + PartialEq,
    A: Aggregate,
{
    let documents: BTreeMap<String, StoredView<V>> = documents
        .list()
        .await?
        .into_iter()
        .map(|document| (document.id.clone(), document))
        .collect();
    let mut result = Verification::default();
    let mut keys = BTreeSet::new();
    let mut after = String::new();
    loop {
        let page = journal.aggregate_ids_after::<A>(&after, KEY_PAGE).await?;
        let Some(last) = page.last().cloned() else {
            break;
        };
        for key in page {
            let history = journal
                .events::<A>(&key)
                .await?
                .into_iter()
                .map(EventEnvelope::<A>::try_from)
                .collect::<Result<Vec<_>, _>>()?;
            result.checked += 1;
            let behind = match documents.get(&key).map(|document| compare(document, &history)) {
                None => history.len(),
                Some(Comparison::Same) => 0,
                Some(Comparison::Behind(events)) => events,
                Some(Comparison::Differs) => {
                    result.differences.push(difference::<V, A>(&key, DifferenceKind::Differs));
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
        result
            .differences
            .push(difference::<V, A>(id, DifferenceKind::WithoutHistory));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{command::Decide, event::Events},
        journal::serialized_event::SerializedEvent,
        memory::{event_repository::MemoryEventRepository, view_repository::MemoryViewRepository},
        view::repository::ViewMeta,
    };
    use serde::Deserialize;
    use std::sync::Arc;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Added(i64);

    impl Events for Added {
        fn event_type(&self) -> &'static str {
            "added"
        }
        fn event_version(&self) -> &'static str {
            "1"
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("never")]
    struct Never;

    #[derive(Debug, Default, Clone, Serialize, Deserialize)]
    struct Counter;

    impl Aggregate for Counter {
        const TYPE: &'static str = "counter";
        type Id = String;
        type Command = ();
        type Event = Added;
        type Rejection = Never;
        type Rules = ();
        fn apply(&mut self, _: Added) {}
    }

    impl Decide<Counter> for () {
        fn decide(self, _: &Counter, _: &()) -> Result<Added, Never> {
            Ok(Added(0))
        }
    }

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    struct Total {
        sum: i64,
    }

    impl View<Counter> for Total {
        const NAME: &'static str = "totals";
        fn apply(&mut self, event: &EventEnvelope<Counter>) {
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

    async fn journal(histories: &[(&str, usize)]) -> Arc<MemoryEventRepository> {
        let journal = MemoryEventRepository::default();
        for (id, events) in histories {
            for sequence in 1..=*events {
                journal.persist::<Counter>(&[event(id, sequence)], None).await.unwrap();
            }
        }
        Arc::new(journal)
    }

    fn documents(documents: &[(&str, usize, Total)]) -> Arc<MemoryViewRepository<Total, Counter>> {
        let repository = MemoryViewRepository::default();
        for (id, last_sequence, view) in documents {
            repository.put(id, 1, *last_sequence, view).unwrap();
        }
        Arc::new(repository)
    }

    async fn verify<R: ViewRepository<Total, Counter> + 'static>(
        journal: Arc<MemoryEventRepository>,
        documents: Arc<R>,
    ) -> Verification {
        super::verify::<Total, Counter>(&*journal, &*documents).await.unwrap()
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
        let kinds: Vec<(&str, DifferenceKind)> = verification
            .differences
            .iter()
            .map(|difference| (difference.id.as_str(), difference.kind))
            .collect();
        assert_eq!(
            kinds,
            [
                ("ahead", DifferenceKind::Differs),
                ("wrong", DifferenceKind::Differs),
                ("orphan", DifferenceKind::WithoutHistory)
            ]
        );
        assert_eq!(
            serde_json::to_value(&verification.differences).unwrap(),
            serde_json::json!([
                { "view": "totals", "id": "ahead", "kind": "differs" },
                { "view": "totals", "id": "wrong", "kind": "differs" },
                { "view": "totals", "id": "orphan", "kind": "without_history" }
            ])
        );
        assert_eq!((verification.checked, verification.behind_documents), (2, 0));
        assert!(verification.differences.iter().all(|difference| difference.view == "totals"));
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
        documents: Arc<MemoryViewRepository<Total, Counter>>,
        journal: Arc<MemoryEventRepository>,
        id: &'static str,
        next: usize,
    }

    impl ViewRepository<Total, Counter> for CommitAfterListing {
        async fn load(&self, id: &str) -> Result<Option<Total>, PersistenceError> {
            self.documents.load(id).await
        }
        async fn load_stored(&self, id: &str) -> Result<Option<StoredView<Total>>, PersistenceError> {
            self.documents.load_stored(id).await
        }
        async fn update_view(&self, view: Total, context: ViewMeta) -> Result<(), PersistenceError> {
            self.documents.update_view(view, context).await
        }
        async fn list(&self) -> Result<Vec<StoredView<Total>>, PersistenceError> {
            let listed = self.documents.list().await;
            self.journal
                .persist::<Counter>(&[event(self.id, self.next)], None)
                .await
                .unwrap();
            listed
        }
        async fn range(&self, lower: &str, upper: &str) -> Result<Vec<StoredView<Total>>, PersistenceError> {
            self.documents.range(lower, upper).await
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
        let broken = SerializedEvent {
            aggregate_id: "c-1".into(),
            sequence: 1,
            aggregate_type: Counter::TYPE.into(),
            event_type: "added".into(),
            event_version: "1".into(),
            payload: serde_json::json!("not a number"),
            metadata: serde_json::json!({}),
        };
        journal.persist::<Counter>(&[broken], None).await.unwrap();
        let error = super::verify::<Total, Counter>(&*journal, &*documents(&[]))
            .await
            .unwrap_err();
        assert!(!error.to_string().is_empty());
        assert!(matches!(error, PersistenceError::Deserialization(_)));
    }
}
