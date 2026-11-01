//! An in-memory `EventRepository` with the write rules of the PostgreSQL one, its outbox and a
//! position per commit: for tests of everything that reads a journal, without a database.
use crate::{
    domain::aggregate::Aggregate,
    journal::{
        PersistenceError,
        feed::{FeedEntry, Position},
        repository::EventRepository,
        serialized_event::SerializedEvent,
        snapshot::SerializedSnapshot,
    },
    memory::task_queue::Delivery,
    mutex::lock,
    task::NewTask,
};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
};
use tokio::{sync::Semaphore, time::Instant};

/// A stored event with the position of the commit that wrote it.
#[derive(Debug)]
struct Row {
    position: Position,
    event: SerializedEvent,
}

/// The latest snapshot of one aggregate.
#[derive(Debug)]
struct Snapshot {
    payload: Value,
    last_sequence: usize,
    revision: usize,
}

/// A task in the in-memory outbox and the event it was published with.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboxEntry {
    pub aggregate_id: String,
    /// The sequence of the last event of the write that published the task.
    pub aggregate_sequence: usize,
    pub task: NewTask,
}

/// A row of the outbox: the task, the aggregate type and the event it was published with, and the columns of its
/// delivery, which a task queue over the journal reads and writes (`MemoryTaskQueue`).
#[derive(Debug)]
pub(crate) struct OutboxRow {
    pub(crate) aggregate_type: String,
    pub(crate) entry: OutboxEntry,
    pub(crate) delivery: Delivery,
}

#[derive(Debug, Default)]
struct Journal {
    last_position: u64,
    rows: Vec<Row>,
    snapshots: HashMap<(String, String), Snapshot>,
    /// The outbox in commit order.
    outbox: Vec<OutboxRow>,
}

/// Events and snapshots in memory with the rules of `postgres::event_repository::PostgresEventRepository`
/// by meaning, not byte for byte: a taken `(aggregate id, sequence)` is an
/// [`PersistenceError::Conflict`] and nothing of the batch is written; a first snapshot
/// inserts and a later one replaces exactly the previous version, otherwise the same conflict; every
/// non-empty `persist` takes the next [`Position`], shared by all its events, so `feed_after`, `head`
/// and `aggregate_ids_after` behave like the journal of one PostgreSQL aggregate type. The snapshot is
/// kept per aggregate (PostgreSQL keys it by `last_sequence` as well), which decides the same conflicts.
///
/// Clones share the journal, so a test can commit through one handle and read through another. The outbox rows keep
/// the columns of their delivery, which `memory::task_queue::MemoryTaskQueue` claims and settles: the outbox and
/// the task queue are one table, as in PostgreSQL. [`hold_loads`](Self::hold_loads) makes the loads of the aggregates
/// wait for a test, so that a command can hold its aggregate's lock while others queue behind it.
///
/// ```
/// # mod example {
/// # use cqrsx::domain::{aggregate::Aggregate, command::Decide, event::Events};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// # pub struct Counted;
/// # impl Events for Counted {
/// #     fn event_type(&self) -> &'static str { "counted" }
/// #     fn event_version(&self) -> &'static str { "1" }
/// # }
/// # #[derive(Debug, thiserror::Error)]
/// # #[error("never")]
/// # pub struct Never;
/// # #[derive(Debug, Default, Clone, Serialize, Deserialize)]
/// # pub struct Counter;
/// # impl Aggregate for Counter {
/// #     const TYPE: &'static str = "counter";
/// #     type Id = String;
/// #     type Command = ();
/// #     type Event = Counted;
/// #     type Rejection = Never;
/// #     type Rules = ();
/// #     fn apply(&mut self, _: Counted) {}
/// # }
/// # impl Decide<Counter> for () {
/// #     fn decide(self, _: &Counter, _: &()) -> Result<Counted, Never> { Ok(Counted) }
/// # }
/// # }
/// # use example::{Counted, Counter};
/// # use cqrsx::domain::event::EventEnvelope;
/// use cqrsx::journal::{feed::Position, repository::EventRepository, serialized_event::SerializedEvent};
/// use cqrsx::memory::event_repository::MemoryEventRepository;
///
/// # async fn example() -> Result<(), cqrsx::journal::PersistenceError> {
/// # let event = |id: &str, sequence| SerializedEvent::try_from(&EventEnvelope::<Counter> {
/// #     aggregate_id: id.into(), sequence, payload: Counted, metadata: Default::default() }).unwrap();
/// let journal = MemoryEventRepository::default();
/// journal.persist::<Counter>(&[event("a", 1), event("a", 2)], None).await?; // position 1
/// journal.persist::<Counter>(&[event("b", 1)], None).await?;                // position 2
/// journal.persist::<Counter>(&[event("a", 3)], None).await?;                // position 3
///
/// // A page is at least `limit` events long but never cuts a commit in two.
/// let page = journal.feed_after::<Counter>(Position::START, 1).await?;
/// assert_eq!(page.len(), 2);
/// let next = journal.feed_after::<Counter>(page[1].position, 10).await?;
/// assert_eq!((next.len(), next[1].event.aggregate_id.as_str()), (2, "a"));
/// assert_eq!(journal.head::<Counter>().await?, Position::new(3));
/// assert_eq!(journal.aggregate_ids_after::<Counter>("", 10).await?, ["a", "b"]);
/// # Ok(()) }
/// # tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(example()).unwrap();
/// ```
#[derive(Clone, Debug, Default)]
pub struct MemoryEventRepository {
    journal: Arc<Mutex<Journal>>,
    gate: Arc<Mutex<Option<Arc<Semaphore>>>>,
}

impl MemoryEventRepository {
    fn journal(&self) -> MutexGuard<'_, Journal> {
        lock(&self.journal, "in-memory journal")
    }

    /// The tasks published with the events of aggregate type `A`, in commit order: the outbox as the
    /// PostgreSQL repository with an outbox would hold it.
    pub fn outbox<A: Aggregate>(&self) -> Vec<OutboxEntry> {
        self.journal()
            .outbox
            .iter()
            .filter(|row| row.aggregate_type == A::TYPE)
            .map(|row| row.entry.clone())
            .collect()
    }

    /// Runs `operation` on the rows of the outbox, in commit order, under the journal's lock: one statement of a task
    /// queue over the journal.
    pub(crate) fn outbox_rows<T>(&self, operation: impl FnOnce(&mut [OutboxRow]) -> T) -> T {
        operation(&mut self.journal().outbox)
    }

    /// How many events of aggregate type `A` lie above `after`, counted up to `cap` (at least 1): how far a reader
    /// whose checkpoint is `after` is behind, as `PostgresEventRepository::count_after` counts it.
    pub async fn count_after<A: Aggregate>(&self, after: Position, cap: u64) -> Result<u64, PersistenceError> {
        if cap == 0 {
            return Err(PersistenceError::Unexpected("count cap must be positive".into()));
        }
        let journal = self.journal();
        let behind = journal
            .rows
            .iter()
            .filter(|row| row.event.aggregate_type == A::TYPE && row.position > after)
            .count();
        Ok(u64::try_from(behind).unwrap_or(u64::MAX).min(cap))
    }

    /// From now on every read of a snapshot, which begins every load of an aggregate by a store with snapshots, waits
    /// for a permit of the returned gate: a command holds its aggregate's lock while its load waits, and the commands
    /// behind it queue. The rest of the journal does not wait; a closed gate lets every load through.
    pub fn hold_loads(&self) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        *lock(&self.gate, "in-memory journal gate") = Some(gate.clone());
        gate
    }

    /// The rows of type `A` above `after` in journal order (by position, aggregate id and sequence), borrowed: a page
    /// copies only the events it returns.
    fn ordered_after<A: Aggregate>(journal: &Journal, after: Position) -> Vec<&Row> {
        let mut rows: Vec<&Row> = journal
            .rows
            .iter()
            .filter(|row| row.event.aggregate_type == A::TYPE && row.position > after)
            .collect();
        rows.sort_by(|a, b| {
            (a.position, &a.event.aggregate_id, a.event.sequence).cmp(&(b.position, &b.event.aggregate_id, b.event.sequence))
        });
        rows
    }
}

/// A journal page needs a positive limit, checked before anything is read.
fn page_limit(limit: usize) -> Result<(), PersistenceError> {
    if limit == 0 {
        return Err(PersistenceError::Unexpected("feed page limit must be positive".into()));
    }
    Ok(())
}

impl EventRepository for MemoryEventRepository {
    async fn events<A: Aggregate>(&self, aggregate_id: &str) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.events_after::<A>(aggregate_id, 0).await
    }

    async fn events_after<A: Aggregate>(
        &self,
        aggregate_id: &str,
        after_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let journal = self.journal();
        let mut events: Vec<_> = journal
            .rows
            .iter()
            .filter(|row| {
                row.event.aggregate_type == A::TYPE
                    && row.event.aggregate_id == aggregate_id
                    && row.event.sequence > after_sequence
            })
            .map(|row| row.event.clone())
            .collect();
        events.sort_by_key(|event| event.sequence);
        Ok(events)
    }

    async fn snapshot<A: Aggregate>(&self, aggregate_id: &str) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        let gate = lock(&self.gate, "in-memory journal gate").clone();
        // A closed gate lets every load through.
        if let Some(gate) = gate
            && let Ok(permit) = gate.acquire().await
        {
            permit.forget();
        }
        Ok(self
            .journal()
            .snapshots
            .get(&(A::TYPE.to_owned(), aggregate_id.to_owned()))
            .map(|snapshot| SerializedSnapshot {
                aggregate_id: aggregate_id.to_owned(),
                payload: snapshot.payload.clone(),
                last_sequence: snapshot.last_sequence,
                revision: snapshot.revision,
            }))
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<SerializedSnapshot>,
    ) -> Result<(), PersistenceError> {
        self.persist_with_tasks::<A>(events, snapshot_update, &[]).await
    }

    /// The outbox by the PostgreSQL rules: tasks need an event in the same write, a task id is published
    /// once (the primary key: a second one refuses the whole write), and a write that conflicts publishes
    /// nothing. Unlike PostgreSQL several events may share a write; its tasks get the last event's sequence. A task
    /// is pending from the write on, available at once, after its delay or at its time, by the Tokio clock.
    async fn persist_with_tasks<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<SerializedSnapshot>,
        tasks: &[NewTask],
    ) -> Result<(), PersistenceError> {
        let mut journal = self.journal();
        // Everything is checked before anything is written: one transaction, all or nothing.
        let last_event = match (events.last(), tasks.is_empty()) {
            (None, false) => {
                return Err(PersistenceError::Unexpected(
                    "outbox tasks require a committed domain event".into(),
                ));
            }
            (last, _) => last,
        };
        let mut ids = HashSet::new();
        for task in tasks {
            if !ids.insert(task.id()) || journal.outbox.iter().any(|row| row.entry.task.id() == task.id()) {
                return Err(PersistenceError::Unexpected(
                    "duplicate task id: the outbox already holds it".into(),
                ));
            }
        }
        let mut batch: HashSet<(&str, usize)> = HashSet::new();
        for event in events {
            let taken = journal.rows.iter().any(|row| {
                row.event.aggregate_type == A::TYPE
                    && row.event.aggregate_id == event.aggregate_id
                    && row.event.sequence == event.sequence
            });
            if taken || !batch.insert((event.aggregate_id.as_str(), event.sequence)) {
                return Err(PersistenceError::Conflict);
            }
        }
        if let Some(snapshot) = &snapshot_update {
            if snapshot.revision == 0 {
                return Err(PersistenceError::Unexpected("snapshot version must be positive".into()));
            }
            let stored = journal
                .snapshots
                .get(&(A::TYPE.to_owned(), snapshot.aggregate_id.clone()))
                .map(|stored| stored.revision);
            match (snapshot.revision, stored) {
                (1, None) => {}
                (next, Some(version)) if version.checked_add(1) == Some(next) => {}
                _ => return Err(PersistenceError::Conflict),
            }
        }
        if !events.is_empty() {
            journal.last_position += 1;
            let position = Position::new(journal.last_position);
            for event in events {
                let mut event = event.clone();
                event.aggregate_type = A::TYPE.to_owned();
                journal.rows.push(Row { position, event });
            }
        }
        if let Some(snapshot) = snapshot_update {
            journal.snapshots.insert(
                (A::TYPE.to_owned(), snapshot.aggregate_id),
                Snapshot {
                    payload: snapshot.payload,
                    last_sequence: snapshot.last_sequence,
                    revision: snapshot.revision,
                },
            );
        }
        if let Some(event) = last_event {
            let now = Instant::now();
            for task in tasks {
                let entry = OutboxEntry {
                    aggregate_id: event.aggregate_id.clone(),
                    aggregate_sequence: event.sequence,
                    task: task.clone(),
                };
                journal.outbox.push(OutboxRow {
                    aggregate_type: A::TYPE.to_owned(),
                    delivery: Delivery::pending(&entry.task, now),
                    entry,
                });
            }
        }
        Ok(())
    }

    async fn feed_after<A: Aggregate>(&self, after: Position, limit: usize) -> Result<Vec<FeedEntry>, PersistenceError> {
        page_limit(limit)?;
        let journal = self.journal();
        let rows = Self::ordered_after::<A>(&journal, after);
        let mut page: Vec<FeedEntry> = Vec::with_capacity(rows.len().min(limit));
        for row in rows {
            // Never cut a commit in two: a full page runs on to the end of its last position.
            if page.len() >= limit && page.last().is_none_or(|entry| entry.position != row.position) {
                break;
            }
            page.push(FeedEntry {
                position: row.position,
                event: row.event.clone(),
            });
        }
        Ok(page)
    }

    async fn head<A: Aggregate>(&self) -> Result<Position, PersistenceError> {
        Ok(self
            .journal()
            .rows
            .iter()
            .filter(|row| row.event.aggregate_type == A::TYPE)
            .map(|row| row.position)
            .max()
            .unwrap_or(Position::START))
    }

    async fn aggregate_ids_after<A: Aggregate>(&self, after: &str, limit: usize) -> Result<Vec<String>, PersistenceError> {
        page_limit(limit)?;
        let journal = self.journal();
        let mut ids: Vec<String> = journal
            .rows
            .iter()
            .filter(|row| row.event.aggregate_type == A::TYPE && row.event.aggregate_id.as_str() > after)
            .map(|row| row.event.aggregate_id.clone())
            .collect();
        ids.sort();
        ids.dedup();
        ids.truncate(limit);
        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            aggregate::tests::{Delta, Ledger},
            event::EventEnvelope,
        },
        journal::{
            EventStore,
            store::{JournalEventStore, tests::TestAggregate},
        },
    };
    use std::{collections::HashMap, future::Future};

    fn delta(id: &str, sequence: usize, value: i32) -> SerializedEvent {
        SerializedEvent::try_from(&EventEnvelope::<Ledger> {
            aggregate_id: id.into(),
            sequence,
            payload: Delta(value),
            metadata: HashMap::new(),
        })
        .unwrap()
    }

    fn keys(page: &[FeedEntry]) -> Vec<(u64, &str, usize)> {
        page.iter()
            .map(|entry| {
                (
                    entry.position.value(),
                    entry.event.aggregate_id.as_str(),
                    entry.event.sequence,
                )
            })
            .collect()
    }

    fn snapshot(id: &str, last_sequence: usize, revision: usize) -> SerializedSnapshot {
        SerializedSnapshot {
            aggregate_id: id.into(),
            payload: serde_json::json!({ "last": last_sequence }),
            last_sequence,
            revision,
        }
    }

    #[tokio::test]
    async fn a_commit_takes_one_position_shared_by_its_events_and_a_page_ends_on_a_commit() {
        let journal = MemoryEventRepository::default();
        assert_eq!(journal.head::<Ledger>().await.unwrap(), Position::START);
        journal
            .persist::<Ledger>(&[delta("a", 1, 1), delta("a", 2, 2), delta("a", 3, 3)], None)
            .await
            .unwrap();
        journal.persist::<Ledger>(&[delta("b", 1, 1)], None).await.unwrap();
        journal.persist::<Ledger>(&[], None).await.unwrap();
        journal.persist::<Ledger>(&[delta("a", 4, 4)], None).await.unwrap();

        let first = journal.feed_after::<Ledger>(Position::START, 2).await.unwrap();
        assert_eq!(
            keys(&first),
            [(1, "a", 1), (1, "a", 2), (1, "a", 3)],
            "the page runs to the end of commit 1"
        );
        let rest = journal.feed_after::<Ledger>(Position::new(1), 10).await.unwrap();
        assert_eq!(keys(&rest), [(2, "b", 1), (3, "a", 4)], "an empty commit took no position");
        assert!(journal.feed_after::<Ledger>(Position::new(3), 1).await.unwrap().is_empty());
        assert_eq!(journal.head::<Ledger>().await.unwrap(), Position::new(3));
        assert_eq!(journal.feed_after::<Ledger>(Position::START, 10).await.unwrap().len(), 5);
        assert_eq!(
            journal
                .events::<Ledger>("a")
                .await
                .unwrap()
                .iter()
                .map(|e| e.sequence)
                .collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
        assert_eq!(journal.events_after::<Ledger>("a", 3).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn keys_are_listed_in_order_page_by_page_and_other_types_stay_invisible() {
        let journal = MemoryEventRepository::default();
        for id in ["c", "a", "b"] {
            journal
                .persist::<Ledger>(&[delta(id, 1, 1), delta(id, 2, 2)], None)
                .await
                .unwrap();
        }
        journal.persist::<TestAggregate>(&[delta("z", 1, 1)], None).await.unwrap();
        assert_eq!(journal.aggregate_ids_after::<Ledger>("", 2).await.unwrap(), ["a", "b"]);
        assert_eq!(journal.aggregate_ids_after::<Ledger>("b", 2).await.unwrap(), ["c"]);
        assert!(journal.aggregate_ids_after::<Ledger>("c", 2).await.unwrap().is_empty());
        assert_eq!(journal.aggregate_ids_after::<TestAggregate>("", 10).await.unwrap(), ["z"]);
        assert_eq!(journal.head::<TestAggregate>().await.unwrap(), Position::new(4));
        assert_eq!(journal.head::<Ledger>().await.unwrap(), Position::new(3));
        let stored = journal.feed_after::<TestAggregate>(Position::START, 10).await.unwrap();
        assert_eq!(keys(&stored), [(4, "z", 1)]);
        assert_eq!(
            stored[0].event.aggregate_type,
            TestAggregate::TYPE,
            "the type of the persist call is stored"
        );
        assert!(journal.events::<Ledger>("z").await.unwrap().is_empty());
        assert_eq!(journal.feed_after::<Ledger>(Position::START, 10).await.unwrap().len(), 6);
    }

    #[tokio::test]
    async fn a_taken_sequence_conflicts_and_the_whole_batch_is_dropped() {
        let journal = MemoryEventRepository::default();
        journal.persist::<Ledger>(&[delta("a", 1, 1)], None).await.unwrap();
        for batch in [
            vec![delta("a", 2, 2), delta("a", 1, 9)],
            vec![delta("a", 2, 2), delta("a", 2, 3)],
        ] {
            assert!(matches!(
                journal.persist::<Ledger>(&batch, None).await,
                Err(PersistenceError::Conflict)
            ));
        }
        assert_eq!(
            keys(&journal.feed_after::<Ledger>(Position::START, 10).await.unwrap()),
            [(1, "a", 1)]
        );
        journal.persist::<Ledger>(&[delta("a", 2, 2)], None).await.unwrap();
        assert_eq!(
            journal.head::<Ledger>().await.unwrap(),
            Position::new(2),
            "a refused commit takes no position"
        );
    }

    #[tokio::test]
    async fn snapshots_follow_the_version_chain_and_a_conflict_writes_no_event_either() {
        let journal = MemoryEventRepository::default();
        assert!(journal.snapshot::<Ledger>("a").await.unwrap().is_none());
        journal
            .persist::<Ledger>(&[delta("a", 1, 1)], Some(snapshot("a", 1, 1)))
            .await
            .unwrap();
        let stored = journal.snapshot::<Ledger>("a").await.unwrap().unwrap();
        assert_eq!(
            (stored.aggregate_id.as_str(), stored.last_sequence, stored.revision),
            ("a", 1, 1)
        );
        assert_eq!(stored.payload, serde_json::json!({ "last": 1 }));
        for wrong in [1, 3] {
            assert!(matches!(
                journal
                    .persist::<Ledger>(&[delta("a", 2, 2)], Some(snapshot("a", 2, wrong)))
                    .await,
                Err(PersistenceError::Conflict)
            ));
        }
        assert_eq!(
            journal.events::<Ledger>("a").await.unwrap().len(),
            1,
            "the event of a refused commit is gone too"
        );
        assert!(matches!(
            journal
                .persist::<Ledger>(&[delta("a", 2, 2)], Some(snapshot("a", 2, 0)))
                .await,
            Err(PersistenceError::Unexpected(_))
        ));
        journal
            .persist::<Ledger>(&[delta("a", 2, 2)], Some(snapshot("a", 2, 2)))
            .await
            .unwrap();
        journal.persist::<Ledger>(&[], Some(snapshot("a", 2, 3))).await.unwrap();
        assert_eq!(journal.snapshot::<Ledger>("a").await.unwrap().unwrap().revision, 3);
        assert_eq!(
            journal.head::<Ledger>().await.unwrap(),
            Position::new(2),
            "a snapshot alone takes no position"
        );
        assert!(journal.snapshot::<TestAggregate>("a").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_journal_page_needs_a_positive_limit() {
        let journal = MemoryEventRepository::default();
        assert!(matches!(
            journal.feed_after::<Ledger>(Position::START, 0).await,
            Err(PersistenceError::Unexpected(error)) if error.to_string() == "feed page limit must be positive"
        ));
        assert!(matches!(
            journal.aggregate_ids_after::<Ledger>("", 0).await,
            Err(PersistenceError::Unexpected(_))
        ));
    }

    #[tokio::test]
    async fn clones_share_the_journal_and_a_store_commits_into_it() {
        let journal = MemoryEventRepository::default();
        let store = JournalEventStore::<_, Ledger>::new(journal.clone());
        let context = store.load_aggregate("x").await.unwrap();
        store
            .commit(vec![Delta(5), Delta(-2)], context, HashMap::new(), &[])
            .await
            .unwrap();
        let context = store.load_aggregate("x").await.unwrap();
        assert_eq!(context.aggregate.balance, 3);
        store.commit(vec![Delta(1)], context, HashMap::new(), &[]).await.unwrap();
        let page = journal.feed_after::<Ledger>(Position::START, 10).await.unwrap();
        assert_eq!(keys(&page), [(1, "x", 1), (1, "x", 2), (2, "x", 3)]);
        assert_eq!(journal.events_after::<Ledger>("x", 2).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn count_after_counts_the_events_of_a_type_above_a_position_up_to_the_cap() {
        let journal = MemoryEventRepository::default();
        journal
            .persist::<Ledger>(&[delta("a", 1, 1), delta("a", 2, 2)], None)
            .await
            .unwrap();
        journal.persist::<TestAggregate>(&[delta("z", 1, 1)], None).await.unwrap();
        journal.persist::<Ledger>(&[delta("b", 1, 1)], None).await.unwrap();
        let mut counted = Vec::new();
        for (after, cap) in [(0, 10), (1, 10), (3, 10), (0, 2)] {
            counted.push(journal.count_after::<Ledger>(Position::new(after), cap).await.unwrap());
        }
        assert_eq!(counted, [3, 1, 0, 2], "above the position, of its type, up to the cap");
        assert_eq!(journal.count_after::<TestAggregate>(Position::START, 10).await.unwrap(), 1);
        assert!(matches!(
            journal.count_after::<Ledger>(Position::START, 0).await,
            Err(PersistenceError::Unexpected(error)) if error.to_string() == "count cap must be positive"
        ));
    }

    #[tokio::test]
    async fn held_loads_wait_for_a_permit_and_the_rest_of_the_journal_does_not() {
        let journal = MemoryEventRepository::default();
        journal.persist::<Ledger>(&[delta("a", 1, 3)], None).await.unwrap();
        let store = JournalEventStore::<_, Ledger>::new(journal.clone()).with_snapshots(crate::journal::store::tests::every(10));
        let gate = journal.hold_loads();
        let mut load = Box::pin(store.load_aggregate("a"));
        let waiting = std::future::poll_fn(|cx| std::task::Poll::Ready(load.as_mut().poll(cx).is_pending())).await;
        assert!(waiting, "the load waits at the gate");
        assert_eq!(
            journal.events::<Ledger>("a").await.unwrap().len(),
            1,
            "the events do not wait"
        );
        assert_eq!(journal.feed_after::<Ledger>(Position::START, 10).await.unwrap().len(), 1);
        gate.add_permits(1);
        assert_eq!(load.await.unwrap().aggregate.balance, 3);
        gate.close();
        assert_eq!(
            store.load_aggregate("a").await.unwrap().aggregate.balance,
            3,
            "a closed gate lets every load through"
        );
    }

    fn task(n: u8) -> NewTask {
        NewTask::from_payload(
            crate::task::format::TaskFormat::try_new("notify", 1).unwrap(),
            serde_json::json!({ "n": n }),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn tasks_publish_with_their_events_and_never_alone() {
        let journal = MemoryEventRepository::default();
        let alone = journal.persist_with_tasks::<Ledger>(&[], None, &[task(0)]).await.unwrap_err();
        assert_eq!(alone.to_string(), "outbox tasks require a committed domain event");
        journal
            .persist_with_tasks::<Ledger>(&[delta("a", 1, 5), delta("a", 2, 6)], None, &[task(1), task(2)])
            .await
            .unwrap();
        let entries = |journal: &MemoryEventRepository| -> Vec<(String, usize, u64)> {
            journal
                .outbox::<Ledger>()
                .into_iter()
                .map(|entry| {
                    let n = entry.task.payload()["n"].as_u64().unwrap();
                    (entry.aggregate_id, entry.aggregate_sequence, n)
                })
                .collect()
        };
        assert_eq!(
            entries(&journal),
            [("a".to_owned(), 2, 1), ("a".to_owned(), 2, 2)],
            "the tasks of a write go with its last event"
        );
        assert!(matches!(
            journal
                .persist_with_tasks::<Ledger>(&[delta("a", 2, 7)], None, &[task(3)])
                .await,
            Err(PersistenceError::Conflict)
        ));
        let published = journal.outbox::<Ledger>()[0].task.clone();
        let duplicate = journal
            .persist_with_tasks::<Ledger>(&[delta("a", 3, 8)], None, &[task(4), published])
            .await
            .unwrap_err();
        assert_eq!(duplicate.to_string(), "duplicate task id: the outbox already holds it");
        let twice = task(5);
        assert!(
            journal
                .persist_with_tasks::<Ledger>(&[delta("a", 3, 8)], None, &[twice.clone(), twice])
                .await
                .is_err(),
            "one id twice in a write"
        );
        assert_eq!(
            journal.events::<Ledger>("a").await.unwrap().len(),
            2,
            "the refused writes wrote no event"
        );
        assert_eq!(entries(&journal).len(), 2, "and no task");
        assert!(
            journal.outbox::<TestAggregate>().is_empty(),
            "the outbox is per aggregate type"
        );
        journal.persist::<Ledger>(&[delta("a", 3, 8)], None).await.unwrap();
        assert_eq!(entries(&journal).len(), 2, "a write without tasks publishes none");
    }
}
