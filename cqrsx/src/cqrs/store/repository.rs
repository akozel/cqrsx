// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::{
    aggregate::Aggregate,
    store::{
        PersistenceError,
        feed::{FeedEntry, Position},
        serialized_event::SerializedEvent,
        snapshot::SerializedSnapshot,
        stream::ReplayStream,
    },
};
use std::future::Future;

/// The persistence behind a `PersistedEventStore`: the events and snapshots of every aggregate,
/// written one commit per transaction, and the journal of an aggregate type read in commit order
/// (`feed_after`, `head`, `aggregate_ids_after`; see `store::feed`).
pub trait PersistedEventRepository: Send + Sync {
    /// Returns all events for a single aggregate instance.
    fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, PersistenceError>> + Send;

    /// Returns events strictly after `after_sequence`, ordered by aggregate sequence.
    fn get_events_after<A: Aggregate>(
        &self,
        aggregate_id: &str,
        after_sequence: usize,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, PersistenceError>> + Send;

    /// Returns the current snapshot for an aggregate instance.
    fn get_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> impl Future<Output = Result<Option<SerializedSnapshot>, PersistenceError>> + Send;

    /// Commits the updated aggregate and accompanying events.
    fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<crate::cqrs::store::snapshot::SnapshotUpdate>,
    ) -> impl Future<Output = Result<(), PersistenceError>> + Send;

    /// Streams all events for an aggregate instance.
    fn stream_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> impl Future<Output = Result<ReplayStream, PersistenceError>> + Send;

    /// Streams all events for an aggregate type.
    fn stream_all_events<A: Aggregate>(&self) -> impl Future<Output = Result<ReplayStream, PersistenceError>> + Send;

    /// The events of aggregate type `A` after `after` in the order of [`Position`], taken from the
    /// transactions that had finished when the read began: at least `limit` events when that many
    /// exist, and always to the end of the last transaction started, so a page never cuts a commit in
    /// two. Positions never decrease along a page; inside one aggregate the sequence grows by one.
    /// A reader that keeps the last position of every page sees every event exactly once: a slower
    /// transaction with a lower id is waited for, never skipped. `limit == 0` is an error.
    fn feed_after<A: Aggregate>(
        &self,
        after: Position,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<FeedEntry>, PersistenceError>> + Send;

    /// The greatest position among the written events of aggregate type `A`, [`Position::START`]
    /// for an empty journal. It can lie above what `feed_after` returns yet, while a slower
    /// transaction with a lower id is still open.
    fn head<A: Aggregate>(&self) -> impl Future<Output = Result<Position, PersistenceError>> + Send;

    /// The ids of the aggregates of type `A` above `after` in id order, at most `limit` of them:
    /// the keys of a journal page by page, for a projection that rebuilds its documents.
    /// `limit == 0` is an error.
    fn aggregate_ids_after<A: Aggregate>(
        &self,
        after: &str,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<String>, PersistenceError>> + Send;
}
