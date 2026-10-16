// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::{
    aggregate::Aggregate,
    store::{PersistenceError, serialized_event::SerializedEvent, snapshot::SerializedSnapshot, stream::ReplayStream},
};
use std::future::Future;

/// Handles the database access needed for operation of a PersistedSnapshotStore.
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
}
