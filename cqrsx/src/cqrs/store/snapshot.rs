//! Persisted snapshots and exact-prefix updates committed with events.
// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use serde_json::Value;

/// A snapshot of exactly the committed prefix ending at `last_sequence`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotUpdate {
    pub aggregate_id: String,
    pub payload: Value,
    pub last_sequence: usize,
    pub next_version: usize,
}

/// Used by repositories to store and load snapshots from a database.
#[derive(Debug, PartialEq, Eq)]
pub struct SerializedSnapshot {
    /// The aggregate ID of the aggregate instance that has been loaded.
    pub aggregate_id: String,
    /// The current state of the aggregate instance.
    pub aggregate: Value,
    /// The last committed event sequence number for this aggregate instance.
    pub last_sequence: usize,
    /// The last committed snapshot version for this aggregate instance.
    pub snapshot_version: usize,
}
