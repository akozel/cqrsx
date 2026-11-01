//! Persisted snapshots, written with events and read back.
// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and cqrsx/licenses/.
use serde_json::Value;

/// A snapshot of exactly the committed prefix ending at `last_sequence`, in both directions: the repository gets it
/// with the events that complete the prefix and returns it when it loads the aggregate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SerializedSnapshot {
    /// The aggregate ID of the aggregate instance that has been loaded.
    pub aggregate_id: String,
    /// The state of the aggregate instance.
    pub payload: Value,
    /// The last committed event sequence number for this aggregate instance.
    pub last_sequence: usize,
    /// The revision of the snapshot: the one to write (on a write) or the one stored (on a read).
    pub revision: usize,
}
