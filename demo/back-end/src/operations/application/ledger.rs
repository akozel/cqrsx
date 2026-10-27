//! The port to the event ledger: its size and how far a reader of it is behind. The adapter is
//! `bootstrap::ledger`; tests use in-memory doubles.
use crate::shared::application::StorageError;
use async_trait::async_trait;
use cqrsx::cqrs::store::feed::Position;

/// Size of the event ledger, for progress reporting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LedgerTotals {
    pub events: u64,
    pub aggregates: u64,
}

/// The size of the event ledger and how far a reader of it is behind.
#[async_trait]
pub trait EventLedgerStats: Send + Sync {
    async fn totals(&self) -> Result<LedgerTotals, StorageError>;
    /// The events of `aggregate_type` above `after`, counted up to `cap`: the lag of a projection whose
    /// checkpoint stands at `after`.
    async fn count_after(&self, aggregate_type: &str, after: Position, cap: u64) -> Result<u64, StorageError>;
}
