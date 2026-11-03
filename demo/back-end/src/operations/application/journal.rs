//! The port to the journal: its size and how far a reader of it is behind. The adapter is the framework's views
//! (`framework::FrameworkReadSide`); tests use in-memory doubles.
use crate::shared::application::StorageError;
use async_trait::async_trait;
use cqrsx::journal::feed::Position;
use cqrsx_framework::views::JournalTotals;

/// The size of the journal and how far a reader of it is behind.
#[async_trait]
pub trait JournalStats: Send + Sync {
    async fn totals(&self) -> Result<JournalTotals, StorageError>;
    /// The events of `aggregate_type` above `after`, counted up to `cap`: the lag of a projection whose
    /// checkpoint stands at `after`.
    async fn count_after(&self, aggregate_type: &str, after: Position, cap: u64) -> Result<u64, StorageError>;
}
