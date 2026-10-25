//! Ports: what the application needs from the outside world. The adapters live in
//! `infrastructure`; tests use in-memory doubles.
use crate::application::StorageError;
use async_trait::async_trait;
use cqrsx::{
    cqrs::store::feed::Position,
    task::{attempt::TaskAttempt, handler::TaskHandlerError},
};
use serde::{Deserialize, Serialize};
use std::future::Future;

/// An e-mail for the external mail service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mail {
    /// Business key and idempotency key: `e_ticket:<booking_id>` or `refund:<cancellation_id>`.
    pub id: String,
    pub kind: &'static str,
    pub to_fan_id: String,
    pub to_name: String,
    pub subject: String,
    pub concert_id: String,
    pub seat: String,
    pub amount: u32,
}

/// The external mail service, called from a task attempt. Delivery is at-least-once (a lease can
/// expire, the process can crash after the effect and before settlement), so a repeated `send`
/// of the same `Mail::id` must not create a second e-mail. A failure asks the queue to retry.
pub trait Mailer: Send {
    fn send(&mut self, attempt: &TaskAttempt, mail: Mail) -> impl Future<Output = Result<(), TaskHandlerError>> + Send;
}

/// One stored read-model document: the view, the version its repository keeps for optimistic locking and
/// the last event of its aggregate it has applied (verify folds the history up to it).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Stored<V> {
    pub id: String,
    pub version: i64,
    pub last_sequence: usize,
    pub view: V,
}

/// Read access to one read-model collection. The projection processors write it; nothing here does.
#[async_trait]
pub trait ReadModelStore<V>: Send + Sync {
    async fn load(&self, id: &str) -> Result<Option<Stored<V>>, StorageError>;
    /// Every document, ordered by id.
    async fn list(&self) -> Result<Vec<Stored<V>>, StorageError>;
    /// The documents whose id is in `[lower, upper)`, ordered by id (a fan's ledgers, D-14).
    async fn list_range(&self, lower: &str, upper: &str) -> Result<Vec<Stored<V>>, StorageError>;
}

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
