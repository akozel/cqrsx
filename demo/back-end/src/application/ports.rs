//! Ports: what the application needs from the outside world. The adapters live in
//! `infrastructure`; tests use in-memory doubles.
use crate::{application::StorageError, domain::FanId};
use async_trait::async_trait;
use cqrsx::task::{attempt::TaskAttempt, handler::TaskHandlerError};
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

/// An e-mail as the external mail service stored it (`GET /api/fans/{fan_id}/inbox`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveredMail {
    pub inbox_id: String,
    pub kind: String,
    pub to_fan_id: String,
    pub to_name: String,
    pub subject: String,
    pub concert_id: String,
    pub seat: String,
    pub amount: u32,
    pub task_id: String,
    pub delivered_on_attempt: i32,
    /// More than one means repeated deliveries were suppressed as duplicates.
    pub deliveries: i64,
    pub first_delivered_at: String,
}

/// Read access to the external mail service.
#[async_trait]
pub trait Inbox: Send + Sync {
    /// The fan's e-mails, oldest first.
    async fn of_fan(&self, fan_id: &FanId) -> Result<Vec<DeliveredMail>, StorageError>;
}

/// One stored read-model document: the view and the version its repository keeps for optimistic
/// locking.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Stored<V> {
    pub id: String,
    pub version: i64,
    pub view: V,
}

/// Read access to one read-model collection, plus the wipe of Encore!.
#[async_trait]
pub trait ReadModelStore<V>: Send + Sync {
    async fn load(&self, id: &str) -> Result<Option<Stored<V>>, StorageError>;
    /// Every document, ordered by id.
    async fn list(&self) -> Result<Vec<Stored<V>>, StorageError>;
    /// Drops every document. The ledger is untouched; Encore! rebuilds them.
    async fn wipe(&self) -> Result<(), StorageError>;
}

/// Size of the event ledger, for progress reporting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LedgerTotals {
    pub events: u64,
    pub aggregates: u64,
}

#[async_trait]
pub trait EventLedgerStats: Send + Sync {
    async fn totals(&self) -> Result<LedgerTotals, StorageError>;
}
