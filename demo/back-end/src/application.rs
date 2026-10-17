//! Application layer: the use cases of both aggregates (metadata + outbox tasks around the cqrsx
//! command port), the workflows driven by outbox tasks, read models, the activity log, the
//! box-office lock and the operations over the whole ledger (rebuild, verify, time travel).
pub mod activity;
pub mod command_service;
pub mod concert;
pub mod fan;
pub mod lock;
pub mod operations;
pub mod projections;
pub mod tasks;
pub mod workflows;

/// A storage round trip (PostgreSQL or MongoDB) failed. The message is operator-facing; the HTTP
/// layer turns it into `503 unavailable`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct StorageError(pub String);

impl StorageError {
    pub fn new(error: impl std::fmt::Display) -> Self {
        Self(error.to_string())
    }
}
