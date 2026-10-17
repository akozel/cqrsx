//! Application layer: command services (metadata + outbox tasks around the cqrsx command port),
//! activity log, personas, the box-office lock and the rebuild, time travel and verify services.
pub mod activity;
pub mod command_service;
pub mod lock;
pub mod personas;
pub mod read_side;
pub mod rebuild;
pub mod tasks;
pub mod time_travel;
pub mod verify;

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
