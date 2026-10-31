//! What the flows of several modules share: the names of the task policies, the activity log, the port to the outside
//! world (the mail service) and the ticket limit the UI shows. The adapters live in `shared::infrastructure`; tests use
//! in-memory doubles. The commands and the queries of the read models are the engine's.
pub mod activity;
pub mod mail;
pub mod policies;
#[cfg(test)]
pub(crate) mod test_support;

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

/// The tickets one fan may hold for one concert (rule R6), as the UI shows it (`GET /api/config`): the box office
/// provides it with its rules (`concert::module`), the operations read it from the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TicketLimit(pub usize);
