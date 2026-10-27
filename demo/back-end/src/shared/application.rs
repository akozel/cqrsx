//! What the flows of several modules share: the names of the task policies, the activity log, and the ports to the
//! outside world (the mail service, the read models). The adapters live in `shared::infrastructure`; tests use
//! in-memory doubles. The metadata of a command is cqrsx's (`CommandMetadata`).
pub mod activity;
pub mod mail;
pub mod policies;
pub mod read_model;
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
