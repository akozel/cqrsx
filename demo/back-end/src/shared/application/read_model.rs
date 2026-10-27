//! The port to the read models: what the HTTP queries and verify read. The adapter lives in
//! `shared::infrastructure`; tests use in-memory doubles.
use crate::shared::application::StorageError;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

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
