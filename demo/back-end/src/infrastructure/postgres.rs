//! PostgreSQL through cqrsx: read-only access to the committed history and the size of the ledger.
//! The command executors are the library's `PostgresCommandExecutor`, assembled in `bootstrap`.
use crate::application::{
    StorageError,
    ports::{EventLedgerStats, LedgerTotals},
    projections::maintenance::HistoryFactory,
};
use async_trait::async_trait;
use cqrsx::{
    cqrs::{aggregate::Aggregate, store::persisted::PersistedEventStore},
    postgres::event_repository::PostgresEventRepository,
};
use sqlx::PgPool;
use std::sync::Arc;

/// The read path over the event tables: committed history, with or without snapshots.
pub type HistoryStore<A> = PersistedEventStore<PostgresEventRepository, A>;

/// Committed state, accelerated by the snapshots the command executors write every
/// `snapshot_every` events.
pub fn history_store<A: Aggregate>(pool: PgPool, snapshot_every: usize) -> HistoryStore<A> {
    PersistedEventStore::new_snapshot_store(PostgresEventRepository::new(pool), snapshot_every)
}

/// Committed history from events alone, without snapshots (the time machine shows that state is
/// stored nowhere).
pub fn event_history<A: Aggregate>(pool: PgPool) -> HistoryStore<A> {
    PersistedEventStore::new_event_store(PostgresEventRepository::new(pool))
}

/// A fresh repository over the saved history for every replay (`EventReplay` takes it by value).
pub fn history_repositories(pool: PgPool) -> HistoryFactory<PostgresEventRepository> {
    Arc::new(move || PostgresEventRepository::new(pool.clone()))
}

/// Size of the ledger, counted in the `events` table.
pub struct PostgresLedger {
    pool: PgPool,
}

impl PostgresLedger {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EventLedgerStats for PostgresLedger {
    async fn totals(&self) -> Result<LedgerTotals, StorageError> {
        let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events")
            .fetch_one(&self.pool)
            .await
            .map_err(StorageError::new)?;
        let aggregates: i64 = sqlx::query_scalar("SELECT count(DISTINCT (aggregate_type, aggregate_id)) FROM events")
            .fetch_one(&self.pool)
            .await
            .map_err(StorageError::new)?;
        Ok(LedgerTotals {
            events: events.max(0) as u64,
            aggregates: aggregates.max(0) as u64,
        })
    }
}
