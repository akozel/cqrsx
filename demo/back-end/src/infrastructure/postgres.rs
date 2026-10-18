//! PostgreSQL through cqrsx: read-only access to the committed history and the size of the ledger.
//! The command executors are the library's `PostgresCommandExecutor`, assembled in `bootstrap`.
use crate::{
    application::{
        StorageError,
        ports::{EventLedgerStats, LedgerTotals},
        projections::maintenance::HistoryFactory,
    },
    domain::{concert::Concert, fan::Fan},
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

/// Size of the ledger: the saved history of both aggregate types, counted by the library (the
/// owner of the `events` table).
pub struct PostgresLedger {
    repository: PostgresEventRepository,
}

impl PostgresLedger {
    pub fn new(pool: PgPool) -> Self {
        Self {
            repository: PostgresEventRepository::new(pool),
        }
    }
}

#[async_trait]
impl EventLedgerStats for PostgresLedger {
    async fn totals(&self) -> Result<LedgerTotals, StorageError> {
        let concerts = self.repository.totals::<Concert>().await.map_err(StorageError::new)?;
        let fans = self.repository.totals::<Fan>().await.map_err(StorageError::new)?;
        Ok(LedgerTotals {
            events: concerts.events + fans.events,
            aggregates: concerts.aggregates + fans.aggregates,
        })
    }
}
