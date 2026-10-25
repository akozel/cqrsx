//! PostgreSQL through cqrsx: read-only access to the committed history, the size of the ledger and how
//! far a projection is behind it. The command executors are the library's `PostgresCommandExecutor`,
//! assembled in `bootstrap`.
use crate::{
    application::{
        StorageError,
        ports::{EventLedgerStats, LedgerTotals},
    },
    domain::{concert::Concert, fan::Fan},
};
use async_trait::async_trait;
use cqrsx::{
    cqrs::{
        aggregate::Aggregate,
        store::{feed::Position, persisted::PersistedEventStore},
    },
    postgres::event_repository::PostgresEventRepository,
};
use sqlx::PgPool;

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

/// Size of the ledger and the lag of a reader, counted by the library (the owner of the `events` table):
/// the totals read the whole history on the history pool, the lag is a short count on the feed index under
/// the main pool's statement timeout.
pub struct PostgresLedger {
    journal: PostgresEventRepository,
    history: PostgresEventRepository,
}

impl PostgresLedger {
    pub fn new(pool: PgPool, history_pool: PgPool) -> Self {
        Self {
            journal: PostgresEventRepository::new(pool),
            history: PostgresEventRepository::new(history_pool),
        }
    }
}

#[async_trait]
impl EventLedgerStats for PostgresLedger {
    async fn totals(&self) -> Result<LedgerTotals, StorageError> {
        let concerts = self.history.totals::<Concert>().await.map_err(StorageError::new)?;
        let fans = self.history.totals::<Fan>().await.map_err(StorageError::new)?;
        Ok(LedgerTotals {
            events: concerts.events + fans.events,
            aggregates: concerts.aggregates + fans.aggregates,
        })
    }

    async fn count_after(&self, aggregate_type: &str, after: Position, cap: u64) -> Result<u64, StorageError> {
        let counted = if aggregate_type == Concert::TYPE {
            self.journal.count_after::<Concert>(after, cap).await
        } else if aggregate_type == Fan::TYPE {
            self.journal.count_after::<Fan>(after, cap).await
        } else {
            return Err(StorageError(format!("no journal of {aggregate_type}")));
        };
        counted.map_err(StorageError::new)
    }
}
