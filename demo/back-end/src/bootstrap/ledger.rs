//! The size of the event ledger and how far a projection is behind it, over PostgreSQL through cqrsx: the
//! adapter of the operations' `EventLedgerStats`. It sums the journals of every aggregate type of the box office,
//! which only the composition root knows.
use crate::{
    concert::domain::Concert,
    fan::domain::Fan,
    operations::application::ledger::{EventLedgerStats, LedgerTotals},
    shared::application::StorageError,
};
use async_trait::async_trait;
use cqrsx::{
    cqrs::{aggregate::Aggregate, store::feed::Position},
    postgres::event_repository::PostgresEventRepository,
};
use sqlx::PgPool;

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
