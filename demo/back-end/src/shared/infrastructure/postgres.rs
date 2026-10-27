//! PostgreSQL through cqrsx: read-only access to the committed history. The command executors are the library's
//! `PostgresCommandExecutor`, assembled in `bootstrap`; the size of the ledger is `bootstrap::ledger`.
use cqrsx::{
    cqrs::{aggregate::Aggregate, store::persisted::PersistedEventStore},
    postgres::event_repository::PostgresEventRepository,
};
use sqlx::PgPool;

/// The read path over the event tables: committed history.
pub type HistoryStore<A> = PersistedEventStore<PostgresEventRepository, A>;

/// Committed history from events alone, without snapshots (the time machine shows that state is
/// stored nowhere).
pub fn event_history<A: Aggregate>(pool: PgPool) -> HistoryStore<A> {
    PersistedEventStore::new_event_store(PostgresEventRepository::new(pool))
}
