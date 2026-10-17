//! PostgreSQL through cqrsx: the event store with the transactional outbox, the command executors
//! built on it and read-only access to the committed history.
use crate::application::{
    StorageError,
    ports::{EventLedgerStats, LedgerTotals},
    projections::maintenance::HistoryFactory,
};
use async_trait::async_trait;
use cqrsx::{
    cqrs::{
        CqrsFramework,
        aggregate::Aggregate,
        command::{executor::CqrsCommandExecutor, retry::AggregateConflictRetryPolicy},
        event_handler::EventHandler,
        store::persisted::PersistedEventStore,
    },
    postgres::{event_repository::PostgresEventRepository, outbox::PostgresEventRepositoryWithOutbox},
    task::NewTask,
};
use sqlx::PgPool;
use std::sync::Arc;

/// The write path: one event plus its outbox tasks in one SQL statement, with snapshots.
pub type OutboxStore<A> = PersistedEventStore<PostgresEventRepositoryWithOutbox, A>;
/// The read path over the same tables: committed history, with or without snapshots.
pub type HistoryStore<A> = PersistedEventStore<PostgresEventRepository, A>;
pub type BuildFramework<A> = Box<dyn Fn(Vec<NewTask>) -> CqrsFramework<A, OutboxStore<A>> + Send + Sync>;
pub type PostgresExecutor<A> = CqrsCommandExecutor<A, BuildFramework<A>>;
/// Builds one event handler; called for every framework, i.e. for every command.
pub type HandlerFactory<A> = Arc<dyn Fn() -> Box<dyn EventHandler<A>> + Send + Sync>;

/// Shared wiring for the executors of both aggregates.
pub struct Executors {
    pub pool: PgPool,
    pub snapshot_every: usize,
    pub retry: AggregateConflictRetryPolicy,
}

impl Executors {
    /// One long-lived executor per aggregate. Every call builds a fresh `CqrsFramework` (via the
    /// factory closure) that binds the immutable outbox task list to one PostgreSQL repository;
    /// conflict retries reuse that framework.
    pub fn executor<A>(&self, factories: Vec<HandlerFactory<A>>, services: A::Services) -> PostgresExecutor<A>
    where
        A: Aggregate + 'static,
        A::Services: Clone + 'static,
    {
        let pool = self.pool.clone();
        let snapshot_every = self.snapshot_every;
        let build: BuildFramework<A> = Box::new(move |tasks| {
            let repository = PostgresEventRepositoryWithOutbox::new(pool.clone()).with_outbox(tasks);
            CqrsFramework::new(
                PersistedEventStore::new_snapshot_store(repository, snapshot_every),
                factories.iter().map(|build| build()).collect(),
                services.clone(),
            )
        });
        CqrsCommandExecutor::new(build, self.retry)
    }

    /// Committed state, accelerated by the same snapshots the executors write.
    pub fn history<A: Aggregate>(&self) -> HistoryStore<A> {
        PersistedEventStore::new_snapshot_store(PostgresEventRepository::new(self.pool.clone()), self.snapshot_every)
    }
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
