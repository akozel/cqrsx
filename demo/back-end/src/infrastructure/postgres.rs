//! PostgreSQL through cqrsx: the event store with the transactional outbox, the command executors
//! built on it and read-only access to the committed history.
use crate::application::command_service::NamedHandler;
use cqrsx::{
    cqrs::{
        CqrsFramework,
        aggregate::Aggregate,
        command::{executor::CqrsCommandExecutor, retry::AggregateConflictRetryPolicy},
        store::persisted::PersistedEventStore,
    },
    postgres::{event_repository::PostgresEventRepository, outbox::PostgresEventRepositoryWithOutbox},
    task::NewTask,
};
use sqlx::PgPool;

/// The write path: one event plus its outbox tasks in one SQL statement, with snapshots.
pub type OutboxStore<A> = PersistedEventStore<PostgresEventRepositoryWithOutbox, A>;
/// The read path over the same tables: committed history, with or without snapshots.
pub type HistoryStore<A> = PersistedEventStore<PostgresEventRepository, A>;
pub type BuildFramework<A> = Box<dyn Fn(Vec<NewTask>) -> CqrsFramework<A, OutboxStore<A>> + Send + Sync>;
pub type PostgresExecutor<A> = CqrsCommandExecutor<A, BuildFramework<A>>;

/// Shared wiring for the executors of both aggregates.
pub struct Executors {
    pub pool: PgPool,
    pub snapshot_every: usize,
    pub retry: AggregateConflictRetryPolicy,
}

impl Executors {
    /// One long-lived executor per aggregate, plus the projection names in registration order.
    /// Every call builds a fresh `CqrsFramework` (via the factory closure) that binds the immutable
    /// outbox task list to one PostgreSQL repository; conflict retries reuse that framework.
    pub fn executor<A>(&self, handlers: Vec<NamedHandler<A>>, services: A::Services) -> (PostgresExecutor<A>, Vec<&'static str>)
    where
        A: Aggregate + 'static,
        A::Services: Clone + 'static,
    {
        let (names, factories): (Vec<_>, Vec<_>) = handlers.into_iter().map(|h| (h.name, h.build)).unzip();
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
        (CqrsCommandExecutor::new(build, self.retry), names)
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
