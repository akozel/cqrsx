//! Atomic event/snapshot/task publication for one accepted aggregate command.
mod write;
mod write_plan;

use self::write_plan::WritePlan;
use crate::event_repository::PostgresEventRepository;
use cqrsx::{
    domain::aggregate::Aggregate,
    journal::{
        PersistenceError,
        repository::PersistedEventRepository,
        serialized_event::SerializedEvent,
        snapshot::{SerializedSnapshot, SnapshotUpdate},
    },
    task::NewTask,
};
use sqlx::PgPool;
/// Commit one accepted event, its optional snapshot, and its tasks atomically
/// ([`persist_with_tasks`](PersistedEventRepository::persist_with_tasks)).
///
/// Multi-event batches, snapshot-only writes, and tasks without an event are
/// rejected. The repository holds no state of a command: build one per process
/// and share it, the tasks come with every write. Reads delegate to a plain
/// PostgreSQL repository on the same pool. A clone shares the pool and the SQL
/// texts.
#[derive(Clone, Debug)]
pub struct PostgresEventRepositoryWithOutbox {
    pool: PgPool,
    reader: PostgresEventRepository,
}

impl PostgresEventRepositoryWithOutbox {
    pub fn new(pool: PgPool) -> Self {
        Self {
            reader: PostgresEventRepository::new(pool.clone()),
            pool,
        }
    }
}

impl PersistedEventRepository for PostgresEventRepositoryWithOutbox {
    async fn events<A: Aggregate>(&self, aggregate_id: &str) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.reader.events::<A>(aggregate_id).await
    }

    async fn events_after<A: Aggregate>(
        &self,
        aggregate_id: &str,
        after_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.reader.events_after::<A>(aggregate_id, after_sequence).await
    }

    async fn snapshot<A: Aggregate>(&self, aggregate_id: &str) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        self.reader.snapshot::<A>(aggregate_id).await
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<SnapshotUpdate>,
    ) -> Result<(), PersistenceError> {
        self.persist_with_tasks::<A>(events, snapshot_update, &[]).await
    }

    async fn persist_with_tasks<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<SnapshotUpdate>,
        tasks: &[NewTask],
    ) -> Result<(), PersistenceError> {
        match WritePlan::try_new(events, snapshot_update, tasks)? {
            WritePlan::NoOp => Ok(()),
            WritePlan::OneEvent {
                event,
                sequence,
                snapshot,
            } => write::persist_one::<A>(&self.pool, tasks, event, sequence, snapshot).await,
        }
    }

    async fn feed_after<A: Aggregate>(
        &self,
        after: cqrsx::journal::feed::Position,
        limit: usize,
    ) -> Result<Vec<cqrsx::journal::feed::FeedEntry>, PersistenceError> {
        self.reader.feed_after::<A>(after, limit).await
    }

    async fn head<A: Aggregate>(&self) -> Result<cqrsx::journal::feed::Position, PersistenceError> {
        self.reader.head::<A>().await
    }

    async fn aggregate_ids_after<A: Aggregate>(&self, after: &str, limit: usize) -> Result<Vec<String>, PersistenceError> {
        self.reader.aggregate_ids_after::<A>(after, limit).await
    }
}
