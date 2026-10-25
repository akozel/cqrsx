//! Atomic event/snapshot/task publication for one accepted aggregate command.
mod write;
mod write_plan;

use self::write_plan::WritePlan;
use crate::{
    cqrs::{
        aggregate::Aggregate,
        store::{
            PersistenceError, repository::PersistedEventRepository, serialized_event::SerializedEvent,
            snapshot::SerializedSnapshot,
        },
    },
    postgres::event_repository::PostgresEventRepository,
    task::NewTask,
};
use sqlx::PgPool;
/// Commit one accepted event, its optional snapshot, and immutable tasks atomically.
///
/// Multi-event batches, snapshot-only writes, and tasks without an event are
/// rejected. Bind a fresh repository/task list per command; share the pool.
/// Reads delegate to a plain PostgreSQL repository on the same pool.
pub struct PostgresEventRepositoryWithOutbox {
    pool: PgPool,
    reader: PostgresEventRepository,
    tasks: Vec<NewTask>,
}

impl PostgresEventRepositoryWithOutbox {
    pub fn new(pool: PgPool) -> Self {
        Self {
            reader: PostgresEventRepository::new(pool.clone()),
            pool,
            tasks: Vec::new(),
        }
    }

    /// Bind work to one command execution, never to a shared mutable repository.
    pub fn with_outbox(mut self, tasks: Vec<NewTask>) -> Self {
        self.tasks = tasks;
        self
    }

    async fn persist_command<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<crate::cqrs::store::snapshot::SnapshotUpdate>,
    ) -> Result<(), PersistenceError> {
        match WritePlan::new(events, snapshot_update, &self.tasks)? {
            WritePlan::NoOp => Ok(()),
            WritePlan::OneEvent {
                event,
                sequence,
                snapshot,
            } => write::persist_one::<A>(&self.pool, &self.tasks, event, sequence, snapshot).await,
        }
    }
}

impl PersistedEventRepository for PostgresEventRepositoryWithOutbox {
    async fn get_events<A: Aggregate>(&self, aggregate_id: &str) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.reader.get_events::<A>(aggregate_id).await
    }

    async fn get_events_after<A: Aggregate>(
        &self,
        aggregate_id: &str,
        after_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.reader.get_events_after::<A>(aggregate_id, after_sequence).await
    }

    async fn get_snapshot<A: Aggregate>(&self, aggregate_id: &str) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        self.reader.get_snapshot::<A>(aggregate_id).await
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<crate::cqrs::store::snapshot::SnapshotUpdate>,
    ) -> Result<(), PersistenceError> {
        self.persist_command::<A>(events, snapshot_update).await
    }

    async fn feed_after<A: Aggregate>(
        &self,
        after: crate::cqrs::store::feed::Position,
        limit: usize,
    ) -> Result<Vec<crate::cqrs::store::feed::FeedEntry>, PersistenceError> {
        self.reader.feed_after::<A>(after, limit).await
    }

    async fn head<A: Aggregate>(&self) -> Result<crate::cqrs::store::feed::Position, PersistenceError> {
        self.reader.head::<A>().await
    }

    async fn aggregate_ids_after<A: Aggregate>(&self, after: &str, limit: usize) -> Result<Vec<String>, PersistenceError> {
        self.reader.aggregate_ids_after::<A>(after, limit).await
    }
}
