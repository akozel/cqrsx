//! Where a framework keeps its data: the one module that asks which store the builder chose. Everything else takes the
//! answer through the methods of [`Journal`]: the executor of an aggregate type, the installation of a view, the task
//! queue, the history, closing the pools, and what the stores in memory give a test.
//!
//! The choice is a record of the builder ([`Stores`], with the kinds [`EventStoreKind`] and [`ProjectionStoreKind`]) until
//! the stores are opened ([`Stores::open`]) and a [`Journal`] after.
#[cfg(feature = "test-utils")]
use crate::memory::{MemoryJournal, MemoryViewStore};
#[cfg(feature = "projections-mongodb")]
use crate::mongodb::{checkpoint_store::MongoCheckpointStore, view_repository::MongoViewRepository};
#[cfg(feature = "event-store-postgres")]
use crate::postgres::{
    PostgresPools, event_repository::PostgresEventRepository, outbox::PostgresEventRepositoryWithOutbox,
    task_queue::PostgresTaskQueue,
};
#[cfg(feature = "projections-postgres")]
use crate::postgres::{checkpoint_store::PostgresCheckpointStore, view_repository::PostgresViewRepository};
#[cfg(feature = "operation-utils")]
use crate::views::JournalTotals;
use crate::{
    build::{BuildError, ExecutorParts, ViewParts},
    settings::Settings,
    views::Counts,
};
#[cfg(feature = "test-utils")]
use cqrsx::memory::event_repository::MemoryEventRepository;
#[cfg(feature = "test-utils")]
use cqrsx::memory::task_queue::MemoryTaskQueue;
use cqrsx::{
    command::{CommandError, CommandExecutor, CommitReport, executor::StoreCommandExecutor},
    domain::{aggregate::Aggregate, command::Command, rejection::Rejection},
    journal::{PersistenceError, feed::Position, store::EventSourcedStore},
    reactions::Reactions,
    task::format::TaskFormat,
    view::View,
    worker::queue::{
        TaskProcessingError, TaskQueue,
        lease::LeasedTask,
        settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult},
    },
};
#[cfg(feature = "operation-utils")]
use cqrsx::{
    domain::event::EventEnvelope,
    journal::{AggregateStore, repository::EventRepository},
};
use std::{collections::HashMap, future::Future, num::NonZeroUsize, sync::Arc, time::Duration};

// The kinds are `pub` in this private module: the sealed traits of the markers (`build::sealed`) name them in a constant
// that is reachable at `pub` through `EventStore`/`ProjectionStore`; nothing outside the crate can name the kinds.
/// The journal a framework is built with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventStoreKind {
    #[cfg(feature = "test-utils")]
    Memory,
    #[cfg(feature = "event-store-postgres")]
    Postgres,
}

impl EventStoreKind {
    /// Whether the journal outlives the process.
    pub fn is_durable(self) -> bool {
        match self {
            #[cfg(feature = "test-utils")]
            Self::Memory => false,
            #[cfg(feature = "event-store-postgres")]
            Self::Postgres => true,
        }
    }
}

/// The views' store a framework is built with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionStoreKind {
    #[cfg(feature = "test-utils")]
    Memory,
    #[cfg(feature = "projections-postgres")]
    Postgres,
    #[cfg(feature = "projections-mongodb")]
    MongoDb,
}

impl ProjectionStoreKind {
    /// The name of the store when it outlives the process.
    pub fn durable_name(self) -> Option<&'static str> {
        match self {
            #[cfg(feature = "test-utils")]
            Self::Memory => None,
            #[cfg(feature = "projections-postgres")]
            Self::Postgres => Some("PostgreSQL"),
            #[cfg(feature = "projections-mongodb")]
            Self::MongoDb => Some("MongoDB"),
        }
    }
}

/// The stores a framework is built over, and what the application opened for them.
#[derive(Default)]
pub(crate) struct Stores {
    pub(crate) event_store: Option<EventStoreKind>,
    pub(crate) views: Option<ProjectionStoreKind>,
    #[cfg(feature = "event-store-postgres")]
    pub(crate) pools: Option<PostgresPools>,
    #[cfg(feature = "projections-mongodb")]
    pub(crate) client: Option<::mongodb::Client>,
}

impl Stores {
    /// The framework has its journal, and its views have a store when there are views: the kind of the journal.
    pub(crate) fn ensure_chosen(&self, has_views: bool) -> Result<EventStoreKind, BuildError> {
        let event_store = self.event_store.ok_or(BuildError::NoJournal)?;
        if has_views && self.views.is_none() {
            return Err(BuildError::ViewsWithoutStore);
        }
        Ok(event_store)
    }

    /// The journal over the chosen stores (`event_store`, the kind [`ensure_chosen`](Self::ensure_chosen) gave), opened
    /// now or taken from the application: in memory (a test), or the pools of PostgreSQL in the order of the plan (main,
    /// projection, history) with the views in PostgreSQL (the main pool), in MongoDB (a lazy client) or in memory. The
    /// checkpoints live with the views: a framework without views has none.
    pub(crate) async fn open(self, event_store: EventStoreKind, settings: &Settings) -> Result<Journal, BuildError> {
        match event_store {
            #[cfg(feature = "test-utils")]
            EventStoreKind::Memory => {
                // Nothing to open in memory.
                let _ = settings;
                Ok(Journal::Memory {
                    journal: MemoryJournal::new(),
                    views: MemoryViewStore::new(),
                })
            }
            #[cfg(feature = "event-store-postgres")]
            EventStoreKind::Postgres => {
                let pools = match self.pools {
                    Some(pools) => pools,
                    None => crate::postgres::open(settings).await?,
                };
                let views = match self.views {
                    #[cfg(feature = "test-utils")]
                    Some(ProjectionStoreKind::Memory) => ViewStore::Memory(MemoryViewStore::new()),
                    #[cfg(feature = "projections-mongodb")]
                    Some(ProjectionStoreKind::MongoDb) => {
                        let client = match self.client {
                            Some(client) => client,
                            None => crate::mongodb::client(settings).await?,
                        };
                        ViewStore::MongoDb(client.database(settings.mongodb().database()))
                    }
                    #[cfg(feature = "projections-postgres")]
                    Some(ProjectionStoreKind::Postgres) => ViewStore::Postgres,
                    // Without views nothing reads a store of the views.
                    None => ViewStore::None,
                };
                Ok(Journal::Postgres { pools, views })
            }
        }
    }
}

/// Where the framework keeps the journal, the snapshots, the outbox and the task queue, and its views.
pub(crate) enum Journal {
    #[cfg(feature = "test-utils")]
    Memory { journal: MemoryJournal, views: MemoryViewStore },
    #[cfg(feature = "event-store-postgres")]
    Postgres { pools: PostgresPools, views: ViewStore },
}

/// Where a framework over PostgreSQL keeps the views and their checkpoints.
#[cfg(feature = "event-store-postgres")]
pub(crate) enum ViewStore {
    #[cfg(feature = "test-utils")]
    Memory(MemoryViewStore),
    #[cfg(feature = "projections-postgres")]
    Postgres,
    #[cfg(feature = "projections-mongodb")]
    MongoDb(::mongodb::Database),
    /// The framework has no views: nothing reads a store of the views.
    None,
}

impl Journal {
    /// The executor of `A` over the journal's store with `parts`, a snapshot every `snapshot_every` events.
    pub(crate) fn executor<A>(&self, snapshot_every: NonZeroUsize, parts: ExecutorParts<A>) -> Executor<A>
    where
        A: Reactions + Clone + 'static,
        A::Command: Command + Clone,
        A::Rejection: Rejection + Send + 'static,
    {
        match self {
            #[cfg(feature = "test-utils")]
            Self::Memory { journal, .. } => {
                let store = EventSourcedStore::new(journal.journal.clone()).with_snapshots(snapshot_every);
                Executor::Memory(parts.over(store))
            }
            #[cfg(feature = "event-store-postgres")]
            Self::Postgres { pools, .. } => {
                let store = EventSourcedStore::new(PostgresEventRepositoryWithOutbox::new(pools.main.clone()))
                    .with_snapshots(snapshot_every);
                Executor::Postgres(parts.over(store))
            }
        }
    }

    /// Installs the processor of `V` over the journal and the checkpoints of the views' store, and the repository of its
    /// documents, which the queries read; `parts` registers them.
    #[cfg_attr(
        not(any(feature = "test-utils", feature = "projections-postgres", feature = "projections-mongodb")),
        expect(
            unused_variables,
            clippy::extra_unused_type_parameters,
            clippy::needless_pass_by_value,
            reason = "without a store of the views no view is installed"
        )
    )]
    pub(crate) fn install_view<V: View<A> + PartialEq + 'static, A: Aggregate + 'static>(
        &mut self,
        parts: ViewParts<'_>,
    ) -> Result<(), BuildError> {
        match self {
            #[cfg(feature = "test-utils")]
            Self::Memory { journal, views } => {
                let documents = views.documents::<V, A>();
                let checkpoints = views.checkpoints.clone();
                let journal = journal.journal.clone();
                parts.register::<V, A, _, _, _, _, _>(
                    Arc::new(journal.clone()),
                    checkpoints,
                    documents,
                    journal.clone(),
                    journal,
                );
                Ok(())
            }
            #[cfg(feature = "event-store-postgres")]
            Self::Postgres { pools, views } => match views {
                ViewStore::None => Err(BuildError::ViewsWithoutStore),
                #[cfg(feature = "test-utils")]
                ViewStore::Memory(views) => {
                    let (feed, history, counter) = pools.view_readers();
                    let (documents, checkpoints) = (views.documents::<V, A>(), views.checkpoints.clone());
                    parts.register::<V, A, _, _, _, _, _>(feed, checkpoints, documents, history, counter);
                    Ok(())
                }
                #[cfg(feature = "projections-postgres")]
                ViewStore::Postgres => {
                    let (feed, history, counter) = pools.view_readers();
                    let main = pools.main.clone();
                    let documents = Arc::new(PostgresViewRepository::<V, A>::new(main.clone()));
                    let checkpoints = Arc::new(PostgresCheckpointStore::new(main));
                    parts.register::<V, A, _, _, _, _, _>(feed, checkpoints, documents, history, counter);
                    Ok(())
                }
                #[cfg(feature = "projections-mongodb")]
                ViewStore::MongoDb(database) => {
                    let (feed, history, counter) = pools.view_readers();
                    let documents = Arc::new(MongoViewRepository::<V, A>::new(database));
                    let checkpoints = Arc::new(MongoCheckpointStore::new(database));
                    parts.register::<V, A, _, _, _, _, _>(feed, checkpoints, documents, history, counter);
                    Ok(())
                }
            },
        }
    }

    /// The task queue over the journal's outbox.
    pub(crate) fn task_queue(&self) -> Arc<Queue> {
        match self {
            #[cfg(feature = "test-utils")]
            Self::Memory { journal, .. } => Arc::new(Queue::Memory(journal.queue.clone())),
            #[cfg(feature = "event-store-postgres")]
            Self::Postgres { pools, .. } => Arc::new(Queue::Postgres(PostgresTaskQueue::new(pools.main.clone()))),
        }
    }

    /// Closes the pools of a framework over PostgreSQL: the main pool, the history pool, the projection pool.
    #[cfg(feature = "axum")]
    pub(crate) async fn close(&self) {
        match self {
            #[cfg(feature = "test-utils")]
            Self::Memory { .. } => {}
            #[cfg(feature = "event-store-postgres")]
            Self::Postgres { pools, .. } => {
                pools.main.close().await;
                pools.history.close().await;
                pools.projection.close().await;
            }
        }
    }

    /// The committed events of the aggregate `id`, from the journal alone, without a snapshot (on PostgreSQL through the
    /// pool of whole-history reads, without a statement timeout).
    #[cfg(feature = "operation-utils")]
    pub(crate) async fn history<A: Aggregate>(&self, id: &str) -> Result<Vec<EventEnvelope<A>>, PersistenceError> {
        match self {
            #[cfg(feature = "test-utils")]
            Self::Memory { journal, .. } => EventSourcedStore::<_, A>::new(journal.journal.clone()).load_events(id).await,
            #[cfg(feature = "event-store-postgres")]
            Self::Postgres { pools, .. } => {
                EventSourcedStore::<_, A>::new(PostgresEventRepository::new(pools.history.clone()))
                    .load_events(id)
                    .await
            }
        }
    }

    /// The journal in memory; `None` when the framework keeps its journal in PostgreSQL.
    #[cfg(feature = "test-utils")]
    pub(crate) fn memory(&self) -> Option<&MemoryJournal> {
        match self {
            Self::Memory { journal, .. } => Some(journal),
            #[cfg(feature = "event-store-postgres")]
            Self::Postgres { .. } => None,
        }
    }

    /// The views in memory; `None` when the framework keeps its views in PostgreSQL or MongoDB, or has none.
    #[cfg(feature = "test-utils")]
    pub(crate) fn memory_views(&self) -> Option<&MemoryViewStore> {
        match self {
            Self::Memory { views, .. } => Some(views),
            #[cfg(feature = "event-store-postgres")]
            Self::Postgres { views, .. } => match views {
                ViewStore::Memory(views) => Some(views),
                #[cfg(feature = "projections-postgres")]
                ViewStore::Postgres => None,
                #[cfg(feature = "projections-mongodb")]
                ViewStore::MongoDb(_) => None,
                ViewStore::None => None,
            },
        }
    }
}

/// The executor of one aggregate type over the framework's journal: the variants are the stores, so a command from an
/// entry point waits for the store's own future, without an allocation.
pub(crate) enum Executor<A: Aggregate> {
    #[cfg(feature = "test-utils")]
    Memory(StoreCommandExecutor<A, EventSourcedStore<MemoryEventRepository, A>>),
    #[cfg(feature = "event-store-postgres")]
    Postgres(StoreCommandExecutor<A, EventSourcedStore<PostgresEventRepositoryWithOutbox, A>>),
}

impl<A> CommandExecutor<A> for Executor<A>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone + 'static,
    A::Rejection: Send + 'static,
{
    async fn execute_with_metadata(
        &self,
        id: &A::Id,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, CommandError<A::Rejection>> {
        match self {
            #[cfg(feature = "test-utils")]
            Self::Memory(executor) => executor.execute_with_metadata(id, command, metadata).await,
            #[cfg(feature = "event-store-postgres")]
            Self::Postgres(executor) => executor.execute_with_metadata(id, command, metadata).await,
        }
    }
}

/// The task queue of the framework's journal: the variants are the stores. The core's [`TaskQueue`] returns
/// `impl Future`, so it cannot be a trait object.
pub(crate) enum Queue {
    #[cfg(feature = "test-utils")]
    Memory(Arc<MemoryTaskQueue>),
    #[cfg(feature = "event-store-postgres")]
    Postgres(PostgresTaskQueue),
}

impl TaskQueue for Queue {
    async fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
        match self {
            #[cfg(feature = "test-utils")]
            Self::Memory(queue) => queue.claim(format, owner, limit, lease_duration).await,
            #[cfg(feature = "event-store-postgres")]
            Self::Postgres(queue) => queue.claim(format, owner, limit, lease_duration).await,
        }
    }

    async fn settle(&self, task: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
        match self {
            #[cfg(feature = "test-utils")]
            Self::Memory(queue) => queue.settle(task, request).await,
            #[cfg(feature = "event-store-postgres")]
            Self::Postgres(queue) => queue.settle(task, request).await,
        }
    }

    async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
        match self {
            #[cfg(feature = "test-utils")]
            Self::Memory(queue) => queue.recover_expired(limit).await,
            #[cfg(feature = "event-store-postgres")]
            Self::Postgres(queue) => queue.recover_expired(limit).await,
        }
    }
}

#[cfg(feature = "test-utils")]
impl Counts for MemoryEventRepository {
    fn count_after<A: Aggregate>(&self, after: Position, cap: u64) -> impl Future<Output = Result<u64, PersistenceError>> + Send {
        MemoryEventRepository::count_after::<A>(self, after, cap)
    }

    #[cfg(feature = "operation-utils")]
    async fn totals<A: Aggregate>(&self) -> Result<JournalTotals, PersistenceError> {
        let events = MemoryEventRepository::count_after::<A>(self, Position::START, u64::MAX).await?;
        let (mut aggregates, mut after) = (0, String::new());
        loop {
            let page = self.aggregate_ids_after::<A>(&after, 100).await?;
            let Some(last) = page.last() else {
                return Ok(JournalTotals { events, aggregates });
            };
            after = last.clone();
            aggregates += page.len() as u64;
        }
    }
}

#[cfg(feature = "event-store-postgres")]
impl Counts for PostgresEventRepository {
    fn count_after<A: Aggregate>(&self, after: Position, cap: u64) -> impl Future<Output = Result<u64, PersistenceError>> + Send {
        PostgresEventRepository::count_after::<A>(self, after, cap)
    }

    #[cfg(feature = "operation-utils")]
    async fn totals<A: Aggregate>(&self) -> Result<JournalTotals, PersistenceError> {
        let totals = PostgresEventRepository::totals::<A>(self).await?;
        Ok(JournalTotals {
            events: totals.events,
            aggregates: totals.aggregates,
        })
    }
}
