//! Test fixtures that need no running database: lazily connecting PostgreSQL pools and MongoDB
//! clients (enough to wire the whole graph and exercise every branch that returns before
//! storage), and in-memory doubles of the read-side ports (the read models are the cqrsx
//! `MemViewRepository`, the checkpoints its `MemCheckpointStore`).
use crate::{
    bootstrap::projections::checkpoint_keys,
    config::{Config, DEFAULT_MONGODB_POOL_SIZE, DEFAULT_NODE_NAME, outbox_default},
    operations::application::{
        ledger::{EventLedgerStats, LedgerTotals},
        rebuild::{DEFAULT_MAX_PACE, DEFAULT_STATUS_TIMEOUT, ReadSide, ReadSideLimits, RebuildService},
    },
    process::Process,
    shared::application::{
        StorageError,
        activity::ActivityHub,
        read_model::{ReadModelStore, Stored},
    },
};
use async_trait::async_trait;
use cqrsx::{
    cqrs::{
        aggregate::Aggregate,
        command::locks::LockLimits,
        projection::{View, checkpoint::CheckpointStore, processor::config::ProcessorConfig, repository::ViewRepository},
        store::feed::Position,
        testing::{mem_checkpoint_store::MemCheckpointStore, mem_view_repository::MemViewRepository},
    },
    postgres::{event_repository::DEFAULT_COUNT_AFTER_CAP, pool::StatementTimeout},
    task::runtime::config::TaskRuntimeConfig,
};
use mongodb::{Client, options::ClientOptions};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{num::NonZeroU32, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

/// A valid configuration with the demo defaults and databases nobody listens on.
pub fn config() -> Config {
    Config {
        database_url: String::new(),
        database_pool_size: NonZeroU32::new(10).unwrap(),
        database_statement_timeout: StatementTimeout::Default,
        database_history_pool_size: NonZeroU32::new(1).unwrap(),
        database_projection_pool_size: NonZeroU32::new(4).unwrap(),
        mongodb_uri: String::new(),
        mongodb_pool_size: DEFAULT_MONGODB_POOL_SIZE,
        read_db: "encore_read_test".into(),
        http_addr: "127.0.0.1:0".parse().unwrap(),
        mail_service_url: "http://127.0.0.1:1".into(),
        task_max_attempts: 6,
        task_retry_base: Duration::from_secs(1),
        task_retry_max: Duration::from_secs(8),
        ticket_limit: 4,
        snapshot_every: 10,
        command_lock_limits: LockLimits::default(),
        projection: ProcessorConfig::default(),
        projection_lag_cap: DEFAULT_COUNT_AFTER_CAP,
        projection_status_timeout: DEFAULT_STATUS_TIMEOUT,
        node_name: DEFAULT_NODE_NAME.into(),
        replay_slow_delay: Duration::ZERO,
        replay_max_pace: DEFAULT_MAX_PACE,
        task_runtime: TaskRuntimeConfig::from_lookup(outbox_default).unwrap(),
    }
}

/// Nothing listens on port 1: the first acquire fails fast instead of retrying for the default 30 s.
pub fn lazy_pool() -> PgPool {
    PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(200))
        .connect_lazy_with("postgresql://encore:encore@127.0.0.1:1/encore".parse().unwrap())
}

pub async fn lazy_mongo() -> Client {
    let mut options = ClientOptions::parse("mongodb://127.0.0.1:1").await.unwrap();
    options.server_selection_timeout = Some(Duration::from_millis(50));
    Client::with_options(options).unwrap()
}

/// A process over `config` and databases nobody listens on: enough to wire the whole graph and to
/// exercise every branch that answers before storage is touched.
pub async fn process_with(config: Config) -> Process {
    Process {
        config: Arc::new(config),
        pool: lazy_pool(),
        history_pool: lazy_pool(),
        projection_pool: lazy_pool(),
        mongo: lazy_mongo().await,
        shutdown: CancellationToken::new(),
    }
}

/// [`process_with`] over the demo defaults.
pub async fn process() -> Process {
    process_with(config()).await
}

/// A read-model collection in memory holding `documents`, ordered by id like the MongoDB one.
pub fn read_model<V: View<A>, A: Aggregate>(documents: impl IntoIterator<Item = Stored<V>>) -> Arc<MemViewRepository<V, A>> {
    let repository = MemViewRepository::default();
    for document in documents {
        repository
            .put(&document.id, document.version, document.last_sequence, &document.view)
            .unwrap();
    }
    Arc::new(repository)
}

/// A ledger of fixed size whose readers are `lag` events behind.
pub struct FixedLedger {
    pub lag: u64,
}

#[async_trait]
impl EventLedgerStats for FixedLedger {
    async fn totals(&self) -> Result<LedgerTotals, StorageError> {
        Ok(LedgerTotals {
            events: 9,
            aggregates: 3,
        })
    }

    async fn count_after(&self, _: &str, _: Position, cap: u64) -> Result<u64, StorageError> {
        Ok(self.lag.min(cap))
    }
}

/// The checkpoints of both read models in memory, for the router tests, which may not name cqrsx: they
/// drive the rows as running processors would.
#[derive(Clone, Default)]
pub struct Checkpoints(MemCheckpointStore);

impl Checkpoints {
    /// Both rows registered and claimed by `owner`.
    pub async fn owned_by(owner: &str) -> Self {
        let checkpoints = Self::default();
        for key in checkpoint_keys() {
            checkpoints.0.register(&key).await.unwrap();
            checkpoints
                .0
                .claim(&key, owner, Duration::from_secs(60))
                .await
                .unwrap()
                .unwrap();
        }
        checkpoints
    }

    /// Every operation fails while set, like a MongoDB that does not answer.
    pub fn set_unavailable(&self, unavailable: bool) {
        self.0.set_unavailable(unavailable);
    }

    /// The read side over these checkpoints, with readers `lag` events behind.
    pub fn read_side(&self, lag: u64) -> Arc<dyn ReadSide> {
        Arc::new(RebuildService::new(
            Arc::new(self.0.clone()),
            checkpoint_keys(),
            Arc::new(FixedLedger { lag }),
            Arc::new(ActivityHub::new(64, 64)),
            ReadSideLimits {
                lag_cap: DEFAULT_COUNT_AFTER_CAP,
                max_pace: DEFAULT_MAX_PACE,
                status_timeout: DEFAULT_STATUS_TIMEOUT,
            },
            CancellationToken::new(),
        ))
    }
}

#[async_trait]
impl<V, A> ReadModelStore<V> for MemViewRepository<V, A>
where
    V: View<A> + 'static,
    A: Aggregate + 'static,
{
    async fn load(&self, id: &str) -> Result<Option<Stored<V>>, StorageError> {
        let found = self.load_with_context(id).await.map_err(StorageError::new)?;
        Ok(found.map(|(view, context)| Stored {
            id: context.view_instance_id,
            version: context.version,
            last_sequence: context.last_sequence,
            view,
        }))
    }

    async fn list(&self) -> Result<Vec<Stored<V>>, StorageError> {
        let documents = self.list_all().map_err(StorageError::new)?;
        Ok(documents
            .into_iter()
            .map(|(context, view)| Stored {
                id: context.view_instance_id,
                version: context.version,
                last_sequence: context.last_sequence,
                view,
            })
            .collect())
    }

    async fn list_range(&self, lower: &str, upper: &str) -> Result<Vec<Stored<V>>, StorageError> {
        let all = ReadModelStore::list(self).await?;
        Ok(all
            .into_iter()
            .filter(|document| lower <= document.id.as_str() && document.id.as_str() < upper)
            .collect())
    }
}
