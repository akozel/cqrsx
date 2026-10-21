//! Test fixtures that need no running database: lazily connecting PostgreSQL pools and MongoDB
//! clients (enough to wire the whole graph and exercise every branch that returns before
//! storage), and in-memory doubles of the read-side ports (the read models are the cqrsx
//! `MemViewRepository`).
use crate::{
    application::{
        StorageError,
        ports::{ReadModelStore, Stored},
    },
    config::{Config, outbox_default},
};
use async_trait::async_trait;
use cqrsx::{
    cqrs::{
        aggregate::Aggregate,
        projection::{View, repository::ViewRepository},
        testing::mem_view_repository::MemViewRepository,
    },
    postgres::pool::StatementTimeout,
    task::runtime::config::TaskRuntimeConfig,
};
use mongodb::{Client, options::ClientOptions};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{num::NonZeroU32, sync::Arc, time::Duration};

/// A valid configuration with the demo defaults and databases nobody listens on.
pub fn config() -> Config {
    Config {
        database_url: String::new(),
        database_pool_size: NonZeroU32::new(10).unwrap(),
        database_statement_timeout: StatementTimeout::Default,
        database_history_pool_size: NonZeroU32::new(1).unwrap(),
        mongodb_uri: String::new(),
        read_db: "encore_read_test".into(),
        http_addr: "127.0.0.1:0".parse().unwrap(),
        mail_service_url: "http://127.0.0.1:1".into(),
        task_max_attempts: 6,
        chain_max_attempts: 60,
        task_retry_base: Duration::from_secs(1),
        task_retry_max: Duration::from_secs(8),
        ticket_limit: 4,
        snapshot_every: 10,
        replay_slow_delay: Duration::ZERO,
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

/// A read-model collection in memory holding `documents`, ordered by id like the MongoDB one.
pub fn read_model<V: View<A>, A: Aggregate>(documents: impl IntoIterator<Item = Stored<V>>) -> Arc<MemViewRepository<V, A>> {
    let repository = MemViewRepository::default();
    for document in documents {
        repository.put(&document.id, document.version, &document.view).unwrap();
    }
    Arc::new(repository)
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

    async fn wipe(&self) -> Result<(), StorageError> {
        self.clear();
        Ok(())
    }
}
