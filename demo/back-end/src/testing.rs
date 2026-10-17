//! Test fixtures that need no running database: lazily connecting PostgreSQL pools and MongoDB
//! clients (enough to wire the whole graph and exercise every branch that returns before
//! storage), and in-memory doubles of the read-side ports.
use crate::{
    application::{
        StorageError,
        ports::{DeliveredMail, Inbox, ReadModelStore, Stored},
    },
    domain::FanId,
};
use async_trait::async_trait;
use mongodb::{Client, options::ClientOptions};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

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

/// A read-model collection in memory, ordered by id like the MongoDB one.
pub struct MemoryReadModelStore<V> {
    documents: Mutex<BTreeMap<String, Stored<V>>>,
}

impl<V> MemoryReadModelStore<V> {
    pub fn new(documents: impl IntoIterator<Item = Stored<V>>) -> Arc<Self> {
        Arc::new(Self {
            documents: Mutex::new(documents.into_iter().map(|d| (d.id.clone(), d)).collect()),
        })
    }
}

#[async_trait]
impl<V: Clone + Send + 'static> ReadModelStore<V> for MemoryReadModelStore<V> {
    async fn load(&self, id: &str) -> Result<Option<Stored<V>>, StorageError> {
        Ok(self.documents.lock().unwrap().get(id).cloned())
    }

    async fn list(&self) -> Result<Vec<Stored<V>>, StorageError> {
        Ok(self.documents.lock().unwrap().values().cloned().collect())
    }

    async fn wipe(&self) -> Result<(), StorageError> {
        self.documents.lock().unwrap().clear();
        Ok(())
    }
}

/// The external mail service in memory.
pub struct MemoryInbox(pub Vec<DeliveredMail>);

#[async_trait]
impl Inbox for MemoryInbox {
    async fn of_fan(&self, fan_id: &FanId) -> Result<Vec<DeliveredMail>, StorageError> {
        Ok(self.0.iter().filter(|m| m.to_fan_id == fan_id.as_str()).cloned().collect())
    }
}
