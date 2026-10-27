//! The read side the HTTP queries use: both read models, read through cqrsx's view repository (`ViewRepository`:
//! one document, every document, a range of ids), so that the presentation never touches a database driver.
use crate::{
    concert::{application::views::concert_hall::ConcertHallView, domain::Concert},
    fan::{application::views::fan_wallet::FanWalletView, domain::Fan},
};
#[cfg(test)]
use cqrsx::cqrs::testing::mem_view_repository::MemViewRepository;
use cqrsx::{
    cqrs::{
        aggregate::Aggregate,
        projection::{
            View,
            repository::{StoredView, ViewContext, ViewRepository},
        },
        store::PersistenceError,
    },
    mongodb::view_repository::MongoViewRepository,
};
use std::sync::Arc;

pub struct ReadModels {
    pub halls: Arc<Documents<ConcertHallView, Concert>>,
    pub wallets: Arc<Documents<FanWalletView, Fan>>,
}

/// Where the documents of one read model live: the collection of the read database in the process (the repository
/// its projection processor writes through), memory in the router tests. Until the engine serves the views of the
/// process (backlog 05/12) the queries reach the one or the other through this enum, by delegation.
pub enum Documents<V, A> {
    MongoDb(Arc<MongoViewRepository<V, A>>),
    #[cfg(test)]
    Memory(MemViewRepository<V, A>),
}

impl<V: View<A>, A: Aggregate> ViewRepository<V, A> for Documents<V, A> {
    async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError> {
        match self {
            Self::MongoDb(documents) => documents.load(view_id).await,
            #[cfg(test)]
            Self::Memory(documents) => documents.load(view_id).await,
        }
    }

    async fn load_with_context(&self, view_id: &str) -> Result<Option<(V, ViewContext)>, PersistenceError> {
        match self {
            Self::MongoDb(documents) => documents.load_with_context(view_id).await,
            #[cfg(test)]
            Self::Memory(documents) => documents.load_with_context(view_id).await,
        }
    }

    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        match self {
            Self::MongoDb(documents) => documents.update_view(view, context).await,
            #[cfg(test)]
            Self::Memory(documents) => documents.update_view(view, context).await,
        }
    }

    async fn list(&self) -> Result<Vec<StoredView<V>>, PersistenceError> {
        match self {
            Self::MongoDb(documents) => documents.list().await,
            #[cfg(test)]
            Self::Memory(documents) => documents.list().await,
        }
    }

    async fn range(&self, lower: &str, upper: &str) -> Result<Vec<StoredView<V>>, PersistenceError> {
        match self {
            Self::MongoDb(documents) => documents.range(lower, upper).await,
            #[cfg(test)]
            Self::Memory(documents) => documents.range(lower, upper).await,
        }
    }
}
