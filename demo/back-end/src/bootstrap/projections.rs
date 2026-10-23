//! One module per read model: its MongoDB collection and repository, and the maintenance Encore! and
//! verify run over it. The index collects them for the aggregates (which register the projections on
//! their executors in [`aggregates`](super::aggregates)), the operations and the HTTP queries. A new
//! read model gets a module like these two, listed here and handed to its aggregate.
pub mod concert_halls;
pub mod fan_wallets;

use crate::{
    application::{
        activity::ActivityHub,
        projections::{maintenance::MaintainedReadModel, notifying::NotifyingHandler},
        read_models::ReadModels,
    },
    infrastructure::{
        mongo::read_side::{HallRepository, WalletRepository},
        postgres::history_repositories,
    },
    process::Process,
};
use cqrsx::cqrs::{
    aggregate::Aggregate,
    event_handler::EventHandler,
    projection::{View, ViewProjection, repository::ViewRepository},
};
use std::sync::Arc;

/// One read model as its module wires it: the live repository and the maintenance over it.
pub struct ReadModel<R> {
    pub repository: Arc<R>,
    pub maintenance: Arc<dyn MaintainedReadModel>,
}

/// The read side as the composition root sees it.
pub struct ReadSideWiring {
    /// The repositories the projections write and the queries read.
    pub halls: Arc<HallRepository>,
    pub wallets: Arc<WalletRepository>,
    /// Encore! and verify over every read model, in replay order, from the PostgreSQL history.
    pub maintenance: Vec<Arc<dyn MaintainedReadModel>>,
    /// What the HTTP queries read through, behind ports.
    pub queries: ReadModels,
}

/// Every read model lives in `ENCORE_READ_DB`; its maintenance replays the history pool.
pub fn read_models(process: &Process) -> ReadSideWiring {
    let read_db = process.mongo.database(&process.config.read_db);
    let history = history_repositories(process.history_pool.clone());
    let halls = concert_halls::wire(&read_db, &history);
    let wallets = fan_wallets::wire(&read_db, &history);
    ReadSideWiring {
        queries: ReadModels {
            halls: halls.repository.clone(),
            wallets: wallets.repository.clone(),
        },
        maintenance: vec![halls.maintenance, wallets.maintenance],
        halls: halls.repository,
        wallets: wallets.repository,
    }
}

/// A read model as an event handler of its aggregate's executor: the projection under the collection's
/// name (the name the activity log and the `projection_failed` warning report), announcing every update.
pub fn handler<R, V, A>(repository: Arc<R>, name: &'static str, activity: Arc<ActivityHub>) -> Arc<dyn EventHandler<A>>
where
    R: ViewRepository<V, A> + 'static,
    V: View<A> + 'static,
    A: Aggregate + 'static,
{
    Arc::new(NotifyingHandler::new(
        ViewProjection::new(repository).with_name(name),
        activity,
    ))
}
