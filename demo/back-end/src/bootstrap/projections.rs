//! The read models: a MongoDB collection and a repository each, the maintenance Encore! and verify run
//! over them, and the ports the HTTP queries read through. A new read model is declared here and
//! handed to its aggregate's executor in [`aggregates`](super::aggregates).
use crate::{
    application::{
        activity::ActivityHub,
        concert::views::concert_hall::ConcertHallView,
        fan::views::fan_wallet::FanWalletView,
        projections::{
            maintenance::{MaintainedReadModel, ProjectionMaintenance},
            notifying::NotifyingHandler,
        },
        read_models::ReadModels,
    },
    domain::{concert::Concert, fan::Fan},
    infrastructure::{
        mongo::read_side::{CONCERT_HALLS, FAN_WALLETS, HallRepository, ReadSide, WalletRepository, scratch},
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
    let read = ReadSide::new(&read_db);
    let history = history_repositories(process.history_pool.clone());
    let maintenance: Vec<Arc<dyn MaintainedReadModel>> = vec![
        Arc::new(ProjectionMaintenance::<_, _, ConcertHallView, Concert>::new(
            CONCERT_HALLS,
            history.clone(),
            read.halls.clone(),
            scratch(&read_db, CONCERT_HALLS),
        )),
        Arc::new(ProjectionMaintenance::<_, _, FanWalletView, Fan>::new(
            FAN_WALLETS,
            history,
            read.wallets.clone(),
            scratch(&read_db, FAN_WALLETS),
        )),
    ];
    ReadSideWiring {
        queries: ReadModels {
            halls: read.halls.clone(),
            wallets: read.wallets.clone(),
        },
        halls: read.halls,
        wallets: read.wallets,
        maintenance,
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
