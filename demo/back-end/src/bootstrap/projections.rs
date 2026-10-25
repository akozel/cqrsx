//! One module per read model: its MongoDB collection, the projection processor that feeds it from the journal
//! of its aggregate type and the verification of it. The index collects them for the aggregates (whose
//! executors wake the processors after a commit, see [`aggregates`](super::aggregates)), the operations and
//! the HTTP queries. A new read model gets a module like these two, listed here.
pub mod concert_halls;
pub mod fan_wallets;

use crate::{
    application::{
        activity::ActivityHub, projections::activity_observer::ActivityObserver, projections::verification::VerifiedReadModel,
        read_models::ReadModels,
    },
    domain::{concert::Concert, fan::Fan},
    infrastructure::mongo::{
        read_model::MongoReadModel,
        read_side::{CONCERT_HALLS, FAN_WALLETS},
    },
    process::Process,
};
use cqrsx::{
    cqrs::{
        aggregate::Aggregate,
        command::wakeup::CommitWakeup,
        projection::{
            View, ViewProjection, checkpoint::CheckpointKey, processor::ProjectionProcessor, runtime::ProjectionRuntime,
        },
    },
    mongodb::projection_checkpoints::MongoCheckpointStore,
    postgres::event_repository::PostgresEventRepository,
};
use std::sync::Arc;

/// The processor of a read model: the journal on the projection pool, the checkpoint next to the views.
pub type Processor<A> = ProjectionProcessor<A, PostgresEventRepository, MongoCheckpointStore>;

/// One read model as its module wires it.
pub struct ReadModel<R, A: Aggregate> {
    pub repository: Arc<R>,
    pub verification: Arc<dyn VerifiedReadModel>,
    pub processor: Processor<A>,
}

/// The read side as the composition root sees it.
pub struct ReadSideWiring {
    /// What the HTTP queries read through, behind ports.
    pub queries: ReadModels,
    /// Verify over every read model, from the history pool.
    pub verification: Vec<Arc<dyn VerifiedReadModel>>,
    /// Both processors, started next to the task runtime by `process`.
    pub runtime: ProjectionRuntime,
    /// The checkpoint rows of both projections in the read database.
    pub checkpoints: Arc<MongoCheckpointStore>,
    /// What the executors wake after a commit: one per aggregate type, shared with its processor.
    pub concert_wakeup: CommitWakeup,
    pub fan_wakeup: CommitWakeup,
}

/// The checkpoint rows of the read models, the concert halls first: the projection names are the
/// collection names.
pub fn checkpoint_keys() -> Vec<CheckpointKey> {
    vec![
        CheckpointKey::new(CONCERT_HALLS, Concert::TYPE),
        CheckpointKey::new(FAN_WALLETS, Fan::TYPE),
    ]
}

/// Every read model and its checkpoint live in `ENCORE_READ_DB`.
pub fn read_models(process: &Process, activity: &Arc<ActivityHub>) -> ReadSideWiring {
    let read_db = process.mongo.database(&process.config.read_db);
    let checkpoints = Arc::new(MongoCheckpointStore::new(&read_db));
    let (concert_wakeup, fan_wakeup) = (CommitWakeup::new(), CommitWakeup::new());
    let halls = concert_halls::wire(process, &read_db, &checkpoints, &concert_wakeup, activity);
    let wallets = fan_wallets::wire(process, &read_db, &checkpoints, &fan_wakeup, activity);
    ReadSideWiring {
        queries: ReadModels {
            halls: halls.repository,
            wallets: wallets.repository,
        },
        verification: vec![halls.verification, wallets.verification],
        runtime: ProjectionRuntime::new()
            .with_processor(halls.processor)
            .with_processor(wallets.processor),
        checkpoints,
        concert_wakeup,
        fan_wakeup,
    }
}

/// A read model's processor: the projection under the collection's name (the name the feed and the
/// checkpoint carry), the journal of `A` through the projection pool, this node as the owner.
pub fn processor<V, A>(
    process: &Process,
    name: &'static str,
    repository: Arc<MongoReadModel<V, A>>,
    checkpoints: &Arc<MongoCheckpointStore>,
    wakeup: &CommitWakeup,
    activity: &Arc<ActivityHub>,
) -> Processor<A>
where
    V: View<A> + 'static,
    A: Aggregate + 'static,
{
    let config = &process.config;
    ProjectionProcessor::new(
        name,
        Arc::new(PostgresEventRepository::new(process.projection_pool.clone())),
        checkpoints.clone(),
        Arc::new(ViewProjection::new(repository).with_name(name)),
        config.projection,
    )
    .with_observer(Arc::new(ActivityObserver::new(
        name,
        config.node_name.clone(),
        activity.clone(),
    )))
    .with_wakeup(wakeup.clone())
    .with_owner(config.node_name.clone())
}
