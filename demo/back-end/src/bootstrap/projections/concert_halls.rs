//! The concert halls: the seat map of every concert, fed from the journal of `Concert`.
use super::{ReadModel, processor};
use crate::{
    application::{
        activity::ActivityHub, concert::views::concert_hall::ConcertHallView, projections::verification::ProjectionVerification,
    },
    domain::concert::Concert,
    infrastructure::mongo::{
        read_model::MongoReadModel,
        read_side::{CONCERT_HALLS, HallRepository},
    },
    process::Process,
};
use cqrsx::{
    cqrs::command::wakeup::CommitWakeup, mongodb::projection_checkpoints::MongoCheckpointStore,
    postgres::event_repository::PostgresEventRepository,
};
use mongodb::Database;
use std::sync::Arc;

pub fn wire(
    process: &Process,
    read_db: &Database,
    checkpoints: &Arc<MongoCheckpointStore>,
    wakeup: &CommitWakeup,
    activity: &Arc<ActivityHub>,
) -> ReadModel<HallRepository, Concert> {
    let repository = Arc::new(MongoReadModel::new(read_db, CONCERT_HALLS));
    ReadModel {
        verification: Arc::new(ProjectionVerification::<_, _, ConcertHallView, Concert>::new(
            CONCERT_HALLS,
            Arc::new(PostgresEventRepository::new(process.history_pool.clone())),
            repository.clone(),
        )),
        processor: processor(process, CONCERT_HALLS, repository.clone(), checkpoints, wakeup, activity),
        repository,
    }
}
