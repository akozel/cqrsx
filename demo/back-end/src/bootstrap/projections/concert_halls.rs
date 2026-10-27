//! The concert halls: the seat map of every concert, fed from the journal of `Concert`.
use super::{ReadModel, processor};
use crate::{
    concert::{
        application::views::concert_hall::{self, ConcertHallView},
        domain::Concert,
    },
    operations::application::verification::ProjectionVerification,
    process::Process,
    shared::{application::activity::ActivityHub, infrastructure::mongo_read_model::MongoReadModel},
};
use cqrsx::{
    cqrs::command::wakeup::CommitWakeup, mongodb::projection_checkpoints::MongoCheckpointStore,
    postgres::event_repository::PostgresEventRepository,
};
use mongodb::Database;
use std::sync::Arc;

pub type HallRepository = MongoReadModel<ConcertHallView, Concert>;

pub fn wire(
    process: &Process,
    read_db: &Database,
    checkpoints: &Arc<MongoCheckpointStore>,
    wakeup: &CommitWakeup,
    activity: &Arc<ActivityHub>,
) -> ReadModel<HallRepository, Concert> {
    let repository = Arc::new(MongoReadModel::new(read_db, concert_hall::NAME));
    ReadModel {
        verification: Arc::new(ProjectionVerification::<_, _, ConcertHallView, Concert>::new(
            concert_hall::NAME,
            Arc::new(PostgresEventRepository::new(process.history_pool.clone())),
            repository.clone(),
        )),
        processor: processor(process, concert_hall::NAME, repository.clone(), checkpoints, wakeup, activity),
        repository,
    }
}
