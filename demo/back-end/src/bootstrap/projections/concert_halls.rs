//! The concert halls: the seat map of every concert, fed from the journal of `Concert`.
use super::{ReadModel, processor};
use crate::{
    concert::{application::views::concert_hall::ConcertHallView, domain::Concert},
    operations::application::verify::ProjectionVerification,
    process::Process,
};
use cqrsx::{
    cqrs::command::wakeup::CommitWakeup,
    engine::build::Registrations,
    mongodb::{projection_checkpoints::MongoCheckpointStore, view_repository::MongoViewRepository},
    postgres::event_repository::PostgresEventRepository,
};
use mongodb::Database;
use std::sync::Arc;

pub fn wire(
    process: &Process,
    read_db: &Database,
    checkpoints: &Arc<MongoCheckpointStore>,
    wakeup: &CommitWakeup,
    registrations: &Registrations,
) -> ReadModel<ConcertHallView, Concert> {
    let repository = Arc::new(MongoViewRepository::new(read_db));
    ReadModel {
        verification: Arc::new(ProjectionVerification::<_, _, ConcertHallView, Concert>::new(
            Arc::new(PostgresEventRepository::new(process.history_pool.clone())),
            repository.clone(),
        )),
        processor: processor(process, repository.clone(), checkpoints, wakeup, registrations),
        repository,
    }
}
