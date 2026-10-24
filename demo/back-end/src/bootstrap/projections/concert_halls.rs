//! The concert halls: the seat map of every concert, fed by the `Concert` executor.
use super::ReadModel;
use crate::{
    application::{
        concert::views::concert_hall::ConcertHallView,
        projections::maintenance::{HistoryFactory, ProjectionMaintenance},
    },
    domain::concert::Concert,
    infrastructure::mongo::{
        read_model::MongoReadModel,
        read_side::{CONCERT_HALLS, HallRepository, scratch},
    },
};
use cqrsx::postgres::event_repository::PostgresEventRepository;
use mongodb::Database;
use std::sync::Arc;

pub fn wire(read_db: &Database, history: &HistoryFactory<PostgresEventRepository>) -> ReadModel<HallRepository> {
    let repository = Arc::new(MongoReadModel::new(read_db, CONCERT_HALLS));
    ReadModel {
        maintenance: Arc::new(ProjectionMaintenance::<_, _, ConcertHallView, Concert>::new(
            CONCERT_HALLS,
            history.clone(),
            repository.clone(),
            scratch(read_db, CONCERT_HALLS),
        )),
        repository,
    }
}
