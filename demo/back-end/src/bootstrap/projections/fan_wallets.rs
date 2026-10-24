//! The fan wallets: every fan's tickets across concerts, fed by the `Fan` executor.
use super::ReadModel;
use crate::{
    application::{
        fan::views::fan_wallet::FanWalletView,
        projections::maintenance::{HistoryFactory, ProjectionMaintenance},
    },
    domain::fan::Fan,
    infrastructure::mongo::{
        read_model::MongoReadModel,
        read_side::{FAN_WALLETS, WalletRepository, scratch},
    },
};
use cqrsx::postgres::event_repository::PostgresEventRepository;
use mongodb::Database;
use std::sync::Arc;

pub fn wire(read_db: &Database, history: &HistoryFactory<PostgresEventRepository>) -> ReadModel<WalletRepository> {
    let repository = Arc::new(MongoReadModel::new(read_db, FAN_WALLETS));
    ReadModel {
        maintenance: Arc::new(ProjectionMaintenance::<_, _, FanWalletView, Fan>::new(
            FAN_WALLETS,
            history.clone(),
            repository.clone(),
            scratch(read_db, FAN_WALLETS),
        )),
        repository,
    }
}
