//! The fan wallets: every fan's tickets across concerts, fed from the journal of `Fan`.
use super::{ReadModel, processor};
use crate::{
    application::{
        activity::ActivityHub, fan::views::fan_wallet::FanWalletView, projections::verification::ProjectionVerification,
    },
    domain::fan::Fan,
    infrastructure::mongo::{
        read_model::MongoReadModel,
        read_side::{FAN_WALLETS, WalletRepository},
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
) -> ReadModel<WalletRepository, Fan> {
    let repository = Arc::new(MongoReadModel::new(read_db, FAN_WALLETS));
    ReadModel {
        verification: Arc::new(ProjectionVerification::<_, _, FanWalletView, Fan>::new(
            FAN_WALLETS,
            Arc::new(PostgresEventRepository::new(process.history_pool.clone())),
            repository.clone(),
        )),
        processor: processor(process, FAN_WALLETS, repository.clone(), checkpoints, wakeup, activity),
        repository,
    }
}
