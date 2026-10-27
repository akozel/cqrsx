//! The fan wallets: every fan's tickets across concerts, fed from the journal of `Fan`.
use super::{ReadModel, processor};
use crate::{
    fan::{
        application::views::fan_wallet::{self, FanWalletView},
        domain::Fan,
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

pub type WalletRepository = MongoReadModel<FanWalletView, Fan>;

pub fn wire(
    process: &Process,
    read_db: &Database,
    checkpoints: &Arc<MongoCheckpointStore>,
    wakeup: &CommitWakeup,
    activity: &Arc<ActivityHub>,
) -> ReadModel<WalletRepository, Fan> {
    let repository = Arc::new(MongoReadModel::new(read_db, fan_wallet::NAME));
    ReadModel {
        verification: Arc::new(ProjectionVerification::<_, _, FanWalletView, Fan>::new(
            fan_wallet::NAME,
            Arc::new(PostgresEventRepository::new(process.history_pool.clone())),
            repository.clone(),
        )),
        processor: processor(process, fan_wallet::NAME, repository.clone(), checkpoints, wakeup, activity),
        repository,
    }
}
