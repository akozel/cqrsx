//! The fan wallets: every fan's tickets across concerts, fed from the journal of `Fan`.
use super::{ReadModel, processor};
use crate::{
    fan::{application::views::fan_wallet::FanWalletView, domain::Fan},
    operations::application::verify::ProjectionVerification,
    process::Process,
    shared::application::activity::ActivityHub,
};
use cqrsx::{
    cqrs::command::wakeup::CommitWakeup,
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
    activity: &Arc<ActivityHub>,
) -> ReadModel<FanWalletView, Fan> {
    let repository = Arc::new(MongoViewRepository::new(read_db));
    ReadModel {
        verification: Arc::new(ProjectionVerification::<_, _, FanWalletView, Fan>::new(
            Arc::new(PostgresEventRepository::new(process.history_pool.clone())),
            repository.clone(),
        )),
        processor: processor(process, repository.clone(), checkpoints, wakeup, activity),
        repository,
    }
}
