//! The fan ledgers: the `Fan` executor feeds the fan wallets read model; its commands come from the
//! outbox task handlers only.
use super::command_config;
use crate::{
    application::{activity::ActivityHub, command_service::CommandService, lock::BoxOfficeLock},
    bootstrap::{
        FanApp, WiringError,
        projections::{self, ReadSideWiring},
    },
    infrastructure::mongo::read_side::FAN_WALLETS,
    process::Process,
};
use cqrsx::{cqrs::event_handler::delivery::DeliveryBacklog, postgres::command::PostgresCommandExecutor};
use std::sync::Arc;

pub fn wire(
    process: &Process,
    read: &ReadSideWiring,
    activity: &Arc<ActivityHub>,
    lock: &Arc<BoxOfficeLock>,
) -> Result<(Arc<FanApp>, DeliveryBacklog), WiringError> {
    let config = &process.config;
    let executor = PostgresCommandExecutor::new(process.pool.clone(), command_config(config)?, ()).with_event_handler(
        projections::handler(read.wallets.clone(), FAN_WALLETS, activity.clone()),
        config.projection_delivery,
    );
    let backlog = executor.backlog();
    let app = FanApp::new(CommandService::new(executor, activity.clone(), lock.clone()));
    Ok((Arc::new(app), backlog))
}
