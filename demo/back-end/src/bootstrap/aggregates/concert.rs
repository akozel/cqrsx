//! The box office: the `Concert` executor feeds the concert halls read model; the use cases read the
//! committed history to build their follow-up tasks.
use super::command_config;
use crate::{
    application::{activity::ActivityHub, command_service::CommandService, lock::BoxOfficeLock},
    bootstrap::{
        ConcertApp, WiringError,
        projections::{self, ReadSideWiring},
    },
    domain::concert::ConcertServices,
    infrastructure::{mongo::read_side::CONCERT_HALLS, postgres::history_store},
    process::Process,
};
use cqrsx::{cqrs::event_handler::delivery::DeliveryBacklog, postgres::command::PostgresCommandExecutor};
use std::sync::Arc;

pub fn wire(
    process: &Process,
    read: &ReadSideWiring,
    activity: &Arc<ActivityHub>,
    lock: &Arc<BoxOfficeLock>,
) -> Result<(Arc<ConcertApp>, DeliveryBacklog), WiringError> {
    let config = &process.config;
    let executor = PostgresCommandExecutor::new(
        process.pool.clone(),
        command_config(config)?,
        ConcertServices {
            ticket_limit: config.ticket_limit,
        },
    )
    .with_event_handler(
        projections::handler(read.halls.clone(), CONCERT_HALLS, activity.clone()),
        config.projection_delivery,
    );
    let backlog = executor.backlog();
    let app = ConcertApp::new(
        CommandService::new(executor, activity.clone(), lock.clone()),
        history_store(process.pool.clone(), config.snapshot_every),
        config.chain_max_attempts,
    );
    Ok((Arc::new(app), backlog))
}
