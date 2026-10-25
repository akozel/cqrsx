//! The box office: the `Concert` executor, whose commits wake the concert halls' processor; the use cases read
//! the committed history to build their follow-up tasks.
use super::command_config;
use crate::{
    application::{activity::ActivityHub, command_service::CommandService},
    bootstrap::{ConcertApp, WiringError},
    domain::concert::ConcertServices,
    infrastructure::postgres::history_store,
    process::Process,
};
use cqrsx::{cqrs::command::wakeup::CommitWakeup, postgres::command::PostgresCommandExecutor};
use std::sync::Arc;

pub fn wire(process: &Process, wakeup: &CommitWakeup, activity: &Arc<ActivityHub>) -> Result<Arc<ConcertApp>, WiringError> {
    let config = &process.config;
    let executor = PostgresCommandExecutor::new(
        process.pool.clone(),
        command_config(config)?,
        ConcertServices {
            ticket_limit: config.ticket_limit,
        },
    )
    .with_commit_wakeup(wakeup.clone());
    Ok(Arc::new(ConcertApp::new(
        CommandService::new(executor, activity.clone()),
        history_store(process.pool.clone(), config.snapshot_every),
        config.task_max_attempts,
    )))
}
