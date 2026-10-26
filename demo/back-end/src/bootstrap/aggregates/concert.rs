//! The box office: the `Concert` executor, whose decisions the workflows plan their tasks from and whose
//! commits wake the concert halls' processor.
use super::command_config;
use crate::{
    application::{activity::ActivityHub, command_service::CommandService, workflows},
    bootstrap::{ConcertApp, WiringError},
    domain::concert::ConcertServices,
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
    .with_task_planner(workflows::concert_tasks(config.task_max_attempts))
    .with_commit_wakeup(wakeup.clone());
    Ok(Arc::new(ConcertApp::new(CommandService::new(executor, activity.clone()))))
}
