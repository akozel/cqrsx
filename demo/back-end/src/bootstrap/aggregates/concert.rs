//! The box office: the `Concert` executor, whose decisions its reactions plan their tasks from and whose
//! commits wake the concert halls' processor.
use super::command_config;
use crate::{
    bootstrap::{ConcertApp, WiringError},
    concert::{application::reactions, domain::ConcertRules},
    process::Process,
    shared::application::{activity::ActivityHub, command_service::CommandService},
};
use cqrsx::{cqrs::command::wakeup::CommitWakeup, postgres::command::PostgresCommandExecutor};
use std::sync::Arc;

pub fn wire(process: &Process, wakeup: &CommitWakeup, activity: &Arc<ActivityHub>) -> Result<Arc<ConcertApp>, WiringError> {
    let config = &process.config;
    let executor = PostgresCommandExecutor::new(
        process.pool.clone(),
        command_config(config)?,
        ConcertRules {
            ticket_limit: config.ticket_limit,
        },
    )
    .with_task_planner(reactions::concert_tasks(config.task_max_attempts))
    .with_commit_wakeup(wakeup.clone());
    Ok(Arc::new(ConcertApp::new(CommandService::new(executor, activity.clone()))))
}
