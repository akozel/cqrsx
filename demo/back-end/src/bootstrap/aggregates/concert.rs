//! The box office: the `Concert` executor, whose decisions its reactions plan their tasks from, with the policies of
//! the task types, and whose commits wake the concert halls' processor.
use super::command_config;
use crate::{
    bootstrap::{ConcertApp, WiringError},
    concert::domain::{Concert, ConcertRules},
    process::Process,
    shared::application::{activity::ActivityHub, command_service::CommandService},
};
use cqrsx::{
    cqrs::{command::wakeup::CommitWakeup, reactions::ReactionsPlanner},
    postgres::command::PostgresCommandExecutor,
    task::policy::Policies,
};
use std::sync::Arc;

pub fn wire(
    process: &Process,
    policies: &Policies,
    wakeup: &CommitWakeup,
    activity: &Arc<ActivityHub>,
) -> Result<Arc<ConcertApp>, WiringError> {
    let config = &process.config;
    let executor = PostgresCommandExecutor::new(
        process.pool.clone(),
        command_config(config)?,
        ConcertRules {
            ticket_limit: config.ticket_limit,
        },
    )
    .with_task_planner(ReactionsPlanner::<Concert>::new(policies.clone()))
    .with_commit_wakeup(wakeup.clone());
    Ok(Arc::new(ConcertApp::new(CommandService::new(executor, activity.clone()))))
}
