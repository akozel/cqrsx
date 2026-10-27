//! The box office: the `Concert` executor, whose decisions its reactions plan their tasks from, with the policies of
//! the task types, whose commits wake the concert halls' processor and whose commands the activity feed reports.
use super::command_config;
use crate::{
    bootstrap::WiringError,
    concert::domain::{Concert, ConcertRules},
    operations::application::activity_observer::CommandActivityObserver,
    process::Process,
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
    observer: &Arc<CommandActivityObserver>,
) -> Result<Arc<PostgresCommandExecutor<Concert>>, WiringError> {
    let config = &process.config;
    let executor = PostgresCommandExecutor::new(
        process.pool.clone(),
        command_config(config)?,
        ConcertRules {
            ticket_limit: config.ticket_limit,
        },
    )
    .with_task_planner(ReactionsPlanner::<Concert>::new(policies.clone()))
    .with_commit_wakeup(wakeup.clone())
    .with_observer(observer.clone());
    Ok(Arc::new(executor))
}
