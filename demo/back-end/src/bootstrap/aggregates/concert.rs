//! The box office: the `Concert` executor with the rules of the `concert` module, whose reactions plan their tasks with
//! the policies of the task types, whose commits wake the concert halls' processor and whose commands the activity log
//! observes.
use super::command_config;
use crate::{bootstrap::WiringError, concert::domain::Concert, process::Process};
use cqrsx::{
    cqrs::{aggregate::Aggregate, command::wakeup::CommitWakeup, reactions::ReactionsPlanner},
    engine::build::Registrations,
    postgres::command::PostgresCommandExecutor,
};
use std::sync::Arc;

pub fn wire(
    process: &Process,
    registrations: &mut Registrations,
    wakeup: &CommitWakeup,
) -> Result<Arc<PostgresCommandExecutor<Concert>>, WiringError> {
    let rules = registrations
        .take_rules::<Concert>()
        .ok_or(WiringError::NotRegistered(Concert::TYPE))?;
    let mut executor = PostgresCommandExecutor::new(process.pool.clone(), command_config(registrations.settings())?, rules)
        .with_task_planner(ReactionsPlanner::<Concert>::new(registrations.policies().clone()))
        .with_commit_wakeup(wakeup.clone());
    if let Some(observer) = registrations.command_observer::<Concert>() {
        executor = executor.with_observer(observer);
    }
    Ok(Arc::new(executor))
}
