//! The fan ledgers: the `Fan` executor with the rules of the `fan` module, whose reactions plan their next tasks with
//! the policies of the task types, whose commits wake the fan wallets' processor and whose commands the activity log
//! observes; its commands come from the outbox task handlers only.
use super::command_config;
use crate::{bootstrap::WiringError, fan::domain::Fan, process::Process};
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
) -> Result<Arc<PostgresCommandExecutor<Fan>>, WiringError> {
    // The ledger's rules are none, but the module must register the aggregate.
    registrations
        .take_rules::<Fan>()
        .ok_or(WiringError::NotRegistered(Fan::TYPE))?;
    let mut executor = PostgresCommandExecutor::new(process.pool.clone(), command_config(registrations.settings())?, ())
        .with_task_planner(ReactionsPlanner::<Fan>::new(registrations.policies().clone()))
        .with_commit_wakeup(wakeup.clone());
    if let Some(observer) = registrations.command_observer::<Fan>() {
        executor = executor.with_observer(observer);
    }
    Ok(Arc::new(executor))
}
