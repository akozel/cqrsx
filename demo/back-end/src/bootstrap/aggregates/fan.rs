//! The fan ledgers: the `Fan` executor, whose decisions its reactions plan their next tasks from, with the policies of
//! the task types, whose commits wake the fan wallets' processor and whose commands the activity feed reports; its
//! commands come from the outbox task handlers only.
use super::command_config;
use crate::{
    bootstrap::WiringError, fan::domain::Fan, operations::application::activity_observer::CommandActivityObserver,
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
) -> Result<Arc<PostgresCommandExecutor<Fan>>, WiringError> {
    let executor = PostgresCommandExecutor::new(process.pool.clone(), command_config(&process.config)?, ())
        .with_task_planner(ReactionsPlanner::<Fan>::new(policies.clone()))
        .with_commit_wakeup(wakeup.clone())
        .with_observer(observer.clone());
    Ok(Arc::new(executor))
}
