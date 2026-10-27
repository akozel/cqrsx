//! The fan ledgers: the `Fan` executor, whose decisions its reactions plan their next tasks from, with the policies of
//! the task types, and whose commits wake the fan wallets' processor; its commands come from the outbox task handlers
//! only.
use super::command_config;
use crate::{
    bootstrap::{FanApp, WiringError},
    fan::domain::Fan,
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
) -> Result<Arc<FanApp>, WiringError> {
    let executor = PostgresCommandExecutor::new(process.pool.clone(), command_config(&process.config)?, ())
        .with_task_planner(ReactionsPlanner::<Fan>::new(policies.clone()))
        .with_commit_wakeup(wakeup.clone());
    Ok(Arc::new(FanApp::new(CommandService::new(executor, activity.clone()))))
}
