//! The fan ledgers: the `Fan` executor, whose decisions the workflows plan their next tasks from and whose
//! commits wake the fan wallets' processor; its commands come from the outbox task handlers only.
use super::command_config;
use crate::{
    application::{activity::ActivityHub, command_service::CommandService, workflows},
    bootstrap::{FanApp, WiringError},
    process::Process,
};
use cqrsx::{cqrs::command::wakeup::CommitWakeup, postgres::command::PostgresCommandExecutor};
use std::sync::Arc;

pub fn wire(process: &Process, wakeup: &CommitWakeup, activity: &Arc<ActivityHub>) -> Result<Arc<FanApp>, WiringError> {
    let executor = PostgresCommandExecutor::new(process.pool.clone(), command_config(&process.config)?, ())
        .with_task_planner(workflows::fan_tasks(process.config.task_max_attempts))
        .with_commit_wakeup(wakeup.clone());
    Ok(Arc::new(FanApp::new(CommandService::new(executor, activity.clone()))))
}
