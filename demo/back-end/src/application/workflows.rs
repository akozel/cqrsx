//! Business processes driven by outbox tasks: one folder per process, one file per step, the
//! task contracts each process publishes in its `tasks` module.
//!
//! - [`ticket_delivery`]: a booked seat reaches the fan's ledger, then the fan gets an e-ticket;
//! - [`refund`]: a cancelled booking reaches the fan's ledger, then the fan gets a refund.
//!
//! This module holds the one step every ledger step performs: metadata from the task, the
//! follow-up task, the internal `Fan` command, and the mapping of its outcome to a task result
//! (PLAN.md §2.7). R9/R11 mean "already done" (redelivery), R10 means "too early, retry".
pub mod refund;
#[cfg(test)]
mod test_support;
pub mod ticket_delivery;

use crate::{
    application::{
        command_service::{CommandMetadata, CommandOutcome},
        fan::FanApp,
        lock::BoxOfficeLock,
        tasks::new_task,
    },
    domain::{
        ConcertId, FanId, FanLedgerId,
        fan::{Fan, FanCommand, FanError},
    },
};
use cqrsx::{
    cqrs::command::CommandExecutor,
    task::{attempt::TaskAttempt, format::TaskPayload, handler::TaskHandlerError},
};
use std::time::Duration;

pub const CHAIN_RETRY_AFTER: Duration = Duration::from_secs(1);
/// While the box office is closed (rebuild) a retry costs one claim attempt, so wait a little
/// in-process first (well inside the 5 s handler timeout) and come back later rather than sooner.
pub const LOCK_WAIT: Duration = Duration::from_secs(3);
pub const LOCKED_RETRY_AFTER: Duration = Duration::from_secs(2);

/// Waits up to `LOCK_WAIT` for a running rebuild to finish. Returns `true` when the lock is free.
pub async fn wait_for_unlock(lock: &BoxOfficeLock) -> bool {
    let deadline = tokio::time::Instant::now() + LOCK_WAIT;
    while lock.is_locked() {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    true
}

fn locked_retry() -> TaskHandlerError {
    TaskHandlerError::Retry {
        reason: "rebuild_in_progress".to_owned(),
        retry_after: Some(LOCKED_RETRY_AFTER),
    }
}

pub fn map_fan_outcome(outcome: CommandOutcome<FanError>) -> Result<(), TaskHandlerError> {
    let retry = |reason: &str| {
        Err(TaskHandlerError::Retry {
            reason: reason.to_owned(),
            retry_after: Some(CHAIN_RETRY_AFTER),
        })
    };
    match outcome {
        // A commit with a failed event handler is still a commit: the read model catches up on Encore!.
        CommandOutcome::Accepted { .. } => Ok(()),
        CommandOutcome::Rejected(FanError::AlreadyRecorded { .. } | FanError::AlreadyCancelled { .. }) => Ok(()),
        CommandOutcome::Rejected(FanError::UnknownTicket { .. }) => retry("unknown_ticket"),
        CommandOutcome::Conflict => retry("conflict"),
        CommandOutcome::Unavailable => retry("unavailable"),
        CommandOutcome::Locked => Err(locked_retry()),
    }
}

/// One link of the aggregate chain: the `Fan` command to run and the task to publish with it.
pub struct ChainStep<'a, F: TaskPayload> {
    pub concert_id: &'a ConcertId,
    pub fan_id: &'a FanId,
    pub command: FanCommand,
    pub follow_up: F,
    pub follow_up_max_attempts: i32,
}

/// Runs one chain step on behalf of `attempt`: the command continues the task's causation chain
/// (cqrsx `Causation::from_task`) and runs on the fan's ledger for the concert (D-14). A malformed
/// payload never reaches this point: the JSON adapter rejects it permanently, ids included. A closed
/// box office waits a little and then asks for a later redelivery without spending a command on it.
pub async fn run_chain_step<F: TaskPayload, E: CommandExecutor<Fan>>(
    fans: &FanApp<E>,
    attempt: &TaskAttempt,
    step: ChainStep<'_, F>,
) -> Result<(), TaskHandlerError> {
    let metadata = CommandMetadata::from_task(attempt);
    let follow_up = new_task(
        step.follow_up,
        &metadata,
        step.concert_id,
        step.fan_id,
        step.follow_up_max_attempts,
    )
    .map_err(|e| TaskHandlerError::Permanent {
        reason: format!("cannot build follow-up task: {e}"),
    })?;
    if !wait_for_unlock(fans.lock()).await {
        return Err(locked_retry());
    }
    let ledger = FanLedgerId::new(step.fan_id, step.concert_id);
    let executed = fans.execute(&ledger, step.command, metadata, vec![follow_up]).await;
    map_fan_outcome(executed.outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command_service::Warning;
    use std::sync::Arc;
    use uuid::Uuid;

    #[test]
    fn outcomes_map_to_task_results_as_in_the_plan_table() {
        let id = Uuid::new_v4();
        assert!(map_fan_outcome(CommandOutcome::Accepted { warnings: vec![] }).is_ok());
        assert!(
            map_fan_outcome(CommandOutcome::Accepted {
                warnings: vec![Warning {
                    code: "projection_failed",
                    projection: "fan_wallets".into()
                }]
            })
            .is_ok()
        );
        assert!(map_fan_outcome(CommandOutcome::Rejected(FanError::AlreadyRecorded { booking_id: id })).is_ok());
        assert!(map_fan_outcome(CommandOutcome::Rejected(FanError::AlreadyCancelled { booking_id: id })).is_ok());
        for (outcome, reason, after) in [
            (
                CommandOutcome::Rejected(FanError::UnknownTicket { booking_id: id }),
                "unknown_ticket",
                CHAIN_RETRY_AFTER,
            ),
            (CommandOutcome::Conflict, "conflict", CHAIN_RETRY_AFTER),
            (CommandOutcome::Unavailable, "unavailable", CHAIN_RETRY_AFTER),
            (CommandOutcome::Locked, "rebuild_in_progress", LOCKED_RETRY_AFTER),
        ] {
            match map_fan_outcome(outcome) {
                Err(TaskHandlerError::Retry { reason: r, retry_after }) => {
                    assert_eq!(r, reason);
                    assert_eq!(retry_after, Some(after));
                }
                other => panic!("expected retry for {reason}, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn an_open_box_office_needs_no_wait() {
        assert!(wait_for_unlock(&BoxOfficeLock::new()).await);
    }

    #[tokio::test(start_paused = true)]
    async fn a_closed_box_office_is_waited_for_and_then_given_up_on() {
        let lock = Arc::new(BoxOfficeLock::new());
        let _guard = lock.try_acquire().unwrap();
        let started = tokio::time::Instant::now();
        assert!(!wait_for_unlock(&lock).await);
        assert!(started.elapsed() >= LOCK_WAIT);
    }
}
