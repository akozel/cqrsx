//! Business processes driven by outbox tasks, one folder per process, one file per step.
//!
//! This module holds the one step every chain handler performs: metadata from the task, the
//! follow-up task, the internal `Fan` command, and the mapping of its outcome to a task result
//! (PLAN.md §2.7). R9/R11 mean "already done" (redelivery), R10 means "too early, retry".
pub mod refund;
pub mod ticket_delivery;

use crate::{
    application::{
        command_service::{CommandMetadata, CommandOutcome, FanCommands},
        lock::BoxOfficeLock,
        tasks::new_task,
    },
    domain::{
        ConcertId, FanId,
        fan::{FanCommand, FanError},
    },
};
use cqrsx::task::{attempt::TaskAttempt, format::TaskPayload, handler::TaskHandlerError};
use std::time::Duration;
use uuid::Uuid;

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

/// `correlation_id` from the task metadata, falling back to the task id for hand-made tasks.
pub fn correlation_of(metadata: &serde_json::Value, task_id: Uuid) -> Uuid {
    metadata
        .get("correlation_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .unwrap_or(task_id)
}

/// One link of the aggregate chain: the `Fan` command to run and the task to publish with it.
pub struct ChainStep<'a, F: TaskPayload> {
    pub concert_id: &'a ConcertId,
    pub fan_id: &'a FanId,
    pub command: FanCommand,
    pub follow_up: F,
    pub follow_up_max_attempts: i32,
}

/// Runs one chain step on behalf of a task of type `P`. A malformed payload never reaches this
/// point: the JSON adapter rejects it permanently, ids included. A closed box office waits a
/// little and then asks for a later redelivery without spending a command on it.
pub async fn run_chain_step<P: TaskPayload, F: TaskPayload>(
    fan_commands: &FanCommands,
    attempt: &TaskAttempt,
    step: ChainStep<'_, F>,
) -> Result<(), TaskHandlerError> {
    let metadata = CommandMetadata::from_task(
        correlation_of(attempt.metadata(), attempt.task_id()),
        attempt.task_id(),
        P::TASK_TYPE,
    );
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
    if !wait_for_unlock(fan_commands.lock()).await {
        return Err(locked_retry());
    }
    let executed = fan_commands
        .execute(step.fan_id, step.command, metadata, vec![follow_up])
        .await;
    map_fan_outcome(executed.outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::command_service::Warning;
    use std::sync::Arc;

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
        let task = Uuid::new_v4();
        assert_eq!(
            correlation_of(&serde_json::json!({ "correlation_id": id.to_string() }), task),
            id
        );
        assert_eq!(correlation_of(&serde_json::json!({}), task), task);
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
