//! Shared mapping for the two chain handlers: outcome of an internal `Fan` command → task result
//! (PLAN.md §2.7). R9/R11 mean "already done" (redelivery), R10 means "too early, retry".
use crate::{
    app::{command_service::CommandOutcome, rebuild::RebuildService},
    domain::fan::FanError,
};
use cqrsx::task::handler::TaskHandlerError;
use std::time::Duration;
use uuid::Uuid;

pub const CHAIN_RETRY_AFTER: Duration = Duration::from_secs(1);
/// While the box office is closed (rebuild) a retry costs one claim attempt, so wait a little
/// in-process first (well inside the 5 s handler timeout) and come back later rather than sooner.
pub const LOCK_WAIT: Duration = Duration::from_secs(3);
pub const LOCKED_RETRY_AFTER: Duration = Duration::from_secs(2);

/// Waits up to `LOCK_WAIT` for a running rebuild to finish. Returns `true` when the lock is free.
pub async fn wait_for_unlock(rebuild: &RebuildService) -> bool {
    let deadline = tokio::time::Instant::now() + LOCK_WAIT;
    while rebuild.is_locked() {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    true
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
        CommandOutcome::Locked => Err(TaskHandlerError::Retry {
            reason: "rebuild_in_progress".to_owned(),
            retry_after: Some(LOCKED_RETRY_AFTER),
        }),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::command_service::Warning;

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
    async fn unlocked_service_needs_no_wait() {
        let service = RebuildService::lock_only();
        assert!(wait_for_unlock(&service).await);
    }
}
