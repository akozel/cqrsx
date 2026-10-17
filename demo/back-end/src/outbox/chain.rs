//! Shared mapping for the two chain handlers: outcome of an internal `Fan` command → task result
//! (PLAN.md §2.7). R9/R11 mean "already done" (redelivery), R10 means "too early, retry".
use crate::{app::command_service::CommandOutcome, domain::fan::FanError};
use cqrsx::task::handler::TaskHandlerError;
use std::time::Duration;
use uuid::Uuid;

pub const CHAIN_RETRY_AFTER: Duration = Duration::from_secs(1);

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
        CommandOutcome::Locked => retry("rebuild_in_progress"),
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
        for (outcome, reason) in [
            (
                CommandOutcome::Rejected(FanError::UnknownTicket { booking_id: id }),
                "unknown_ticket",
            ),
            (CommandOutcome::Conflict, "conflict"),
            (CommandOutcome::Unavailable, "unavailable"),
            (CommandOutcome::Locked, "rebuild_in_progress"),
        ] {
            match map_fan_outcome(outcome) {
                Err(TaskHandlerError::Retry { reason: r, retry_after }) => {
                    assert_eq!(r, reason);
                    assert_eq!(retry_after, Some(CHAIN_RETRY_AFTER));
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
}
