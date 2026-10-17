//! `encore_record_cancellation` → `Fan.RecordCancellation` (+ task `encore_issue_refund`).
use crate::{
    app::{
        command_service::{CommandMetadata, FanCommands},
        tasks::{IssueRefundV1, RecordCancellationV1, new_task},
    },
    domain::fan::FanCommand,
    outbox::chain::{correlation_of, map_fan_outcome},
};
use cqrsx::task::{
    attempt::TaskAttempt,
    format::TaskPayload,
    handler::{TaskHandler, TaskHandlerError},
};
use std::sync::Arc;

pub struct RecordCancellationHandler {
    pub fan_commands: Arc<FanCommands>,
    pub task_max_attempts: i32,
}

impl TaskHandler<RecordCancellationV1> for RecordCancellationHandler {
    async fn handle(&mut self, payload: RecordCancellationV1, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        let metadata = CommandMetadata::from_task(
            correlation_of(attempt.metadata(), attempt.task_id()),
            attempt.task_id(),
            RecordCancellationV1::TASK_TYPE,
        );
        let follow_up = new_task(
            IssueRefundV1 {
                cancellation_id: payload.cancellation_id,
                booking_id: payload.booking_id,
                concert_id: payload.concert_id.clone(),
                seat: payload.seat.clone(),
                fan_id: payload.fan_id.clone(),
                amount: payload.refund,
            },
            &metadata,
            &payload.concert_id,
            &payload.fan_id,
            self.task_max_attempts,
        )
        .map_err(|e| TaskHandlerError::Permanent {
            reason: format!("cannot build follow-up task: {e}"),
        })?;
        let command = FanCommand::RecordCancellation {
            booking_id: payload.booking_id,
            cancellation_id: payload.cancellation_id,
            refund: payload.refund,
        };
        let executed = self
            .fan_commands
            .execute(&payload.fan_id, command, metadata, vec![follow_up])
            .await;
        map_fan_outcome(executed.outcome)
    }
}
