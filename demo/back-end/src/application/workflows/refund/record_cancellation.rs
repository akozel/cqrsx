//! `encore_record_cancellation` → `Fan.RecordCancellation` (+ task `encore_issue_refund`).
use crate::{
    application::{
        command_service::FanCommands,
        tasks::{IssueRefundV1, RecordCancellationV1},
        workflows::{ChainStep, run_chain_step},
    },
    domain::fan::FanCommand,
};
use cqrsx::task::{
    attempt::TaskAttempt,
    handler::{TaskHandler, TaskHandlerError},
};
use std::sync::Arc;

pub struct RecordCancellationHandler {
    pub fan_commands: Arc<FanCommands>,
    pub task_max_attempts: i32,
}

impl TaskHandler<RecordCancellationV1> for RecordCancellationHandler {
    async fn handle(&mut self, payload: RecordCancellationV1, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        let follow_up = IssueRefundV1 {
            cancellation_id: payload.cancellation_id,
            booking_id: payload.booking_id,
            concert_id: payload.concert_id.clone(),
            seat: payload.seat.clone(),
            fan_id: payload.fan_id.clone(),
            amount: payload.refund,
        };
        let command = FanCommand::RecordCancellation {
            booking_id: payload.booking_id,
            cancellation_id: payload.cancellation_id,
            refund: payload.refund,
        };
        run_chain_step::<RecordCancellationV1, _>(
            &self.fan_commands,
            attempt,
            ChainStep {
                concert_id: &payload.concert_id,
                fan_id: &payload.fan_id,
                command,
                follow_up,
                follow_up_max_attempts: self.task_max_attempts,
            },
        )
        .await
    }
}
