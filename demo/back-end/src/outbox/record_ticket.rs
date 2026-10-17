//! `encore_record_ticket` → `Fan.RecordTicket` (+ task `encore_send_e_ticket` in the same commit).
use crate::{
    app::{
        command_service::{CommandMetadata, FanCommands},
        tasks::{RecordTicketV1, SendETicketV1, new_task},
    },
    domain::fan::FanCommand,
    outbox::chain::{correlation_of, map_fan_outcome, wait_for_unlock},
};
use cqrsx::task::{
    attempt::TaskAttempt,
    handler::{TaskHandler, TaskHandlerError},
};
use std::sync::Arc;

pub struct RecordTicketHandler {
    pub fan_commands: Arc<FanCommands>,
    pub task_max_attempts: i32,
}

impl TaskHandler<RecordTicketV1> for RecordTicketHandler {
    async fn handle(&mut self, payload: RecordTicketV1, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        let metadata = CommandMetadata::from_task(
            correlation_of(attempt.metadata(), attempt.task_id()),
            attempt.task_id(),
            RecordTicketV1::TASK_TYPE,
        );
        let follow_up = new_task(
            SendETicketV1 {
                booking_id: payload.booking_id,
                concert_id: payload.concert_id.clone(),
                seat: payload.seat.clone(),
                fan_id: payload.fan_id.clone(),
                fan_name: payload.fan_name.clone(),
                price: payload.price,
            },
            &metadata,
            &payload.concert_id,
            &payload.fan_id,
            self.task_max_attempts,
        )
        .map_err(|e| TaskHandlerError::Permanent {
            reason: format!("cannot build follow-up task: {e}"),
        })?;
        let command = FanCommand::RecordTicket {
            booking_id: payload.booking_id,
            concert_id: payload.concert_id,
            seat: payload.seat,
            zone: payload.zone,
            price: payload.price,
            fan_name: payload.fan_name,
        };
        // Box office closed for a rebuild? Wait a little before spending a claim attempt.
        wait_for_unlock(self.fan_commands.rebuild()).await;
        let executed = self
            .fan_commands
            .execute(&payload.fan_id, command, metadata, vec![follow_up])
            .await;
        map_fan_outcome(executed.outcome)
    }
}

use cqrsx::task::format::TaskPayload;
