//! `encore_record_ticket` → `Fan.RecordTicket` (+ task `encore_send_e_ticket` in the same commit).
use crate::{
    app::{
        command_service::FanCommands,
        tasks::{RecordTicketV1, SendETicketV1},
    },
    domain::fan::FanCommand,
    outbox::chain::{ChainStep, run_chain_step},
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
        let follow_up = SendETicketV1 {
            booking_id: payload.booking_id,
            concert_id: payload.concert_id.clone(),
            seat: payload.seat.clone(),
            fan_id: payload.fan_id.clone(),
            fan_name: payload.fan_name.clone(),
            price: payload.price,
        };
        let command = FanCommand::RecordTicket {
            booking_id: payload.booking_id,
            concert_id: payload.concert_id.clone(),
            seat: payload.seat,
            zone: payload.zone,
            price: payload.price,
            fan_name: payload.fan_name,
        };
        run_chain_step::<RecordTicketV1, _>(
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
