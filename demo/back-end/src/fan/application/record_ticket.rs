//! Step 1 of ticket delivery: task `encore_record_ticket` → `Fan.RecordTicket`; the ledger's
//! `TicketRecorded` plans `encore_send_e_ticket` in the same commit ([reactions](super::reactions)).
use crate::{
    fan::domain::{Fan, FanCommand, FanError, commands::RecordTicket},
    shared::domain::{
        ConcertId, FanId, FanLedgerId,
        venue::{SeatId, Zone},
    },
};
use cqrsx::{
    command::CommandGatewayError,
    task::Task,
    worker::{TaskResult, context::TaskContext, task_handler},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `Concert::SeatBooked` → `Fan.RecordTicket`: step one of ticket delivery
/// ([`record_ticket`](super::record_ticket)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "encore_record_ticket", version = 1)]
pub struct RecordTicketV1 {
    pub booking_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub zone: Zone,
    pub price: u32,
    pub fan_id: FanId,
    pub fan_name: String,
}

impl RecordTicketV1 {
    /// The fan's ledger for the concert (D-14), where the ticket goes.
    pub fn ledger(&self) -> FanLedgerId {
        FanLedgerId::new(&self.fan_id, &self.concert_id)
    }

    /// The internal `Fan` command the task asks for.
    pub fn command(self) -> FanCommand {
        FanCommand::RecordTicket(RecordTicket {
            booking_id: self.booking_id,
            concert_id: self.concert_id,
            seat: self.seat,
            zone: self.zone,
            price: self.price,
            fan_name: self.fan_name,
        })
    }
}

/// The ledger records the ticket (PLAN.md §2.7): R9 (already recorded) and R11 (already cancelled) mean "already
/// done", a redelivery; R10 (the ticket is not recorded yet) means "too early, retry"; a conflict and an unavailable
/// ledger are retried. The command continues the task's causation chain.
#[task_handler]
pub async fn record_ticket(task: RecordTicketV1, ctx: &TaskContext) -> TaskResult {
    let ledger = task.ledger();
    match ctx.execute::<Fan>(&ledger, task.command()).await {
        Err(CommandGatewayError::Domain(FanError::AlreadyRecorded { .. } | FanError::AlreadyCancelled { .. })) => {
            TaskResult::done()
        }
        Err(CommandGatewayError::Domain(FanError::UnknownTicket { .. })) => TaskResult::retry("unknown_ticket"),
        other => other.into(), // accepted: done; conflict, unavailable: retry
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fan::{
        application::test_support::{attempt, fans, ledger, steps, ticket},
        domain::{FanEvent, events::TicketRecorded},
    };
    use cqrsx::journal::EventStore;
    use cqrsx_testing::mem_store::MemStore;
    use serde_json::json;

    #[tokio::test]
    async fn records_the_ticket_and_publishes_the_e_ticket_mail_task() {
        let store = MemStore::default();
        let (fans, executor) = fans(store.clone());
        let (payload, correlation_id) = (ticket(), uuid::Uuid::now_v7());
        let delivery = steps(&fans, attempt(&payload, correlation_id));
        assert_eq!(record_ticket.call(payload.clone(), &delivery).await, TaskResult::Done);
        let events = store.load_events(ledger().as_str()).await.unwrap();
        assert!(
            matches!(&events[0].payload, FanEvent::TicketRecorded(TicketRecorded { booking_id, .. }) if *booking_id == payload.booking_id)
        );
        assert_eq!(events[0].metadata["causation_id"], delivery.task_id().to_string());
        assert_eq!(events[0].metadata["source"], "task:encore_record_ticket");
        let tasks = executor.take_published_tasks();
        assert_eq!(tasks.len(), 1, "the ledger's decision planned the e-ticket");
        assert_eq!(tasks[0].format().to_string(), "encore_send_e_ticket v1");
        assert_eq!(
            tasks[0].payload(),
            &json!({ "booking_id": payload.booking_id, "concert_id": "radiohead-2026-11-21", "seat": "B4",
                     "fan_id": "fox", "fan_name": "Fox", "price": 80 })
        );
        assert_eq!(tasks[0].metadata()["correlation_id"], json!(correlation_id));
        assert_eq!(tasks[0].metadata()["command_id"], json!(events[0].metadata["command_id"]));
        assert_eq!(tasks[0].metadata()["causation_id"], json!(delivery.task_id()));
        assert_eq!(tasks[0].max_attempts().get(), 6);
    }

    #[tokio::test]
    async fn a_redelivered_task_is_done_without_a_second_ticket() {
        let store = MemStore::default();
        let (fans, executor) = fans(store.clone());
        let delivery = steps(&fans, attempt(&ticket(), uuid::Uuid::now_v7()));
        assert_eq!(record_ticket.call(ticket(), &delivery).await, TaskResult::Done);
        assert_eq!(record_ticket.call(ticket(), &delivery).await, TaskResult::Done);
        assert_eq!(
            store.load_events(ledger().as_str()).await.unwrap().len(),
            1,
            "R9: already recorded"
        );
        assert_eq!(executor.take_published_tasks().len(), 1, "and no second e-ticket");
    }
}
