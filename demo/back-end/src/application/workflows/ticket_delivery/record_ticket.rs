//! Step 1 of ticket delivery: task `encore_record_ticket` → `Fan.RecordTicket`; the ledger's
//! `TicketRecorded` plans `encore_send_e_ticket` in the same commit ([`TicketDelivery`](super::TicketDelivery)).
use crate::{
    application::{
        fan::FanApp,
        workflows::{ChainStep, run_chain_step, ticket_delivery::tasks::RecordTicketV1},
    },
    domain::fan::{Fan, FanCommand, commands::RecordTicket},
};
use cqrsx::{
    cqrs::command::CommandExecutor,
    task::{
        attempt::TaskAttempt,
        handler::{TaskHandler, TaskHandlerError},
    },
};
use std::sync::Arc;

pub struct RecordTicketHandler<E> {
    pub fans: Arc<FanApp<E>>,
}

impl<E: CommandExecutor<Fan>> TaskHandler<RecordTicketV1> for RecordTicketHandler<E> {
    async fn handle(&mut self, payload: RecordTicketV1, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        let command = FanCommand::RecordTicket(RecordTicket {
            booking_id: payload.booking_id,
            concert_id: payload.concert_id.clone(),
            seat: payload.seat,
            zone: payload.zone,
            price: payload.price,
            fan_name: payload.fan_name,
        });
        run_chain_step(
            &self.fans,
            attempt,
            ChainStep {
                concert_id: &payload.concert_id,
                fan_id: &payload.fan_id,
                command,
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        application::workflows::test_support::{attempt, fans, ledger, ticket},
        domain::fan::{FanEvent, events::TicketRecorded},
    };
    use cqrsx::cqrs::{store::EventStore, testing::mem_store::MemStore};
    use serde_json::json;

    #[tokio::test]
    async fn records_the_ticket_and_publishes_the_e_ticket_mail_task() {
        let store = MemStore::default();
        let (fans, executor) = fans(store.clone());
        let mut handler = RecordTicketHandler { fans };
        let (payload, correlation_id) = (ticket(), uuid::Uuid::now_v7());
        let delivery = attempt(&payload, correlation_id);
        handler.handle(payload.clone(), &delivery).await.unwrap();
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
        assert_eq!(tasks[0].max_attempts(), 6);
    }

    #[tokio::test]
    async fn a_redelivered_task_is_done_without_a_second_ticket() {
        let store = MemStore::default();
        let (fans, executor) = fans(store.clone());
        let mut handler = RecordTicketHandler { fans };
        let delivery = attempt(&ticket(), uuid::Uuid::now_v7());
        handler.handle(ticket(), &delivery).await.unwrap();
        handler.handle(ticket(), &delivery).await.unwrap();
        assert_eq!(
            store.load_events(ledger().as_str()).await.unwrap().len(),
            1,
            "R9: already recorded"
        );
        assert_eq!(executor.take_published_tasks().len(), 1, "and no second e-ticket");
    }
}
