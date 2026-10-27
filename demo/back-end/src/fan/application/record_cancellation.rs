//! Step 1 of refund: task `encore_record_cancellation` → `Fan.RecordCancellation`; the ledger's
//! `TicketCancelled` plans `encore_issue_refund` in the same commit ([reactions](super::reactions)).
use crate::{
    fan::{
        application::{
            app::FanApp,
            chain::{ChainStep, run_chain_step},
        },
        domain::{Fan, FanCommand, commands::RecordCancellation},
    },
    shared::domain::{ConcertId, FanId, venue::SeatId},
};
use cqrsx::{
    Task,
    cqrs::command::CommandExecutor,
    task::{
        attempt::TaskAttempt,
        handler::{TaskHandler, TaskHandlerError},
    },
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

/// `Concert::BookingCancelled` → `Fan.RecordCancellation`: step one of the refund
/// ([`record_cancellation`](super::record_cancellation)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "encore_record_cancellation", version = 1)]
pub struct RecordCancellationV1 {
    pub booking_id: Uuid,
    pub cancellation_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub fan_id: FanId,
    pub refund: u32,
}

pub struct RecordCancellationHandler<E> {
    pub fans: Arc<FanApp<E>>,
}

impl<E: CommandExecutor<Fan>> TaskHandler<RecordCancellationV1> for RecordCancellationHandler<E> {
    async fn handle(&mut self, payload: RecordCancellationV1, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        let command = FanCommand::RecordCancellation(RecordCancellation {
            booking_id: payload.booking_id,
            cancellation_id: payload.cancellation_id,
            refund: payload.refund,
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
    use crate::fan::application::{
        chain::CHAIN_RETRY_AFTER,
        record_ticket::RecordTicketHandler,
        test_support::{attempt, fans, ledger, ticket},
    };
    use cqrsx::cqrs::{store::EventStore, testing::mem_store::MemStore};
    use uuid::Uuid;

    fn cancellation(booking_id: Uuid) -> RecordCancellationV1 {
        let ticket = ticket();
        RecordCancellationV1 {
            booking_id,
            cancellation_id: Uuid::now_v7(),
            concert_id: ticket.concert_id,
            seat: ticket.seat,
            fan_id: ticket.fan_id,
            refund: ticket.price,
        }
    }

    #[tokio::test]
    async fn a_cancellation_that_overtook_its_ticket_is_retried_later() {
        let (fans, executor) = fans(MemStore::default());
        let mut handler = RecordCancellationHandler { fans };
        let payload = cancellation(ticket().booking_id);
        let result = handler.handle(payload.clone(), &attempt(&payload, Uuid::now_v7())).await;
        match result {
            Err(TaskHandlerError::Retry { reason, retry_after }) => {
                assert_eq!((reason.as_str(), retry_after), ("unknown_ticket", Some(CHAIN_RETRY_AFTER)));
            }
            other => panic!("expected a retry (R10), got {other:?}"),
        }
        assert!(
            executor.take_published_tasks().is_empty(),
            "a rejected command decided nothing, so nothing was planned or published"
        );
    }

    #[tokio::test]
    async fn a_recorded_ticket_is_cancelled_and_the_refund_mail_task_published() {
        let store = MemStore::default();
        let (fans, executor) = fans(store.clone());
        let ticket = ticket();
        RecordTicketHandler { fans: fans.clone() }
            .handle(ticket.clone(), &attempt(&ticket, Uuid::now_v7()))
            .await
            .unwrap();
        executor.take_published_tasks();
        let mut handler = RecordCancellationHandler { fans };
        let payload = cancellation(ticket.booking_id);
        let delivery = attempt(&payload, Uuid::now_v7());
        handler.handle(payload.clone(), &delivery).await.unwrap();
        assert_eq!(store.load_events(ledger().as_str()).await.unwrap().len(), 2);
        let refunds = executor.take_published_tasks();
        assert_eq!(
            refunds.iter().map(|task| task.format().to_string()).collect::<Vec<_>>(),
            ["encore_issue_refund v1"]
        );
        assert_eq!(
            refunds[0].payload()["cancellation_id"],
            serde_json::json!(payload.cancellation_id)
        );
        assert_eq!(refunds[0].payload()["amount"], 80);
        handler.handle(payload, &delivery).await.unwrap();
        assert_eq!(
            store.load_events(ledger().as_str()).await.unwrap().len(),
            2,
            "R11: already cancelled"
        );
        assert!(executor.take_published_tasks().is_empty(), "and no second refund");
    }
}
