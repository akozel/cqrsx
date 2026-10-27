//! Step 1 of refund: task `encore_record_cancellation` → `Fan.RecordCancellation`; the ledger's
//! `TicketCancelled` plans `encore_issue_refund` in the same commit ([reactions](super::reactions)).
use crate::{
    fan::domain::{Fan, FanCommand, FanError, commands::RecordCancellation},
    shared::domain::{ConcertId, FanId, FanLedgerId, venue::SeatId},
};
use cqrsx::{
    Task,
    cqrs::command::CommandGatewayError,
    task::{context::TaskContext, result::TaskResult},
    task_handler,
};
use serde::{Deserialize, Serialize};
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

impl RecordCancellationV1 {
    /// The fan's ledger for the concert (D-14), where the cancellation goes.
    pub fn ledger(&self) -> FanLedgerId {
        FanLedgerId::new(&self.fan_id, &self.concert_id)
    }

    /// The internal `Fan` command the task asks for.
    pub fn command(self) -> FanCommand {
        FanCommand::RecordCancellation(RecordCancellation {
            booking_id: self.booking_id,
            cancellation_id: self.cancellation_id,
            refund: self.refund,
        })
    }
}

/// The ledger records the cancellation (PLAN.md §2.7): R9 (already recorded) and R11 (already cancelled) mean
/// "already done", a redelivery; R10 (the cancellation overtook its ticket) means "too early, retry"; a conflict and
/// an unavailable ledger are retried. The command continues the task's causation chain.
#[task_handler]
pub async fn record_cancellation(task: RecordCancellationV1, ctx: &TaskContext) -> TaskResult {
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
    use crate::fan::application::{
        record_ticket::record_ticket,
        test_support::{attempt, fans, ledger, steps, ticket},
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
        let payload = cancellation(ticket().booking_id);
        let result = record_cancellation
            .call(payload.clone(), &steps(&fans, attempt(&payload, Uuid::now_v7())))
            .await;
        match result {
            TaskResult::Retry { reason } => {
                assert_eq!(reason.as_str(), "unknown_ticket");
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
        assert_eq!(
            record_ticket
                .call(ticket.clone(), &steps(&fans, attempt(&ticket, Uuid::now_v7())))
                .await,
            TaskResult::Done
        );
        executor.take_published_tasks();
        let payload = cancellation(ticket.booking_id);
        let delivery = steps(&fans, attempt(&payload, Uuid::now_v7()));
        assert_eq!(record_cancellation.call(payload.clone(), &delivery).await, TaskResult::Done);
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
        assert_eq!(record_cancellation.call(payload, &delivery).await, TaskResult::Done);
        assert_eq!(
            store.load_events(ledger().as_str()).await.unwrap().len(),
            2,
            "R11: already cancelled"
        );
        assert!(executor.take_published_tasks().is_empty(), "and no second refund");
    }
}
