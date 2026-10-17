//! Step 1 of refund: task `encore_record_cancellation` → `Fan.RecordCancellation`, publishing
//! `encore_issue_refund` in the same commit.
use crate::{
    application::{
        concert::tasks::RecordCancellationV1,
        fan::FanApp,
        workflows::{ChainStep, refund::tasks::IssueRefundV1, run_chain_step},
    },
    domain::fan::{Fan, FanCommand, commands::RecordCancellation},
};
use cqrsx::{
    cqrs::command::CommandExecutor,
    task::{
        attempt::TaskAttempt,
        handler::{TaskHandler, TaskHandlerError},
    },
};
use std::sync::Arc;

pub struct RecordCancellationHandler<E> {
    pub fans: Arc<FanApp<E>>,
    /// Attempt budget of the e-mail task this step publishes.
    pub follow_up_max_attempts: i32,
}

impl<E: CommandExecutor<Fan>> TaskHandler<RecordCancellationV1> for RecordCancellationHandler<E> {
    async fn handle(&mut self, payload: RecordCancellationV1, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        let follow_up = IssueRefundV1 {
            cancellation_id: payload.cancellation_id,
            booking_id: payload.booking_id,
            concert_id: payload.concert_id.clone(),
            seat: payload.seat.clone(),
            fan_id: payload.fan_id.clone(),
            amount: payload.refund,
        };
        let command = FanCommand::RecordCancellation(RecordCancellation {
            booking_id: payload.booking_id,
            cancellation_id: payload.cancellation_id,
            refund: payload.refund,
        });
        run_chain_step::<RecordCancellationV1, _, _>(
            &self.fans,
            attempt,
            ChainStep {
                concert_id: &payload.concert_id,
                fan_id: &payload.fan_id,
                command,
                follow_up,
                follow_up_max_attempts: self.follow_up_max_attempts,
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{
        concert::tasks::RecordTicketV1,
        workflows::{
            CHAIN_RETRY_AFTER,
            test_support::{attempt, fans, ticket},
            ticket_delivery::record_ticket::RecordTicketHandler,
        },
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
        let (fans, bound) = fans(MemStore::default());
        let mut handler = RecordCancellationHandler {
            fans,
            follow_up_max_attempts: 6,
        };
        let result = handler
            .handle(
                cancellation(ticket().booking_id),
                &attempt::<RecordCancellationV1>(Uuid::now_v7()),
            )
            .await;
        match result {
            Err(TaskHandlerError::Retry { reason, retry_after }) => {
                assert_eq!((reason.as_str(), retry_after), ("unknown_ticket", Some(CHAIN_RETRY_AFTER)));
            }
            other => panic!("expected a retry (R10), got {other:?}"),
        }
        assert_eq!(
            bound.take().len(),
            1,
            "the follow-up was bound but never published: nothing was committed"
        );
    }

    #[tokio::test]
    async fn a_recorded_ticket_is_cancelled_and_the_refund_mail_task_published() {
        let store = MemStore::default();
        let (fans, bound) = fans(store.clone());
        let ticket = ticket();
        RecordTicketHandler {
            fans: fans.clone(),
            follow_up_max_attempts: 6,
        }
        .handle(ticket.clone(), &attempt::<RecordTicketV1>(Uuid::now_v7()))
        .await
        .unwrap();
        bound.take();
        let mut handler = RecordCancellationHandler {
            fans,
            follow_up_max_attempts: 6,
        };
        let delivery = attempt::<RecordCancellationV1>(Uuid::now_v7());
        handler.handle(cancellation(ticket.booking_id), &delivery).await.unwrap();
        assert_eq!(store.load_events("fox").await.unwrap().len(), 2);
        assert_eq!(bound.take()[0].format().to_string(), "encore_issue_refund v1");
        handler.handle(cancellation(ticket.booking_id), &delivery).await.unwrap();
        assert_eq!(store.load_events("fox").await.unwrap().len(), 2, "R11: already cancelled");
    }
}
