//! Step 2 of refund: task `encore_issue_refund` → e-mail "💸 Refund".
use crate::{
    fan::domain::events::TicketCancelled,
    shared::{
        application::{
            mail::{Mail, Mailer},
            tasks::new_task,
        },
        domain::{ConcertId, FanId, FanLedgerId, venue::SeatId},
    },
};
use cqrsx::task::{
    NewTask, TaskValidationError,
    attempt::TaskAttempt,
    format::TaskPayload,
    handler::{TaskHandler, TaskHandlerError},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `Fan::TicketCancelled` → e-mail "💸 Refund" (step [`issue_refund`](super::issue_refund)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IssueRefundV1 {
    pub cancellation_id: Uuid,
    pub booking_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub fan_id: FanId,
    pub amount: u32,
}

impl TaskPayload for IssueRefundV1 {
    const TASK_TYPE: &'static str = "encore_issue_refund";
    const TASK_VERSION: i32 = 1;
}

/// The refund of a ticket the ledger cancelled; the fan is the ledger's.
pub fn issue_refund(
    ledger: &FanLedgerId,
    cancelled: &TicketCancelled,
    max_attempts: i32,
) -> Result<NewTask, TaskValidationError> {
    let fan_id = ledger.fan();
    let payload = IssueRefundV1 {
        cancellation_id: cancelled.cancellation_id,
        booking_id: cancelled.booking_id,
        concert_id: cancelled.concert_id.clone(),
        seat: cancelled.seat.clone(),
        fan_id: fan_id.clone(),
        amount: cancelled.refund,
    };
    new_task(payload, &cancelled.concert_id, &fan_id, max_attempts)
}

pub struct IssueRefundHandler<M> {
    pub mailer: M,
}

/// One refund per cancellation: the cancellation id is the mail's idempotency key.
pub fn refund_mail(payload: &IssueRefundV1) -> Mail {
    Mail {
        id: format!("refund:{}", payload.cancellation_id),
        kind: "refund",
        to_fan_id: payload.fan_id.to_string(),
        to_name: payload.fan_id.to_string(),
        subject: format!(
            "💸 Refund {} € for {}, seat {}",
            payload.amount, payload.concert_id, payload.seat
        ),
        concert_id: payload.concert_id.to_string(),
        seat: payload.seat.to_string(),
        amount: payload.amount,
    }
}

impl<M: Mailer> TaskHandler<IssueRefundV1> for IssueRefundHandler<M> {
    async fn handle(&mut self, payload: IssueRefundV1, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        self.mailer.send(attempt, refund_mail(&payload)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::domain::{ConcertId, FanId, venue::SeatId};
    use uuid::Uuid;

    #[test]
    fn the_refund_is_keyed_by_the_cancellation() {
        let mail = refund_mail(&IssueRefundV1 {
            cancellation_id: Uuid::from_u128(2),
            booking_id: Uuid::from_u128(1),
            concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
            seat: SeatId::parse("A1").unwrap(),
            fan_id: FanId::parse("fox").unwrap(),
            amount: 120,
        });
        assert_eq!(mail.id, "refund:00000000-0000-0000-0000-000000000002");
        assert_eq!((mail.kind, mail.amount), ("refund", 120));
        assert_eq!(mail.subject, "💸 Refund 120 € for radiohead-2026-11-21, seat A1");
    }
}
