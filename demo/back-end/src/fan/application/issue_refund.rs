//! Step 2 of refund: task `encore_issue_refund` → e-mail "💸 Refund".
use crate::{
    fan::domain::events::TicketCancelled,
    shared::{
        application::{
            activity::ActivityHub,
            mail::{Mail, Mailer},
        },
        domain::{ConcertId, FanId, FanLedgerId, venue::SeatId},
    },
};
use cqrsx::{
    Task, task_handler,
    worker::{TaskResult, context::TaskContext},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `Fan::TicketCancelled` → e-mail "💸 Refund" (step [`issue_refund`](super::issue_refund)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "encore_issue_refund", version = 1)]
pub struct IssueRefundV1 {
    pub cancellation_id: Uuid,
    pub booking_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub fan_id: FanId,
    pub amount: u32,
}

impl IssueRefundV1 {
    /// The refund of a ticket the ledger cancelled; the fan is the ledger's.
    pub fn of(ledger: &FanLedgerId, cancelled: &TicketCancelled) -> Self {
        Self {
            cancellation_id: cancelled.cancellation_id,
            booking_id: cancelled.booking_id,
            concert_id: cancelled.concert_id.clone(),
            seat: cancelled.seat.clone(),
            fan_id: ledger.fan(),
            amount: cancelled.refund,
        }
    }
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

/// Sends the refund, reporting the attempt to the activity log; the mail service answers whether it was delivered (a
/// repeat is delivered once) or is retried after the pause of the `MAIL` policy.
#[task_handler]
pub async fn issue_refund(task: IssueRefundV1, ctx: &TaskContext, mail: &Mailer, activity: &ActivityHub) -> TaskResult {
    mail.send(ctx, activity, refund_mail(&task)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::{
        application::test_support::RecordingMailer,
        domain::{ConcertId, FanId, venue::SeatId},
    };
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

    #[tokio::test]
    async fn a_refund_task_sends_the_mail_of_its_cancellation() {
        let task = IssueRefundV1 {
            cancellation_id: Uuid::from_u128(2),
            booking_id: Uuid::from_u128(1),
            concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
            seat: SeatId::parse("A1").unwrap(),
            fan_id: FanId::parse("fox").unwrap(),
            amount: 120,
        };
        let ctx = TaskContext::new(
            cqrsx::task::attempt::TaskAttempt::try_first(&task, serde_json::json!({})).unwrap(),
            std::time::Duration::from_secs(1),
        );
        let mail = RecordingMailer::delivering();
        let activity = ActivityHub::new(1, 1);
        assert_eq!(
            issue_refund.call(task.clone(), &ctx, &mail, &activity).await,
            TaskResult::Done
        );
        assert_eq!(mail.sent(), [(ctx.task_id(), refund_mail(&task))]);
    }
}
