//! Step 2 of ticket delivery: task `encore_send_e_ticket` → e-mail "🎫 Your e-ticket".
use crate::{
    fan::domain::events::TicketRecorded,
    shared::{
        application::{
            activity::ActivityHub,
            mail::{Mail, Mailer},
        },
        domain::{ConcertId, FanId, FanLedgerId, venue::SeatId},
    },
};
use cqrsx::{
    Task,
    task::{context::TaskContext, result::TaskResult},
    task_handler,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `Fan::TicketRecorded` → e-mail "🎫 Your e-ticket" (step [`send_e_ticket`](super::send_e_ticket)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "encore_send_e_ticket", version = 1)]
pub struct SendETicketV1 {
    pub booking_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub fan_id: FanId,
    pub fan_name: String,
    pub price: u32,
}

impl SendETicketV1 {
    /// The e-ticket of a ticket the ledger recorded; the fan is the ledger's.
    pub fn of(ledger: &FanLedgerId, recorded: &TicketRecorded) -> Self {
        Self {
            booking_id: recorded.booking_id,
            concert_id: recorded.concert_id.clone(),
            seat: recorded.seat.clone(),
            fan_id: ledger.fan(),
            fan_name: recorded.fan_name.clone(),
            price: recorded.price,
        }
    }
}

/// One e-ticket per booking: the booking id is the mail's idempotency key.
pub fn e_ticket_mail(payload: &SendETicketV1) -> Mail {
    Mail {
        id: format!("e_ticket:{}", payload.booking_id),
        kind: "e_ticket",
        to_fan_id: payload.fan_id.to_string(),
        to_name: payload.fan_name.clone(),
        subject: format!("🎫 Your e-ticket: {}, seat {}", payload.concert_id, payload.seat),
        concert_id: payload.concert_id.to_string(),
        seat: payload.seat.to_string(),
        amount: payload.price,
    }
}

/// Sends the e-ticket, reporting the attempt to the activity log; the mail service answers whether it was delivered (a
/// repeat is delivered once) or is retried after the pause of the `MAIL` policy.
#[task_handler]
pub async fn send_e_ticket(task: SendETicketV1, ctx: &TaskContext, mail: &Mailer, activity: &ActivityHub) -> TaskResult {
    mail.send(ctx, activity, e_ticket_mail(&task)).await
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
    fn the_e_ticket_is_keyed_by_the_booking() {
        let mail = e_ticket_mail(&SendETicketV1 {
            booking_id: Uuid::from_u128(1),
            concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
            seat: SeatId::parse("B4").unwrap(),
            fan_id: FanId::parse("fox").unwrap(),
            fan_name: "Fox".into(),
            price: 80,
        });
        assert_eq!(mail.id, "e_ticket:00000000-0000-0000-0000-000000000001");
        assert_eq!((mail.kind, mail.to_name.as_str(), mail.amount), ("e_ticket", "Fox", 80));
        assert_eq!(mail.subject, "🎫 Your e-ticket: radiohead-2026-11-21, seat B4");
    }

    #[tokio::test]
    async fn an_e_ticket_task_sends_the_mail_of_its_booking() {
        let task = SendETicketV1 {
            booking_id: Uuid::from_u128(1),
            concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
            seat: SeatId::parse("B4").unwrap(),
            fan_id: FanId::parse("fox").unwrap(),
            fan_name: "Fox".into(),
            price: 80,
        };
        let ctx = TaskContext::new(
            cqrsx::task::attempt::TaskAttempt::first(&task, serde_json::json!({})).unwrap(),
            std::time::Duration::from_secs(1),
        );
        let mail = RecordingMailer::delivering();
        let activity = ActivityHub::new(1, 1);
        assert_eq!(
            send_e_ticket.call(task.clone(), &ctx, &mail, &activity).await,
            TaskResult::Done
        );
        assert_eq!(mail.sent(), [(ctx.task_id(), e_ticket_mail(&task))]);
        let refusing = RecordingMailer {
            answer: TaskResult::retry("mailbox_busy"),
            ..RecordingMailer::delivering()
        };
        assert_eq!(
            send_e_ticket.call(task, &ctx, &refusing, &activity).await,
            TaskResult::retry("mailbox_busy"),
            "the mail service's answer is the task's"
        );
    }
}
