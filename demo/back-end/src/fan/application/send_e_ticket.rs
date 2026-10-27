//! Step 2 of ticket delivery: task `encore_send_e_ticket` → e-mail "🎫 Your e-ticket".
use crate::{
    fan::domain::events::TicketRecorded,
    shared::{
        application::{
            mail::{Mail, Mailer},
            tasks::new_task,
        },
        domain::{ConcertId, FanId, FanLedgerId, venue::SeatId},
    },
};
use cqrsx::{
    Task,
    task::{
        NewTask, TaskValidationError,
        attempt::TaskAttempt,
        handler::{TaskHandler, TaskHandlerError},
    },
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

/// The e-ticket of a ticket the ledger recorded; the fan is the ledger's.
pub fn send_e_ticket(ledger: &FanLedgerId, recorded: &TicketRecorded, max_attempts: i32) -> Result<NewTask, TaskValidationError> {
    let fan_id = ledger.fan();
    let payload = SendETicketV1 {
        booking_id: recorded.booking_id,
        concert_id: recorded.concert_id.clone(),
        seat: recorded.seat.clone(),
        fan_id: fan_id.clone(),
        fan_name: recorded.fan_name.clone(),
        price: recorded.price,
    };
    new_task(payload, &recorded.concert_id, &fan_id, max_attempts)
}

pub struct SendETicketHandler<M> {
    pub mailer: M,
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

impl<M: Mailer> TaskHandler<SendETicketV1> for SendETicketHandler<M> {
    async fn handle(&mut self, payload: SendETicketV1, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        self.mailer.send(attempt, e_ticket_mail(&payload)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::domain::{ConcertId, FanId, venue::SeatId};
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
}
