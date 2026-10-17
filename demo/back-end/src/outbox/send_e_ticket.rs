//! `encore_send_e_ticket` → e-mail "🎫 Your e-ticket" through the flaky mail server.
use crate::{
    app::tasks::SendETicketV1,
    outbox::{
        chaos::ChaosPolicy,
        flaky_mail::{FlakyMailServer, Inbox, Mail},
    },
};
use cqrsx::task::{
    attempt::TaskAttempt,
    handler::{TaskHandler, TaskHandlerError},
};

pub struct SendETicketHandler<I: Inbox, C: ChaosPolicy> {
    pub mail: FlakyMailServer<I, C>,
}

pub fn e_ticket_mail(payload: &SendETicketV1) -> Mail {
    Mail {
        id: format!("e_ticket:{}", payload.booking_id),
        kind: "e_ticket",
        to_fan_id: payload.fan_id.clone(),
        to_name: payload.fan_name.clone(),
        subject: format!("🎫 Your e-ticket: {}, seat {}", payload.concert_id, payload.seat),
        concert_id: payload.concert_id.clone(),
        seat: payload.seat.to_string(),
        amount: payload.price,
    }
}

impl<I: Inbox, C: ChaosPolicy> TaskHandler<SendETicketV1> for SendETicketHandler<I, C> {
    async fn handle(&mut self, payload: SendETicketV1, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        self.mail.send(attempt, e_ticket_mail(&payload)).await
    }
}
