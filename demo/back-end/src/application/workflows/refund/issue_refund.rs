//! `encore_issue_refund` → e-mail "💸 Refund" through the flaky mail server.
use crate::{
    application::tasks::IssueRefundV1,
    infrastructure::mail::{
        chaos::ChaosPolicy,
        flaky_mail_server::{FlakyMailServer, Inbox, Mail},
    },
};
use cqrsx::task::{
    attempt::TaskAttempt,
    handler::{TaskHandler, TaskHandlerError},
};

pub struct IssueRefundHandler<I: Inbox, C: ChaosPolicy> {
    pub mail: FlakyMailServer<I, C>,
}

pub fn refund_mail(payload: &IssueRefundV1) -> Mail {
    Mail {
        id: format!("refund:{}", payload.cancellation_id),
        kind: "refund",
        to_fan_id: payload.fan_id.clone(),
        to_name: payload.fan_id.clone(),
        subject: format!(
            "💸 Refund {} € for {}, seat {}",
            payload.amount, payload.concert_id, payload.seat
        ),
        concert_id: payload.concert_id.clone(),
        seat: payload.seat.to_string(),
        amount: payload.amount,
    }
}

impl<I: Inbox, C: ChaosPolicy> TaskHandler<IssueRefundV1> for IssueRefundHandler<I, C> {
    async fn handle(&mut self, payload: IssueRefundV1, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        self.mail.send(attempt, refund_mail(&payload)).await
    }
}
