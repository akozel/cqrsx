//! Ports: what the application needs from the outside world. The adapters live in
//! `infrastructure`; tests use in-memory doubles.
use cqrsx::task::{attempt::TaskAttempt, handler::TaskHandlerError};
use std::future::Future;

/// An e-mail for the external mail service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mail {
    /// Business key and idempotency key: `e_ticket:<booking_id>` or `refund:<cancellation_id>`.
    pub id: String,
    pub kind: &'static str,
    pub to_fan_id: String,
    pub to_name: String,
    pub subject: String,
    pub concert_id: String,
    pub seat: String,
    pub amount: u32,
}

/// The external mail service, called from a task attempt. Delivery is at-least-once (a lease can
/// expire, the process can crash after the effect and before settlement), so a repeated `send`
/// of the same `Mail::id` must not create a second e-mail. A failure asks the queue to retry.
pub trait Mailer: Send {
    fn send(&mut self, attempt: &TaskAttempt, mail: Mail) -> impl Future<Output = Result<(), TaskHandlerError>> + Send;
}
