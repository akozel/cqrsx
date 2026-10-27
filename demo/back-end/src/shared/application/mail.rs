//! The port to the external mail service. The adapter lives in `shared::infrastructure`; tests use in-memory
//! doubles.
use crate::shared::application::activity::ActivityHub;
use async_trait::async_trait;
use cqrsx::task::{context::TaskContext, result::TaskResult};

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

/// The external mail service, called from a task attempt (`ctx`: the attempt and the pause of its retry), which it
/// reports to `activity`, the activity log. Delivery is at-least-once (a lease can expire, the process can crash after
/// the effect and before settlement), so a repeated `send` of the same `Mail::id` must not create a second e-mail. A
/// failure asks the queue to retry, after the pause of the task type's policy.
#[async_trait]
pub trait MailService: Send + Sync {
    async fn send(&self, ctx: &TaskContext, activity: &ActivityHub, mail: Mail) -> TaskResult;
}

/// The task handlers' dependency: the mail service, by this type (the composition provides an `Arc<Mailer>`).
pub type Mailer = dyn MailService;
