//! One application-owned business operation on a decoded task payload.
use crate::task::{attempt::TaskAttempt, format::TaskPayload};
use serde::de::DeserializeOwned;
use std::{future::Future, time::Duration};
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum TaskHandlerError {
    /// Request durable redelivery after optional backoff, subject to max_attempts.
    /// A safe reason is persisted; do not include payloads, credentials, or secrets.
    #[error("{reason}")]
    Retry { reason: String, retry_after: Option<Duration> },
    /// Stop automatic delivery. This does not prove the effect never occurred.
    #[error("{reason}")]
    Permanent { reason: String },
}

/// Application-owned operation receiving its decoded durable DTO and original attempt.
///
/// Implement with an ordinary `async fn`, without `#[async_trait]`. The native
/// future must be `Send`; business validation, DTO conversion, exact redelivery
/// classification, and safe error reasons remain with the handler. Payloads
/// and handlers need `Send`, but neither `Clone` nor `Sync`.
///
/// This is a statically dispatched typed port. Use
/// [`JsonTaskAdapter`](super::adapter::json::JsonTaskAdapter) to register it through
/// the native [`TaskAdapter`](super::adapter::TaskAdapter) boundary.
/// Both ports return native Send futures without invocation boxes.
/// Holding non-Send state across an await is rejected:
/// ```compile_fail
/// use cqrsx::task::{attempt::TaskAttempt, format::TaskPayload, handler::{TaskHandler, TaskHandlerError}};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Payload {}
/// impl TaskPayload for Payload {
///     const TASK_TYPE: &'static str = "non_send_example";
///     const TASK_VERSION: i32 = 1;
/// }
/// struct Handler;
/// impl TaskHandler<Payload> for Handler {
///     async fn handle(&mut self, _: Payload, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
///         let state = std::rc::Rc::new(());
///         std::future::pending::<()>().await;
///         drop(state);
///         Ok(())
///     }
/// }
/// ```
///
/// Tasks can be redelivered after timeout, crash or lost acknowledgement. Local
/// retries share the invocation budget; accepted remote effects cannot be undone
/// by cancellation. Every invocation must be safe to repeat from the beginning.
/// Stable task IDs/metadata can carry idempotency context, but lease tokens remain
/// platform-only:
/// ```compile_fail
/// use cqrsx::task::attempt::TaskAttempt;
/// fn inspect_lease(attempt: &TaskAttempt) { let _ = attempt.lease(); }
/// ```
pub trait TaskHandler<P>: Send
where
    P: TaskPayload + DeserializeOwned + Send + 'static,
{
    /// Perform one replay-safe operation; the executor settles its result.
    fn handle(&mut self, payload: P, attempt: &TaskAttempt) -> impl Future<Output = Result<(), TaskHandlerError>> + Send;
}
