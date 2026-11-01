//! Running tasks: what an application writes ([`TaskHandler`], [`TaskResult`]) and what delivers them: the handler's
//! [`context`] and [`dependencies`], their assembly ([`handlers`]), the [`dispatcher`], [`adapter`]s, the [`executor`],
//! the [`queue`] port and the [`runtime`] with its worker pool.
pub mod adapter;
pub mod context;
pub mod dependencies;
pub mod dispatcher;
pub mod executor;
pub mod handlers;
pub mod queue;
pub mod runtime;
#[cfg(test)]
pub(crate) mod test_support;

use crate::{
    command::CommandError,
    domain::rejection::Rejection,
    task::Task,
    worker::{context::TaskContext, dependencies::DependencyId},
};
pub use cqrsx_macros::task_handler;
use serde::de::DeserializeOwned;
use std::{fmt, future::Future};

/// Application-owned operation receiving its decoded durable DTO and the context of its attempt
/// ([`TaskContext`]: the attempt, the pause of its retry, the dependencies, the command executors).
///
/// Write it as a function with [`#[task_handler]`](crate::worker::task_handler): the attribute implements this trait and lists
/// the function's dependencies. By hand, implement it with an ordinary `async fn`, without `#[async_trait]`; the
/// native future must be `Send`. Business validation, DTO conversion, the classification of the outcome
/// ([`TaskResult`]) and safe reasons remain with the handler; the pause before a retry is the policy's.
///
/// One handler serves every worker of the runtime, concurrently and after a worker's restart: it is `Send + Sync`
/// and keeps no state of its own beyond what is safe to share (a client, a pool, a counter behind a lock). Payloads
/// need `Send`, neither `Clone` nor `Sync`.
///
/// This is a statically dispatched typed port. Use
/// [`JsonTaskAdapter`](crate::worker::adapter::json::JsonTaskAdapter) to register it through
/// the native [`TaskAdapter`](crate::worker::adapter::TaskAdapter) boundary.
/// Both ports return native Send futures without invocation boxes.
/// Holding non-Send state across an await is rejected:
/// ```compile_fail
/// use cqrsx::task::Task;
/// use cqrsx::worker::{TaskHandler, TaskResult, context::TaskContext};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Payload {}
/// impl Task for Payload {
///     const TYPE: &'static str = "non_send_example";
///     const VERSION: i32 = 1;
/// }
/// struct Handler;
/// impl TaskHandler<Payload> for Handler {
///     async fn handle(&self, _: Payload, _: &TaskContext) -> TaskResult {
///         let state = std::rc::Rc::new(());
///         std::future::pending::<()>().await;
///         drop(state);
///         TaskResult::done()
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
/// use cqrsx::worker::context::TaskContext;
/// fn inspect_lease(ctx: &TaskContext) { let _ = ctx.attempt().lease(); }
/// ```
///
/// A handler written as a function:
///
/// ```
/// use cqrsx::{
///     task::{Task, attempt::TaskAttempt},
///     worker::{TaskHandler, TaskResult, context::TaskContext, dependencies::DependencyId, task_handler},
/// };
/// use serde::{Deserialize, Serialize};
/// use std::time::Duration;
///
/// #[derive(Serialize, Deserialize, Task)]
/// #[task(name = "send_e_ticket", version = 1)]
/// struct SendETicket {
///     seat: String,
/// }
///
/// trait MailService: Send + Sync {
///     fn send(&self, task_id: uuid::Uuid, seat: &str) -> bool;
/// }
/// /// The handler's dependency, by this type.
/// type Mailer = dyn MailService;
///
/// /// The task id is the mail's idempotency key: a redelivered task sends one e-mail.
/// #[task_handler]
/// async fn send_e_ticket(task: SendETicket, ctx: &TaskContext, mail: &Mailer) -> TaskResult {
///     if mail.send(ctx.task_id(), &task.seat) { TaskResult::done() } else { TaskResult::retry("mail_unavailable") }
/// }
///
/// // The function is a value: a handler of `SendETicket` that needs a `Mailer`…
/// assert_eq!(<send_e_ticket as TaskHandler<SendETicket>>::DEPENDENCIES, [DependencyId::of::<Mailer>()]);
///
/// // …and its method `call` is the function, for a test to call with a context and its dependencies.
/// struct Refusing;
/// impl MailService for Refusing {
///     fn send(&self, _: uuid::Uuid, _: &str) -> bool { false }
/// }
/// let attempt = TaskAttempt::try_first(&SendETicket { seat: "B4".into() }, serde_json::json!({}))?;
/// let ctx = TaskContext::new(attempt, Duration::from_secs(1));
/// let result = block_on(send_e_ticket.call(SendETicket { seat: "B4".into() }, &ctx, &Refusing));
/// assert_eq!(result, TaskResult::retry("mail_unavailable"));
/// # fn block_on<F: std::future::Future>(future: F) -> F::Output {
/// #     tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(future)
/// # }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// The function is `async`:
///
/// ```compile_fail
/// use cqrsx::worker::{TaskResult, task_handler};
/// # #[derive(serde::Serialize, serde::Deserialize, cqrsx::task::Task)]
/// # #[task(name = "ping", version = 1)]
/// # struct Ping {}
/// #[task_handler]
/// fn ping(task: Ping) -> TaskResult {
///     TaskResult::done()
/// }
/// ```
///
/// takes its task first, by value:
///
/// ```compile_fail
/// use cqrsx::worker::{TaskResult, context::TaskContext, task_handler};
/// #[task_handler]
/// async fn ping(ctx: &TaskContext) -> TaskResult {
///     TaskResult::done()
/// }
/// ```
///
/// and every dependency by reference:
///
/// ```compile_fail
/// use cqrsx::worker::{TaskResult, task_handler};
/// # #[derive(serde::Serialize, serde::Deserialize, cqrsx::task::Task)]
/// # #[task(name = "ping", version = 1)]
/// # struct Ping {}
/// #[task_handler]
/// async fn ping(task: Ping, clock: std::time::Instant) -> TaskResult {
///     TaskResult::done()
/// }
/// ```
pub trait TaskHandler<T>: Send + Sync
where
    T: Task + DeserializeOwned + Send + 'static,
{
    /// The dependencies the assembly must provide for this handler
    /// ([`TaskHandlers::build`](crate::worker::handlers::TaskHandlers::build) refuses it otherwise); `#[task_handler]` lists
    /// the function's dependency parameters here.
    const DEPENDENCIES: &'static [DependencyId] = &[];

    /// Perform one replay-safe operation; the executor settles its result.
    fn handle(&self, task: T, ctx: &TaskContext) -> impl Future<Output = TaskResult> + Send;
}

/// What a task handler answers for one attempt: done, try again, or stop.
///
/// The handler does not choose the pause before the next attempt: the executor applies the policy of the task's type
/// ([`Policies`](crate::task::policy::Policies)), and the handler can read that pause in its context
/// ([`TaskContext::retry_pause`](crate::worker::context::TaskContext::retry_pause)). A reason is persisted with the task
/// (`last_error`): it carries no payload, credentials or secrets.
///
/// ```
/// use cqrsx::worker::TaskResult;
///
/// assert_eq!(TaskResult::done(), TaskResult::Done);
/// assert_eq!(TaskResult::retry("mailbox_busy").to_string(), "mailbox_busy");
/// assert_eq!(TaskResult::fail("bad address"), TaskResult::Fail { reason: "bad address".into() });
/// ```
#[must_use = "the result settles the task"]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TaskResult {
    /// The effect happened, now or by an earlier attempt: the task is completed.
    Done,
    /// Not now: another attempt after the pause of the task type's policy, within its attempt budget.
    Retry { reason: String },
    /// Stop automatic delivery. This does not prove the effect never occurred.
    Fail { reason: String },
}

impl TaskResult {
    /// The effect happened.
    pub fn done() -> Self {
        Self::Done
    }

    /// Another attempt, for `reason`.
    pub fn retry(reason: impl Into<String>) -> Self {
        Self::Retry { reason: reason.into() }
    }

    /// No more attempts, for `reason`.
    pub fn fail(reason: impl Into<String>) -> Self {
        Self::Fail { reason: reason.into() }
    }
}

/// `done`, or the reason of a retry or a failure.
impl fmt::Display for TaskResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Done => f.write_str("done"),
            Self::Retry { reason } | Self::Fail { reason } => f.write_str(reason),
        }
    }
}

/// The answer of a command a task issued ([`TaskContext::execute`](crate::worker::context::TaskContext::execute)) as the
/// task's result: accepted is done; a conflict and an unavailable store are retried, with the reasons `conflict` and
/// `unavailable`; a rejection fails the task with the rejection's code. A handler matches the rejections that mean
/// "already done" or "too early" first and converts the rest.
impl<T, E: Rejection> From<Result<T, CommandError<E>>> for TaskResult {
    fn from(answer: Result<T, CommandError<E>>) -> Self {
        match answer {
            Ok(_) => Self::Done,
            Err(CommandError::Rejected(rejection)) => Self::fail(rejection.code()),
            Err(CommandError::Conflict) => Self::retry("conflict"),
            Err(CommandError::Unavailable) => Self::retry("unavailable"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[derive(Debug)]
    struct SoldOut;

    impl Rejection for SoldOut {
        fn code(&self) -> &'static str {
            "sold_out"
        }

        fn parameters(&self) -> Value {
            json!({})
        }
    }

    #[test]
    fn the_constructors_say_what_the_task_answers_and_display_their_reason() {
        assert_eq!(TaskResult::done(), TaskResult::Done);
        assert_eq!(
            TaskResult::retry("busy"),
            TaskResult::Retry {
                reason: "busy".to_owned()
            }
        );
        assert_eq!(
            TaskResult::fail(String::from("rejected")),
            TaskResult::Fail {
                reason: "rejected".to_owned()
            }
        );
        assert_eq!(
            [TaskResult::done(), TaskResult::retry("busy"), TaskResult::fail("rejected")].map(|result| result.to_string()),
            ["done", "busy", "rejected"]
        );
    }

    #[test]
    fn the_answer_of_a_command_becomes_done_a_retry_with_the_old_reasons_or_a_failure_with_its_code() {
        let answers: [Result<u8, CommandError<SoldOut>>; 4] = [
            Ok(7),
            Err(CommandError::Rejected(SoldOut)),
            Err(CommandError::Conflict),
            Err(CommandError::Unavailable),
        ];
        assert_eq!(
            answers.map(TaskResult::from),
            [
                TaskResult::Done,
                TaskResult::fail("sold_out"),
                TaskResult::retry("conflict"),
                TaskResult::retry("unavailable"),
            ]
        );
    }
}
