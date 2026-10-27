//! One application-owned business operation on a decoded task payload.
use crate::task::{context::TaskContext, dependencies::DependencyId, format::TaskPayload, result::TaskResult};
use serde::de::DeserializeOwned;
use std::future::Future;

/// Application-owned operation receiving its decoded durable DTO and the context of its attempt
/// ([`TaskContext`]: the attempt, the pause of its retry, the dependencies, the command executors).
///
/// Write it as a function with [`#[task_handler]`](crate::task_handler): the attribute implements this trait and lists
/// the function's dependencies. By hand, implement it with an ordinary `async fn`, without `#[async_trait]`; the
/// native future must be `Send`. Business validation, DTO conversion, the classification of the outcome
/// ([`TaskResult`]) and safe reasons remain with the handler; the pause before a retry is the policy's.
///
/// One handler serves every worker of the runtime, concurrently and after a worker's restart: it is `Send + Sync`
/// and keeps no state of its own beyond what is safe to share (a client, a pool, a counter behind a lock). Payloads
/// need `Send`, neither `Clone` nor `Sync`.
///
/// This is a statically dispatched typed port. Use
/// [`JsonTaskAdapter`](super::adapter::json::JsonTaskAdapter) to register it through
/// the native [`TaskAdapter`](super::adapter::TaskAdapter) boundary.
/// Both ports return native Send futures without invocation boxes.
/// Holding non-Send state across an await is rejected:
/// ```compile_fail
/// use cqrsx::task::{context::TaskContext, format::TaskPayload, handler::TaskHandler, result::TaskResult};
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
/// use cqrsx::task::context::TaskContext;
/// fn inspect_lease(ctx: &TaskContext) { let _ = ctx.attempt().lease(); }
/// ```
///
/// A handler written as a function:
///
/// ```
/// use cqrsx::{
///     Task, task_handler,
///     task::{
///         attempt::TaskAttempt, context::TaskContext, dependencies::DependencyId, handler::TaskHandler,
///         result::TaskResult,
///     },
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
/// let attempt = TaskAttempt::first(&SendETicket { seat: "B4".into() }, serde_json::json!({}))?;
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
/// use cqrsx::{task_handler, task::result::TaskResult};
/// # #[derive(serde::Serialize, serde::Deserialize, cqrsx::Task)]
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
/// use cqrsx::{task_handler, task::{context::TaskContext, result::TaskResult}};
/// #[task_handler]
/// async fn ping(ctx: &TaskContext) -> TaskResult {
///     TaskResult::done()
/// }
/// ```
///
/// and every dependency by reference:
///
/// ```compile_fail
/// use cqrsx::{task_handler, task::result::TaskResult};
/// # #[derive(serde::Serialize, serde::Deserialize, cqrsx::Task)]
/// # #[task(name = "ping", version = 1)]
/// # struct Ping {}
/// #[task_handler]
/// async fn ping(task: Ping, clock: std::time::Instant) -> TaskResult {
///     TaskResult::done()
/// }
/// ```
pub trait TaskHandler<P>: Send + Sync
where
    P: TaskPayload + DeserializeOwned + Send + 'static,
{
    /// The dependencies the assembly must provide for this handler
    /// ([`TaskHandlers::build`](super::handlers::TaskHandlers::build) refuses it otherwise); `#[task_handler]` lists
    /// the function's dependency parameters here.
    const DEPENDENCIES: &'static [DependencyId] = &[];

    /// Perform one replay-safe operation; the executor settles its result.
    fn handle(&self, task: P, ctx: &TaskContext) -> impl Future<Output = TaskResult> + Send;
}
