//! What a command executor tells about every command it ran: the aggregate's id, the command, its metadata and the
//! outcome its answer reports, once per command, after the command released its aggregate's lock.
use crate::{
    cqrs::{CommitReport, aggregate::Aggregate, command::CommandGatewayError},
    task::NewTask,
};
use std::collections::HashMap;

/// Hears every command an executor ran, e.g. for an activity feed
/// ([`CommandExecutorCore::with_observer`](super::executor::CommandExecutorCore::with_observer) and the presets'
/// `with_observer`).
///
/// The executor calls it once per command: after the command's attempts, once the aggregate's lock is released and
/// the commit wake-up sent, before the caller gets its answer, on the caller's task. So the next command of the
/// aggregate does not wait for the observer, and the caller does. The reports of one aggregate come in the order of
/// its commits, unless, on a multi-threaded runtime, the caller's thread is preempted between the release of the lock
/// and the call long enough for the next command of the aggregate to commit and report first.
///
/// The core reads neither [`CommandInfo`](super::CommandInfo) nor [`Rejection`](crate::cqrs::rejection::Rejection):
/// an observer that names a command or a rejection bounds its implementation on them. An observer does not block and
/// does not panic: a panic is the command's (its commit stays, the caller panics).
///
/// ```
/// use cqrsx::cqrs::{
///     aggregate::Aggregate,
///     command::{
///         CommandExecutor,
///         decide::Decide,
///         observer::{CommandObserver, CommandReport, Outcome},
///     },
///     event::DomainEvent,
///     testing::{command::TestCommandExecutor, mem_store::MemStore},
/// };
/// use serde::{Deserialize, Serialize};
/// use std::sync::{Arc, Mutex};
///
/// #[derive(Clone, Default, Serialize, Deserialize)]
/// struct Counter(u32);
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// struct Incremented;
/// impl DomainEvent for Incremented {
///     fn event_type(&self) -> String { "incremented".into() }
///     fn event_version(&self) -> String { "1".into() }
/// }
/// #[derive(Debug, PartialEq, thiserror::Error)]
/// #[error("limit reached")]
/// struct LimitReached;
/// impl Aggregate for Counter {
///     const TYPE: &'static str = "counter";
///     type Id = String;
///     type Command = ();
///     type Event = Incremented;
///     type Error = LimitReached;
///     type Rules = u32;
///     fn apply(&mut self, _: Incremented) { self.0 += 1; }
/// }
/// impl Decide<Counter> for () {
///     fn decide(self, counter: &Counter, limit: &u32) -> Result<Incremented, LimitReached> {
///         if counter.0 >= *limit {
///             return Err(LimitReached);
///         }
///         Ok(Incremented)
///     }
/// }
///
/// /// Writes one line per command: the counter and what became of the command.
/// #[derive(Default)]
/// struct Log(Mutex<Vec<String>>);
/// impl CommandObserver<Counter> for Log {
///     fn command(&self, report: &CommandReport<'_, Counter>) {
///         let outcome = match report.outcome {
///             Outcome::Accepted { tasks } => format!("accepted with {} tasks", tasks.len()),
///             Outcome::Rejected(error) => format!("rejected: {error}"),
///             Outcome::Conflict => "conflict".to_owned(),
///             Outcome::Unavailable => "unavailable".to_owned(),
///         };
///         let source = report.metadata.get("source").map_or("-", String::as_str);
///         self.0.lock().unwrap().push(format!("{} from {source}: {outcome}", report.aggregate_id));
///     }
/// }
///
/// # tokio_test(async {
/// let log = Arc::new(Log::default());
/// let counters = TestCommandExecutor::new(MemStore::<Counter>::default(), 1).with_observer(log.clone());
/// let metadata = [("source".to_owned(), "ui".to_owned())].into();
/// counters.execute_with_metadata(&"c-1".into(), (), metadata).await.unwrap();
/// counters.execute(&"c-1".into(), ()).await.unwrap_err();
/// assert_eq!(
///     *log.0.lock().unwrap(),
///     ["c-1 from ui: accepted with 0 tasks", "c-1 from -: rejected: limit reached"]
/// );
/// # });
/// # fn tokio_test(future: impl std::future::Future<Output = ()>) {
/// #     tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(future)
/// # }
/// ```
pub trait CommandObserver<A: Aggregate>: Send + Sync {
    /// One command, as its executor ran it.
    fn command(&self, report: &CommandReport<'_, A>);
}

/// One command as its executor ran it.
pub struct CommandReport<'a, A: Aggregate> {
    /// The aggregate the command ran on.
    pub aggregate_id: &'a A::Id,
    /// The command as the caller passed it (every attempt decided a copy of it).
    pub command: &'a A::Command,
    /// The metadata the caller passed: the map every committed event stores.
    pub metadata: &'a HashMap<String, String>,
    pub outcome: Outcome<'a, A>,
}

/// What became of a command: the outcome its answer reports.
pub enum Outcome<'a, A: Aggregate> {
    /// Committed: its event and, with it, these tasks.
    Accepted { tasks: &'a [NewTask] },
    /// A business rejection; nothing was written.
    Rejected(&'a A::Error),
    /// Every attempt met a concurrent change; nothing was written.
    Conflict,
    /// Not run, or its outcome is unknown: the lock refused it, the store failed or its tasks could not be planned.
    Unavailable,
}

impl<'a, A: Aggregate> Outcome<'a, A> {
    /// The outcome `answer` reports.
    pub(crate) fn of(answer: &'a Result<CommitReport<A>, CommandGatewayError<A::Error>>) -> Self {
        match answer {
            Ok(report) => Outcome::Accepted { tasks: &report.tasks },
            Err(CommandGatewayError::Domain(error)) => Outcome::Rejected(error),
            Err(CommandGatewayError::Conflict) => Outcome::Conflict,
            Err(CommandGatewayError::Unavailable) => Outcome::Unavailable,
        }
    }
}
