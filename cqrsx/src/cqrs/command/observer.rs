//! What a command executor tells about every command it ran: the aggregate's id, the command, its metadata and the
//! outcome its answer reports, once per command, after the command released its aggregate's lock. An observer of every
//! aggregate type (an activity feed in a module that names no aggregate) hears the same report with the type erased:
//! [`AnyCommandObserver`].
use crate::{
    cqrs::{
        CommitReport,
        aggregate::Aggregate,
        command::{CommandGatewayError, CommandInfo},
        rejection::Rejection,
    },
    task::NewTask,
};
use std::{any::Any, collections::HashMap};

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
/// The core reads neither [`CommandInfo`] nor [`Rejection`]:
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
///     testing::{command_executor::TestCommandExecutor, mem_store::MemStore},
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

/// Hears the commands of every aggregate type, e.g. an activity feed in a module that names no aggregate: the report
/// of a [`CommandObserver`] with the aggregate's type erased ([`AnyCommandReport::of`]). An engine tells it about the
/// commands of every executor it builds; when and how often is [`CommandObserver`]'s.
pub trait AnyCommandObserver: Send + Sync {
    /// One command, as its executor ran it.
    fn command(&self, report: &AnyCommandReport<'_>);
}

/// One command of any aggregate type: what [`CommandReport`] holds, with the command as what is known of it
/// ([`CommandInfo`]) and a rejection as what is reported of it ([`Rejection`]).
pub struct AnyCommandReport<'a> {
    /// The aggregate's type, `A::TYPE`.
    pub aggregate_type: &'static str,
    /// The aggregate's id as its text, its `Display`.
    pub aggregate_id: String,
    /// The aggregate's id as its type, for an observer that knows the type (`id.downcast_ref::<LedgerId>()`).
    pub id: &'a (dyn Any + Send + Sync),
    pub command: &'a dyn CommandInfo,
    pub metadata: &'a HashMap<String, String>,
    pub outcome: AnyOutcome<'a>,
}

/// What became of a command of any aggregate type: [`Outcome`] with the rejection as a [`Rejection`].
pub enum AnyOutcome<'a> {
    Accepted { tasks: &'a [NewTask] },
    Rejected(&'a dyn Rejection),
    Conflict,
    Unavailable,
}

impl<'a> AnyCommandReport<'a> {
    /// `report` with its aggregate's type erased.
    pub fn of<A>(report: &CommandReport<'a, A>) -> Self
    where
        A: Aggregate,
        A::Command: CommandInfo,
        A::Error: Rejection,
    {
        Self {
            aggregate_type: A::TYPE,
            aggregate_id: report.aggregate_id.to_string(),
            id: report.aggregate_id,
            command: report.command,
            metadata: report.metadata,
            outcome: match report.outcome {
                Outcome::Accepted { tasks } => AnyOutcome::Accepted { tasks },
                Outcome::Rejected(error) => AnyOutcome::Rejected(error),
                Outcome::Conflict => AnyOutcome::Conflict,
                Outcome::Unavailable => AnyOutcome::Unavailable,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cqrs::{
            command::{Label, decide::Decide},
            event::DomainEvent,
        },
        task::format::TaskFormat,
    };
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};
    use std::sync::Mutex;

    #[derive(Default, Serialize, Deserialize)]
    struct Hall;

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Booked;

    impl DomainEvent for Booked {
        fn event_type(&self) -> String {
            "booked".into()
        }

        fn event_version(&self) -> String {
            "1".into()
        }
    }

    /// A booking of the tests: its fan is its label.
    struct Book;

    impl CommandInfo for Book {
        fn name(&self) -> &'static str {
            "Book"
        }

        fn labels(&self) -> Vec<Label> {
            vec![Label {
                name: "fan_id",
                value: "fox".into(),
            }]
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("seat taken")]
    struct SeatTaken;

    impl Rejection for SeatTaken {
        fn code(&self) -> &'static str {
            "seat_taken"
        }

        fn params(&self) -> Value {
            json!({ "seat": "B4" })
        }
    }

    impl Decide<Hall> for Book {
        fn decide(self, _: &Hall, _: &()) -> Result<Booked, SeatTaken> {
            Err(SeatTaken)
        }
    }

    impl Aggregate for Hall {
        const TYPE: &'static str = "hall";
        type Id = u32;
        type Command = Book;
        type Event = Booked;
        type Error = SeatTaken;
        type Rules = ();

        fn apply(&mut self, _: Booked) {}
    }

    /// Writes what it hears of every command.
    #[derive(Default)]
    struct Log(Mutex<Vec<String>>);

    impl AnyCommandObserver for Log {
        fn command(&self, report: &AnyCommandReport<'_>) {
            let outcome = match &report.outcome {
                AnyOutcome::Accepted { tasks } => format!("accepted with {}", tasks[0].format()),
                AnyOutcome::Rejected(rejection) => format!("rejected: {} {}", rejection.code(), rejection.params()),
                AnyOutcome::Conflict => "conflict".to_owned(),
                AnyOutcome::Unavailable => "unavailable".to_owned(),
            };
            let id = report.id.downcast_ref::<u32>().copied();
            self.0.lock().unwrap().push(format!(
                "{} {} {:?} {} {:?} {:?}: {outcome}",
                report.aggregate_type,
                report.aggregate_id,
                id,
                report.command.name(),
                report.command.labels(),
                report.metadata.get("source"),
            ));
        }
    }

    #[test]
    fn an_any_report_names_the_aggregate_the_command_its_rejection_and_the_typed_id() {
        let log = Log::default();
        let task = NewTask::from_payload(TaskFormat::new("notify", 1).unwrap(), json!({})).unwrap();
        let metadata = HashMap::from([("source".to_owned(), "ui".to_owned())]);
        for outcome in [
            Outcome::Accepted {
                tasks: std::slice::from_ref(&task),
            },
            Outcome::Rejected(&SeatTaken),
            Outcome::Conflict,
            Outcome::Unavailable,
        ] {
            let report = CommandReport::<Hall> {
                aggregate_id: &7,
                command: &Book,
                metadata: &metadata,
                outcome,
            };
            log.command(&AnyCommandReport::of(&report));
        }
        // The fixture is an aggregate: its booking decides, its event applies and is stored by name and version.
        let mut hall = Hall;
        hall.apply(Booked);
        assert!(matches!(Book.decide(&hall, &()), Err(SeatTaken)));
        assert_eq!(
            (Booked.event_type(), Booked.event_version()),
            ("booked".to_owned(), "1".to_owned())
        );
        let head = r#"hall 7 Some(7) Book [Label { name: "fan_id", value: "fox" }] Some("ui")"#;
        assert_eq!(
            *log.0.lock().unwrap(),
            [
                format!("{head}: accepted with notify v1"),
                format!(r#"{head}: rejected: seat_taken {{"seat":"B4"}}"#),
                format!("{head}: conflict"),
                format!("{head}: unavailable"),
            ]
        );
    }
}
