//! What a command executor tells about every command it ran: the aggregate's id, the command, its metadata and the
//! outcome its answer reports, once per command, after the command released its aggregate's lock. An observer of every
//! aggregate type (an activity feed in a module that names no aggregate) hears the same report with the type erased:
//! [`AnyCommandObserver`].
use crate::{
    command::{CommandGatewayError, CommitReport},
    domain::{aggregate::Aggregate, command::CommandInfo, rejection::Rejection},
    task::{NewTask, format::TaskFormat},
};
use std::{any::Any, collections::HashMap, fmt};

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
/// use cqrsx::{
///     command::{CommandExecutor, observer::{CommandObserver, CommandReport, Outcome}},
///     testing::{command_executor::TestCommandExecutor, mem_store::MemStore},
///     domain::{aggregate::Aggregate, command::Decide, event::DomainEvent},
/// };
/// use serde::{Deserialize, Serialize};
/// use std::sync::{Arc, Mutex};
///
/// #[derive(Clone, Default, Serialize, Deserialize)]
/// struct Counter(u32);
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// struct Incremented;
/// impl DomainEvent for Incremented {
///     fn event_type(&self) -> &'static str { "incremented" }
///     fn event_version(&self) -> &'static str { "1" }
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

// By hand: the command need not be `Debug`, and the metadata is the caller's data, so neither is printed.
impl<A: Aggregate> fmt::Debug for CommandReport<'_, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandReport")
            .field("aggregate_type", &A::TYPE)
            .field("aggregate_id", &format_args!("{}", self.aggregate_id))
            .field("outcome", &self.outcome)
            .finish_non_exhaustive()
    }
}

// By hand: a derive would require `A` to be `Debug`. The tasks print as their formats, never their payloads.
impl<A: Aggregate> fmt::Debug for Outcome<'_, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accepted { tasks } => f.debug_struct("Accepted").field("tasks", &formats(tasks)).finish(),
            Self::Rejected(error) => f.debug_tuple("Rejected").field(error).finish(),
            Self::Conflict => f.write_str("Conflict"),
            Self::Unavailable => f.write_str("Unavailable"),
        }
    }
}

/// The formats of `tasks`: what a report prints of its tasks, whose payloads and metadata it never prints.
fn formats(tasks: &[NewTask]) -> Vec<&TaskFormat> {
    tasks.iter().map(NewTask::format).collect()
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

// By hand: the command prints as its name, the typed id and the metadata are not printed.
impl fmt::Debug for AnyCommandReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnyCommandReport")
            .field("aggregate_type", &self.aggregate_type)
            .field("aggregate_id", &self.aggregate_id)
            .field("command", &self.command.name())
            .field("outcome", &self.outcome)
            .finish_non_exhaustive()
    }
}

// By hand: a rejection prints as its code.
impl fmt::Debug for AnyOutcome<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accepted { tasks } => f.debug_struct("Accepted").field("tasks", &formats(tasks)).finish(),
            Self::Rejected(rejection) => f.debug_tuple("Rejected").field(&rejection.code()).finish(),
            Self::Conflict => f.write_str("Conflict"),
            Self::Unavailable => f.write_str("Unavailable"),
        }
    }
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
        domain::{
            command::{Decide, Label},
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
        fn event_type(&self) -> &'static str {
            "booked"
        }

        fn event_version(&self) -> &'static str {
            "1"
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
        let task = NewTask::from_payload(TaskFormat::try_new("notify", 1).unwrap(), json!({})).unwrap();
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
        assert_eq!((Booked.event_type(), Booked.event_version()), ("booked", "1"));
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

    #[test]
    fn a_report_prints_its_aggregate_and_outcome_but_neither_the_metadata_nor_the_task_payloads() {
        let task = NewTask::from_payload(TaskFormat::try_new("notify", 1).unwrap(), json!({ "card": "4111" })).unwrap();
        let metadata = HashMap::from([("token".to_owned(), "t0k3n".to_owned())]);
        let printed: Vec<(String, String)> = [
            Outcome::Accepted {
                tasks: std::slice::from_ref(&task),
            },
            Outcome::Rejected(&SeatTaken),
            Outcome::Conflict,
            Outcome::Unavailable,
        ]
        .into_iter()
        .map(|outcome| {
            let report = CommandReport::<Hall> {
                aggregate_id: &7,
                command: &Book,
                metadata: &metadata,
                outcome,
            };
            (format!("{report:?}"), format!("{:?}", AnyCommandReport::of(&report)))
        })
        .collect();
        let typed = r#"CommandReport { aggregate_type: "hall", aggregate_id: 7, outcome: "#;
        let erased = r#"AnyCommandReport { aggregate_type: "hall", aggregate_id: "7", command: "Book", outcome: "#;
        let accepted = r#"Accepted { tasks: [TaskFormat { task_type: "notify", task_version: 1 }] }"#;
        assert_eq!(
            printed,
            [
                (format!("{typed}{accepted}, .. }}"), format!("{erased}{accepted}, .. }}")),
                (
                    format!("{typed}Rejected(SeatTaken), .. }}"),
                    format!(r#"{erased}Rejected("seat_taken"), .. }}"#)
                ),
                (format!("{typed}Conflict, .. }}"), format!("{erased}Conflict, .. }}")),
                (format!("{typed}Unavailable, .. }}"), format!("{erased}Unavailable, .. }}")),
            ]
        );
    }
}
