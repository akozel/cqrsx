//! The derive macros as a downstream crate uses them: what their expansions do once compiled, on the edges the
//! examples leave out (generic types, raw field names, enums without variants, an overridden code).
// Test code: a panic is the failed check (rule 8 of .claude/rules/rust-style.md); clippy.toml allows it only inside
// `#[test]` functions, not in the helpers of this crate.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing
)]
use cqrsx::{
    Command, Commands, Event, Events, Rejection, Task,
    cqrs::{
        aggregate::Aggregate,
        command::{CommandInfo, Label, decide::Decide},
        event::{DomainEvent, EventKind},
        rejection::Rejection as _,
        testing::TestFramework,
    },
    task::{
        NewTask,
        adapter::{TaskAdapter, json::JsonTaskAdapter},
        attempt::TaskAttempt,
        context::TaskContext,
        dependencies::DependencyId,
        dispatcher::TaskDispatcherError,
        format::{TaskFormat, TaskPayload},
        handler::TaskHandler,
        handlers::TaskHandlers,
        result::TaskResult,
    },
    task_handler,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;
use std::{
    fmt::{Debug, Display},
    sync::Arc,
};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(name = "SeatBooked", version = "1.0")]
struct SeatBooked {
    seat: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(name = "Noted", version = "3")]
struct Noted<T>
where
    T: Clone,
{
    note: T,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Events)]
#[serde(tag = "type", bound(deserialize = "T: DeserializeOwned"))]
enum ConcertEvent<T: Clone + Debug + PartialEq + Send + Sync + Serialize + DeserializeOwned> {
    SeatBooked(SeatBooked),
    Noted(Noted<T>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Events)]
enum NoEvent {}

#[test]
fn derived_events_store_the_name_and_the_version_of_each_payload() {
    assert_eq!((SeatBooked::NAME, SeatBooked::VERSION), ("SeatBooked", "1.0"));
    assert_eq!((Noted::<u8>::NAME, Noted::<u8>::VERSION), ("Noted", "3"));
    let booked = ConcertEvent::<String>::SeatBooked(SeatBooked { seat: "B4".into() });
    let noted = ConcertEvent::Noted(Noted { note: "late".to_owned() });
    assert_eq!((booked.event_type(), booked.event_version()), ("SeatBooked", "1.0"));
    assert_eq!((noted.event_type(), noted.event_version()), ("Noted", "3"));
    // The derive leaves serde's attributes alone: the stored payload is still tagged by the variant.
    assert_eq!(
        serde_json::to_value(&noted).unwrap(),
        serde_json::json!({ "type": "Noted", "note": "late" })
    );
    // An enum without events dispatches nothing; it only has to compile.
    let _: fn(&NoEvent) -> (&'static str, &'static str) = |never| (never.event_type(), never.event_version());
}

#[derive(Serialize, Task)]
#[task(name = "send_e_ticket", version = 2)]
struct SendETicket<'a> {
    seat: &'a str,
}

#[test]
fn a_derived_task_has_its_format_and_publishes() {
    assert_eq!((SendETicket::TASK_TYPE, SendETicket::TASK_VERSION), ("send_e_ticket", 2));
    assert_eq!(TaskFormat::of::<SendETicket>().to_string(), "send_e_ticket v2");
    let task = NewTask::try_new(SendETicket { seat: "B4" }).unwrap();
    assert_eq!(
        (task.format().to_string(), task.payload()),
        ("send_e_ticket v2".to_owned(), &json!({ "seat": "B4" }))
    );
}

/// A refund whose tenant and kind are said of the task, not part of its business data.
#[derive(Debug, PartialEq, Serialize, Deserialize, Task)]
#[serde(deny_unknown_fields)]
#[task(name = "issue_refund", version = 1)]
struct IssueRefund {
    #[label]
    tenant: String,
    amount: u32,
    #[label]
    r#type: u8,
}

/// Hands the decoded refund to the test.
struct Refunds(std::sync::Arc<std::sync::Mutex<Vec<IssueRefund>>>);

impl TaskHandler<IssueRefund> for Refunds {
    async fn handle(&self, refund: IssueRefund, _: &TaskContext) -> TaskResult {
        self.0.lock().unwrap().push(refund);
        TaskResult::Done
    }
}

#[tokio::test]
async fn a_derived_task_keeps_its_labels_in_the_metadata() {
    assert_eq!(IssueRefund::LABELS, ["tenant", "type"]);
    let refund = || IssueRefund {
        tenant: "acme".into(),
        amount: 80,
        r#type: 2,
    };
    let task = NewTask::try_new(refund()).unwrap();
    assert_eq!(task.payload(), &json!({ "amount": 80 }));
    assert_eq!(task.metadata(), &json!({ "tenant": "acme", "type": 2 }));
    // The runtime claims the row as it was written; the adapter puts the labels back before it decodes.
    let claim = TaskAttempt::try_new(
        task.id(),
        task.format().clone(),
        task.payload().clone(),
        task.metadata().clone(),
        1,
    )
    .unwrap();
    let decoded = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let adapter = JsonTaskAdapter::new(Refunds(decoded.clone()));
    let context = |attempt| TaskContext::new(attempt, std::time::Duration::ZERO);
    assert_eq!(adapter.invoke(&context(claim)).await, TaskResult::Done);
    assert_eq!(
        adapter
            .invoke(&context(TaskAttempt::try_first(&refund(), json!({})).unwrap()))
            .await,
        TaskResult::Done
    );
    assert_eq!(*decoded.lock().unwrap(), [refund(), refund()]);
}

#[derive(Debug, Serialize)]
struct Seat {
    row: char,
    number: u8,
}

#[derive(Debug, thiserror::Error, Rejection)]
enum BookingError<T: Serialize + Debug> {
    #[error("sold out")]
    SoldOut,
    #[error("seat taken")]
    SeatTaken { seat: Seat, by: String, booking_id: Uuid },
    #[error("wrong")]
    #[rejection(code = "r3_unknown")]
    Unknown { r#type: T },
    #[error("nothing")]
    Nothing {},
}

#[derive(Debug, Rejection)]
enum NoRejection {}

#[test]
fn derived_rejections_report_their_codes_and_fields() {
    let reported = [
        BookingError::SoldOut,
        BookingError::SeatTaken {
            seat: Seat { row: 'B', number: 4 },
            by: "Fox".into(),
            booking_id: Uuid::from_u128(1),
        },
        BookingError::Unknown { r#type: 9_u32 },
        BookingError::Nothing {},
    ]
    .iter()
    .map(|error| (error.code(), error.params()))
    .collect::<Vec<_>>();
    assert_eq!(
        reported,
        [
            ("sold_out", json!({})),
            (
                "seat_taken",
                json!({ "seat": { "row": "B", "number": 4 }, "by": "Fox", "booking_id": "00000000-0000-0000-0000-000000000001" })
            ),
            ("r3_unknown", json!({ "type": 9 })),
            ("nothing", json!({})),
        ]
    );
    // An enum without rejections reports nothing; it only has to compile.
    let _: fn(&NoRejection) -> (&'static str, serde_json::Value) = |never| (never.code(), never.params());
}

#[derive(Command)]
struct BookSeat {
    seat: String,
    #[label]
    fan_id: String,
    #[command_id]
    booking_id: Uuid,
    #[label]
    r#type: u8,
}

#[derive(Command)]
struct Ping;

#[derive(Command)]
struct Note<T: Display> {
    #[label]
    note: T,
}

#[test]
fn a_derived_command_reports_its_name_id_and_labels_in_field_order() {
    let book = BookSeat {
        seat: "B4".into(),
        fan_id: "fox".into(),
        booking_id: Uuid::from_u128(7),
        r#type: 2,
    };
    assert_eq!(
        (book.name(), book.command_id(), book.labels()),
        (
            "BookSeat",
            Some(Uuid::from_u128(7)),
            vec![
                Label {
                    name: "fan_id",
                    value: "fox".to_owned()
                },
                Label {
                    name: "type",
                    value: "2".to_owned()
                }
            ]
        )
    );
    assert_eq!(book.seat, "B4", "a field without an attribute is the command's only");
    assert_eq!((Ping.name(), Ping.command_id(), Ping.labels()), ("Ping", None, vec![]));
    let note = Note { note: 'x' };
    assert_eq!(
        (note.name(), note.labels()),
        (
            "Note",
            vec![Label {
                name: "note",
                value: "x".to_owned()
            }]
        )
    );
}

/// A tally that adds and resets: two command types, each deciding in its own `impl`.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Tally(u32);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Counted {
    Added(u32),
    Reset,
}

impl DomainEvent for Counted {
    fn event_type(&self) -> &'static str {
        "counted"
    }
    fn event_version(&self) -> &'static str {
        "1"
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("over the limit")]
struct OverTheLimit;

impl Aggregate for Tally {
    const TYPE: &'static str = "tally";
    type Id = String;
    type Command = TallyCommand;
    type Event = Counted;
    type Error = OverTheLimit;
    type Rules = u32;

    fn apply(&mut self, event: Counted) {
        match event {
            Counted::Added(value) => self.0 += value,
            Counted::Reset => self.0 = 0,
        }
    }
}

#[derive(Command)]
struct Add {
    #[command_id]
    id: Uuid,
    value: u32,
}

impl Decide<Tally> for Add {
    fn decide(self, tally: &Tally, limit: &u32) -> Result<Counted, OverTheLimit> {
        if tally.0 + self.value > *limit {
            return Err(OverTheLimit);
        }
        Ok(Counted::Added(self.value))
    }
}

#[derive(Command)]
struct Reset {
    #[label]
    by: String,
}

impl Decide<Tally> for Reset {
    fn decide(self, _: &Tally, _: &u32) -> Result<Counted, OverTheLimit> {
        Ok(Counted::Reset)
    }
}

#[derive(Commands)]
enum TallyCommand {
    Add(Add),
    Reset(Reset),
}

/// A generic enum of commands: one variant, its type a parameter.
#[derive(Commands)]
enum Only<C> {
    One(C),
}

#[derive(Commands)]
enum NoCommand {}

#[test]
fn derived_commands_route_every_variant_to_its_decision_and_report_it() {
    let add = |value| {
        TallyCommand::Add(Add {
            id: Uuid::from_u128(1),
            value,
        })
    };
    TestFramework::<Tally>::new(5)
        .given(vec![Counted::Added(3)])
        .when(add(2))
        .then_expect_event(&Counted::Added(2));
    TestFramework::<Tally>::new(5)
        .given(vec![Counted::Added(3)])
        .when(add(3))
        .then_expect_error(&OverTheLimit);
    TestFramework::<Tally>::new(5)
        .given(vec![Counted::Added(3)])
        .when(TallyCommand::Reset(Reset { by: "ops".into() }))
        .then_expect_event(&Counted::Reset);
    let reset = TallyCommand::Reset(Reset { by: "ops".into() });
    assert_eq!(
        [
            (add(1).name(), add(1).command_id(), add(1).labels()),
            (reset.name(), reset.command_id(), reset.labels())
        ],
        [
            ("Add", Some(Uuid::from_u128(1)), vec![]),
            (
                "Reset",
                None,
                vec![Label {
                    name: "by",
                    value: "ops".to_owned()
                }]
            )
        ]
    );
    let only = Only::One(Reset { by: "ops".into() });
    assert_eq!(
        (only.name(), only.labels()),
        (
            "Reset",
            vec![Label {
                name: "by",
                value: "ops".to_owned()
            }]
        )
    );
    assert_eq!(
        Decide::<Tally>::decide(only, &Tally(4), &5),
        Ok(Counted::Reset),
        "a generic enum decides through its variant's type"
    );
    // An enum without commands decides nothing; it only has to compile.
    let _: fn(NoCommand) -> Result<Counted, OverTheLimit> = |never| never.decide(&Tally(0), &0);
    let _: fn(&NoCommand) -> &'static str = |never| never.name();
}

/// What a task handler may be given besides its task: a port, by a trait object.
trait Clock: Send + Sync {
    fn now(&self) -> u64;
}

/// The handlers' dependency, by this type.
type Time = dyn Clock;

struct Fixed(u64);

impl Clock for Fixed {
    fn now(&self) -> u64 {
        self.0
    }
}

/// Stamps a refund with the time and the attempt, and asks for a retry with them as the reason.
#[task_handler]
async fn stamp_refund(refund: IssueRefund, clock: &Time, ctx: &TaskContext, prefix: &String) -> TaskResult {
    TaskResult::retry(format!(
        "{prefix} {} at {} on attempt {}",
        refund.tenant,
        clock.now(),
        ctx.attempt().number()
    ))
}

#[tokio::test]
async fn a_task_handler_function_is_a_handler_with_its_context_and_its_dependencies() {
    assert_eq!(
        <stamp_refund as TaskHandler<IssueRefund>>::DEPENDENCIES,
        [DependencyId::of::<Time>(), DependencyId::of::<String>()]
    );
    let refund = || IssueRefund {
        tenant: "acme".into(),
        amount: 80,
        r#type: 2,
    };
    let ctx = TaskContext::new(
        TaskAttempt::try_first(&refund(), json!({})).unwrap(),
        std::time::Duration::ZERO,
    );
    // Called as a function, with its dependencies by hand.
    assert_eq!(
        stamp_refund.call(refund(), &Fixed(7), &ctx, &"refund".to_owned()).await,
        TaskResult::retry("refund acme at 7 on attempt 1")
    );
    // Registered as a handler, with its dependencies from the assembly.
    let clock: std::sync::Arc<Time> = std::sync::Arc::new(Fixed(9));
    let dispatcher = TaskHandlers::new()
        .handler(stamp_refund)
        .provide(clock)
        .provide(std::sync::Arc::new("stamped".to_owned()))
        .build()
        .unwrap();
    let claim = TaskAttempt::try_first(&refund(), json!({})).unwrap();
    assert_eq!(
        dispatcher.dispatch(Arc::new(claim), std::time::Duration::ZERO).await,
        TaskResult::retry("stamped acme at 9 on attempt 1")
    );
    // A context without the dependency: the handler fails the task and names it.
    let bare = TaskContext::new(
        TaskAttempt::try_first(&refund(), json!({})).unwrap(),
        std::time::Duration::ZERO,
    );
    assert_eq!(
        stamp_refund.handle(refund(), &bare).await,
        TaskResult::fail(format!(
            "the dependency `{}` is not provided",
            DependencyId::of::<Time>().name()
        ))
    );
    assert_eq!(format!("{stamp_refund:?}"), "stamp_refund");
}

#[test]
fn assembly_refuses_a_task_handler_whose_dependency_nobody_provides() {
    let clock: std::sync::Arc<Time> = std::sync::Arc::new(Fixed(9));
    assert_eq!(
        TaskHandlers::new().handler(stamp_refund).provide(clock).build().err(),
        Some(TaskDispatcherError::MissingDependency {
            format: TaskFormat::of::<IssueRefund>(),
            dependency: DependencyId::of::<String>().name(),
        })
    );
}
