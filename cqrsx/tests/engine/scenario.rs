//! The path of `tests/cqrs/planning.rs` through modules on the engine in memory: a box office whose bookings go into
//! the fans' wallets by a step of a chain, whose recorded tickets are mailed through a dependency, with a view of the
//! halls; the commands, the tasks, the view and the chain's causation, written with the derive macros.
use cqrsx::{
    Command, Event, Events, Rejection, Task,
    cqrs::{
        aggregate::Aggregate,
        command::{CommandGatewayError, causation::CORRELATION_ID, decide::Decide},
        event::EventEnvelope,
        projection::View,
        reactions::{Decided, Reactions, Then},
        store::repository::PersistedEventRepository,
    },
    engine::{
        Cqrs,
        module::Module,
        setup::{Setup, SetupError},
    },
    task::{
        context::TaskContext,
        policy::{PolicyName, Retry},
        result::TaskResult,
        testing::mem_task_queue::TaskStatus,
    },
    task_handler,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use uuid::Uuid;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Show {
    seats: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(name = "seat_booked", version = "1")]
struct SeatBooked {
    seat: String,
    fan: String,
    booking_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Events)]
enum ShowEvent {
    SeatBooked(SeatBooked),
}

/// A booking: its id is the command's.
#[derive(Clone, Debug, Command)]
struct BookSeat {
    seat: String,
    #[label]
    fan: String,
    #[command_id]
    booking_id: Uuid,
}

#[derive(Debug, PartialEq, thiserror::Error, Rejection)]
enum ShowError {
    #[error("seat {seat} is taken")]
    SeatTaken { seat: String },
}

impl Aggregate for Show {
    const TYPE: &'static str = "show";
    type Id = String;
    type Command = BookSeat;
    type Event = ShowEvent;
    type Error = ShowError;
    type Rules = ();

    fn apply(&mut self, ShowEvent::SeatBooked(booked): ShowEvent) {
        self.seats.insert(booked.seat, booked.fan);
    }
}

impl Decide<Show> for BookSeat {
    fn decide(self, show: &Show, _: &()) -> Result<ShowEvent, ShowError> {
        if show.seats.contains_key(&self.seat) {
            return Err(ShowError::SeatTaken { seat: self.seat });
        }
        Ok(ShowEvent::SeatBooked(SeatBooked {
            seat: self.seat,
            fan: self.fan,
            booking_id: self.booking_id,
        }))
    }
}

/// A booked seat goes into the fan's wallet.
impl Reactions for Show {
    fn react(decided: &Decided<'_, Self>, then: &mut Then) {
        match decided.event {
            ShowEvent::SeatBooked(booked) => then.task(RecordTicket {
                fan: booked.fan.clone(),
                seat: booked.seat.clone(),
            }),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Wallet {
    tickets: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(name = "ticket_recorded", version = "1")]
struct TicketRecorded {
    seat: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Events)]
enum WalletEvent {
    TicketRecorded(TicketRecorded),
}

#[derive(Clone, Debug, Command)]
struct Record {
    seat: String,
}

#[derive(Debug, PartialEq, thiserror::Error, Rejection)]
enum WalletError {
    #[error("ticket {seat} is recorded already")]
    AlreadyRecorded { seat: String },
}

impl Aggregate for Wallet {
    const TYPE: &'static str = "wallet";
    type Id = String;
    type Command = Record;
    type Event = WalletEvent;
    type Error = WalletError;
    type Rules = ();

    fn apply(&mut self, WalletEvent::TicketRecorded(recorded): WalletEvent) {
        self.tickets.push(recorded.seat);
    }
}

impl Decide<Wallet> for Record {
    fn decide(self, wallet: &Wallet, _: &()) -> Result<WalletEvent, WalletError> {
        if wallet.tickets.contains(&self.seat) {
            return Err(WalletError::AlreadyRecorded { seat: self.seat });
        }
        Ok(WalletEvent::TicketRecorded(TicketRecorded { seat: self.seat }))
    }
}

/// A recorded ticket is mailed to its fan.
impl Reactions for Wallet {
    fn react(decided: &Decided<'_, Self>, then: &mut Then) {
        match decided.event {
            WalletEvent::TicketRecorded(recorded) => then.task(MailTicket {
                to: decided.id.clone(),
                seat: recorded.seat.clone(),
            }),
        }
    }
}

/// The step of the chain: the ticket goes into the fan's wallet.
#[derive(Debug, Serialize, Deserialize, Task)]
#[task(name = "record_ticket", version = 1)]
struct RecordTicket {
    fan: String,
    seat: String,
}

/// Records the ticket; one recorded already is done.
#[task_handler]
async fn record_ticket(task: RecordTicket, ctx: &TaskContext) -> TaskResult {
    match ctx.execute::<Wallet>(&task.fan, Record { seat: task.seat }).await {
        Err(CommandGatewayError::Domain(WalletError::AlreadyRecorded { .. })) => TaskResult::done(),
        other => other.into(),
    }
}

/// The e-mail of a ticket, to the fan its label names.
#[derive(Debug, Serialize, Deserialize, Task)]
#[task(name = "mail_ticket", version = 1)]
struct MailTicket {
    #[label]
    to: String,
    seat: String,
}

trait MailService: Send + Sync {
    fn send(&self, to: &str, seat: &str);
}

/// The handlers' dependency: the mail service, by this type.
type Mailer = dyn MailService;

#[derive(Default)]
struct Outbox(Mutex<Vec<String>>);

impl MailService for Outbox {
    fn send(&self, to: &str, seat: &str) {
        self.0.lock().unwrap().push(format!("{to}:{seat}"));
    }
}

#[task_handler]
async fn mail_ticket(task: MailTicket, mail: &Mailer) -> TaskResult {
    mail.send(&task.to, &task.seat);
    TaskResult::done()
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
struct Hall {
    seats: BTreeMap<String, String>,
}

impl View<Show> for Hall {
    const NAME: &'static str = "halls";

    fn update(&mut self, event: &EventEnvelope<Show>) {
        let ShowEvent::SeatBooked(booked) = &event.payload;
        self.seats.insert(booked.seat.clone(), booked.fan.clone());
    }
}

const STEP: PolicyName = PolicyName::new("step");
const MAIL: PolicyName = PolicyName::new("mail");

/// The box office: the shows and their halls.
fn box_office(_: &Setup) -> Result<Module, SetupError> {
    Ok(Module::new("box_office").aggregate::<Show>(()).view::<Hall, Show>())
}

/// The wallets: the step (`STEP`, a second between attempts) and the e-mail (`MAIL`), both with the budget of
/// `ATTEMPTS`, and a mail service of their own.
fn wallets(setup: &Setup) -> Result<Module, SetupError> {
    let attempts = setup.env("ATTEMPTS", 3)?;
    let mail: Arc<Mailer> = Arc::new(Outbox::default());
    Ok(Module::new("wallets")
        .aggregate::<Wallet>(())
        .provide(mail)
        .policy(STEP, Retry::fixed(Duration::from_secs(1)), attempts)
        .policy(MAIL, Retry::backoff(Duration::from_secs(1), Duration::from_secs(8)), attempts)
        .task(record_ticket)
        .policy(STEP)
        .task(mail_ticket)
        .policy(MAIL))
}

#[tokio::test]
async fn the_box_office_and_the_wallets_run_through_their_modules_in_memory() {
    let outbox = Arc::new(Outbox::default());
    let mail: Arc<Mailer> = outbox.clone();
    let cqrs = Cqrs::in_memory()
        .env("SHOP", |name| (name == "SHOP_ATTEMPTS").then(|| "5".to_owned()))
        .module(box_office)
        .module(wallets)
        .provide(mail)
        .build()
        .unwrap();
    let ui = cqrs.from("ui");
    let booking_id = Uuid::now_v7();
    let book = |fan: &str, booking_id| BookSeat {
        seat: "A1".into(),
        fan: fan.into(),
        booking_id,
    };
    let booked = ui.execute::<Show>(&"s-1".into(), book("fox", booking_id)).await.unwrap();
    assert_eq!(booked.tasks[0].max_attempts(), 5, "the policy's budget from SHOP_ATTEMPTS");
    assert_eq!(
        ui.execute::<Show>(&"s-1".into(), book("panda", Uuid::now_v7()))
            .await
            .unwrap_err(),
        CommandGatewayError::Domain(ShowError::SeatTaken { seat: "A1".into() })
    );

    assert_eq!(cqrs.run_tasks().await, 2, "the step, then the e-mail its command published");
    assert_eq!(*outbox.0.lock().unwrap(), ["fox:A1"]);
    let tasks = cqrs.outbox().tasks();
    let delivered: Vec<(&str, TaskStatus)> = tasks
        .iter()
        .map(|task| (task.entry.task.format().task_type(), task.status))
        .collect();
    assert_eq!(
        delivered,
        [
            ("record_ticket", TaskStatus::Completed),
            ("mail_ticket", TaskStatus::Completed)
        ]
    );
    let chain = booking_id.to_string();
    assert_eq!(
        tasks[1].entry.task.metadata()[CORRELATION_ID],
        chain,
        "one correlation id along the chain"
    );
    assert_eq!(
        tasks[1].entry.task.metadata()["to"],
        "fox",
        "the label is kept in the metadata"
    );
    let recorded = cqrs.journal().get_events::<Wallet>("fox").await.unwrap();
    assert_eq!(recorded[0].metadata[CORRELATION_ID], chain);

    cqrs.catch_up().await;
    let hall = cqrs.view::<Hall>().get(&"s-1").await.unwrap().unwrap();
    assert_eq!(hall.view.seats, BTreeMap::from([("A1".to_owned(), "fox".to_owned())]));
    assert_eq!(cqrs.views().verify("halls").await.unwrap().differences, []);
}
