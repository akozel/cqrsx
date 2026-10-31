//! The modules of the engine's tests: a box office whose bookings go into the fans' wallets by a step of a chain, whose
//! recorded tickets are mailed through a dependency, with a view of the halls; written with the derive macros.
use cqrsx::{
    Command, Event, Events, Rejection, Task,
    command::CommandGatewayError,
    cqrs::projection::View,
    domain::{aggregate::Aggregate, command::Decide, event::EventEnvelope},
    engine::{
        module::Module,
        setup::{Setup, SetupError},
    },
    reactions::{Decided, Reactions, Then},
    task::{
        context::TaskContext,
        policy::{PolicyName, Retry},
        result::TaskResult,
    },
    task_handler,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    num::NonZeroU16,
    sync::{Arc, Mutex},
    time::Duration,
};
use uuid::Uuid;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Show {
    pub(crate) seats: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(name = "seat_booked", version = "1")]
pub(crate) struct SeatBooked {
    pub(crate) seat: String,
    pub(crate) fan: String,
    pub(crate) booking_id: Uuid,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Events)]
pub(crate) enum ShowEvent {
    SeatBooked(SeatBooked),
}

/// A booking: its id is the command's.
#[derive(Clone, Debug, Command)]
pub(crate) struct BookSeat {
    pub(crate) seat: String,
    #[label]
    pub(crate) fan: String,
    #[command_id]
    pub(crate) booking_id: Uuid,
}

#[derive(Debug, PartialEq, thiserror::Error, Rejection)]
pub(crate) enum ShowError {
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
pub(crate) struct Wallet {
    pub(crate) tickets: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(name = "ticket_recorded", version = "1")]
pub(crate) struct TicketRecorded {
    pub(crate) seat: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Events)]
pub(crate) enum WalletEvent {
    TicketRecorded(TicketRecorded),
}

#[derive(Clone, Debug, Command)]
pub(crate) struct Record {
    pub(crate) seat: String,
}

#[derive(Debug, PartialEq, thiserror::Error, Rejection)]
pub(crate) enum WalletError {
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
pub(crate) struct RecordTicket {
    pub(crate) fan: String,
    pub(crate) seat: String,
}

/// Records the ticket; one recorded already is done.
#[task_handler]
pub(crate) async fn record_ticket(task: RecordTicket, ctx: &TaskContext) -> TaskResult {
    match ctx.execute::<Wallet>(&task.fan, Record { seat: task.seat }).await {
        Err(CommandGatewayError::Domain(WalletError::AlreadyRecorded { .. })) => TaskResult::done(),
        other => other.into(),
    }
}

/// The e-mail of a ticket, to the fan its label names.
#[derive(Debug, Serialize, Deserialize, Task)]
#[task(name = "mail_ticket", version = 1)]
pub(crate) struct MailTicket {
    #[label]
    pub(crate) to: String,
    pub(crate) seat: String,
}

pub(crate) trait MailService: Send + Sync {
    fn send(&self, to: &str, seat: &str);
}

/// The handlers' dependency: the mail service, by this type.
pub(crate) type Mailer = dyn MailService;

#[derive(Default)]
pub(crate) struct Outbox(pub(crate) Mutex<Vec<String>>);

impl MailService for Outbox {
    fn send(&self, to: &str, seat: &str) {
        self.0.lock().unwrap().push(format!("{to}:{seat}"));
    }
}

#[task_handler]
pub(crate) async fn mail_ticket(task: MailTicket, mail: &Mailer) -> TaskResult {
    mail.send(&task.to, &task.seat);
    TaskResult::done()
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Hall {
    pub(crate) seats: BTreeMap<String, String>,
}

impl View<Show> for Hall {
    const NAME: &'static str = "halls";

    fn update(&mut self, event: &EventEnvelope<Show>) {
        let ShowEvent::SeatBooked(booked) = &event.payload;
        self.seats.insert(booked.seat.clone(), booked.fan.clone());
    }
}

pub(crate) const STEP: PolicyName = PolicyName::try_new("step").unwrap();
pub(crate) const MAIL: PolicyName = PolicyName::try_new("mail").unwrap();
/// The budget of both policies without `ATTEMPTS`.
const ATTEMPTS: NonZeroU16 = NonZeroU16::new(3).unwrap();

/// The box office: the shows and their halls.
pub(crate) fn box_office(_: &Setup) -> Result<Module, SetupError> {
    Ok(Module::new("box_office").aggregate::<Show>(()).view::<Hall, Show>())
}

/// The wallets: the step (`STEP`, a second between attempts) and the e-mail (`MAIL`), both with the budget of
/// `ATTEMPTS`, and a mail service of their own.
pub(crate) fn wallets(setup: &Setup) -> Result<Module, SetupError> {
    let attempts = setup.env("ATTEMPTS", ATTEMPTS)?;
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
