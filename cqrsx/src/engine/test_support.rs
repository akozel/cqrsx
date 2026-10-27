//! Fixtures of the engine's tests: a box office of shows and the fans' wallets as two modules, a step of a chain and an
//! e-mail between them, a view of each.
use crate::{
    cqrs::{
        aggregate::Aggregate,
        command::{CommandGatewayError, CommandInfo, decide::Decide},
        event::{DomainEvent, EventEnvelope},
        projection::View,
        reactions::{Decided, Reactions, Then},
        rejection::Rejection,
    },
    engine::{
        Cqrs,
        module::Module,
        setup::{Setup, SetupError},
    },
    task::{
        context::TaskContext,
        dependencies::DependencyId,
        format::TaskPayload,
        handler::TaskHandler,
        policy::{PolicyName, Retry},
        result::TaskResult,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use uuid::Uuid;

/// The box office of a show: a seat goes to one fan.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Show {
    seats: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum ShowEvent {
    Booked { seat: String, fan: String },
}

impl DomainEvent for ShowEvent {
    fn event_type(&self) -> String {
        "booked".into()
    }

    fn event_version(&self) -> String {
        "1".into()
    }
}

/// A booking: its id, when it has one, is the command's.
#[derive(Clone, Debug)]
pub(crate) struct Book {
    pub(crate) seat: String,
    pub(crate) fan: String,
    pub(crate) booking_id: Option<Uuid>,
}

impl CommandInfo for Book {
    fn name(&self) -> &'static str {
        "Book"
    }

    fn command_id(&self) -> Option<Uuid> {
        self.booking_id
    }

    fn labels(&self) -> Vec<(&'static str, String)> {
        vec![("fan", self.fan.clone())]
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("seat taken")]
pub(crate) struct SeatTaken;

impl Rejection for SeatTaken {
    fn code(&self) -> &'static str {
        "seat_taken"
    }

    fn params(&self) -> Value {
        json!({})
    }
}

impl Aggregate for Show {
    const TYPE: &'static str = "show";
    type Id = String;
    type Command = Book;
    type Event = ShowEvent;
    type Error = SeatTaken;
    type Rules = ();

    fn apply(&mut self, ShowEvent::Booked { seat, fan }: ShowEvent) {
        self.seats.insert(seat, fan);
    }
}

impl Decide<Show> for Book {
    fn decide(self, show: &Show, _: &()) -> Result<ShowEvent, SeatTaken> {
        if show.seats.contains_key(&self.seat) {
            return Err(SeatTaken);
        }
        Ok(ShowEvent::Booked {
            seat: self.seat,
            fan: self.fan,
        })
    }
}

/// A booked seat goes into the fan's wallet.
impl Reactions for Show {
    fn react(decided: &Decided<'_, Self>, then: &mut Then) {
        let ShowEvent::Booked { seat, fan } = decided.event;
        then.task(RecordTicket {
            fan: fan.clone(),
            seat: seat.clone(),
        });
    }
}

/// A fan's wallet: every ticket once, so a redelivered step is refused.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Wallet {
    tickets: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum WalletEvent {
    Recorded { seat: String },
}

impl DomainEvent for WalletEvent {
    fn event_type(&self) -> String {
        "recorded".into()
    }

    fn event_version(&self) -> String {
        "1".into()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Record {
    pub(crate) seat: String,
}

impl CommandInfo for Record {
    fn name(&self) -> &'static str {
        "Record"
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("already recorded")]
pub(crate) struct AlreadyRecorded;

impl Rejection for AlreadyRecorded {
    fn code(&self) -> &'static str {
        "already_recorded"
    }

    fn params(&self) -> Value {
        json!({})
    }
}

impl Aggregate for Wallet {
    const TYPE: &'static str = "wallet";
    type Id = String;
    type Command = Record;
    type Event = WalletEvent;
    type Error = AlreadyRecorded;
    type Rules = ();

    fn apply(&mut self, WalletEvent::Recorded { seat }: WalletEvent) {
        self.tickets.push(seat);
    }
}

impl Decide<Wallet> for Record {
    fn decide(self, wallet: &Wallet, _: &()) -> Result<WalletEvent, AlreadyRecorded> {
        if wallet.tickets.contains(&self.seat) {
            return Err(AlreadyRecorded);
        }
        Ok(WalletEvent::Recorded { seat: self.seat })
    }
}

/// A recorded ticket is mailed to its fan.
impl Reactions for Wallet {
    fn react(decided: &Decided<'_, Self>, then: &mut Then) {
        let WalletEvent::Recorded { seat } = decided.event;
        then.task(MailTicket {
            to: decided.id.clone(),
            seat: seat.clone(),
        });
    }
}

/// The step of the chain: the ticket goes into the fan's wallet.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RecordTicket {
    pub(crate) fan: String,
    pub(crate) seat: String,
}

impl TaskPayload for RecordTicket {
    const TASK_TYPE: &'static str = "record_ticket";
    const TASK_VERSION: i32 = 1;
}

/// Records the ticket; one already recorded is done.
pub(crate) struct RecordInWallet;

impl TaskHandler<RecordTicket> for RecordInWallet {
    async fn handle(&self, task: RecordTicket, ctx: &TaskContext) -> TaskResult {
        match ctx.execute::<Wallet>(&task.fan, Record { seat: task.seat }).await {
            Err(CommandGatewayError::Domain(AlreadyRecorded)) => TaskResult::Done,
            other => other.into(),
        }
    }
}

/// The e-mail of a ticket.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MailTicket {
    pub(crate) to: String,
    pub(crate) seat: String,
}

impl TaskPayload for MailTicket {
    const TASK_TYPE: &'static str = "mail_ticket";
    const TASK_VERSION: i32 = 1;
}

/// A mail service that records the letters and refuses the first `refusals` of them.
#[derive(Default)]
pub(crate) struct Mailbox {
    pub(crate) sent: Mutex<Vec<String>>,
    pub(crate) refusals: AtomicUsize,
}

impl Mailbox {
    pub(crate) fn sent(&self) -> Vec<String> {
        self.sent.lock().unwrap().clone()
    }
}

/// Mails the ticket.
pub(crate) struct SendMail;

impl TaskHandler<MailTicket> for SendMail {
    const DEPENDENCIES: &'static [DependencyId] = &[DependencyId::of::<Mailbox>()];

    async fn handle(&self, task: MailTicket, ctx: &TaskContext) -> TaskResult {
        let mailbox = ctx.dependency::<Mailbox>().expect("the build checks the dependencies");
        if mailbox
            .refusals
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return TaskResult::retry("mailbox busy");
        }
        mailbox.sent.lock().unwrap().push(format!("{}:{}", task.to, task.seat));
        TaskResult::Done
    }
}

/// The seats of a show and their fans.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Hall {
    pub(crate) seats: BTreeMap<String, String>,
}

impl View<Show> for Hall {
    const NAME: &'static str = "halls";

    fn update(&mut self, event: &EventEnvelope<Show>) {
        let ShowEvent::Booked { seat, fan } = &event.payload;
        self.seats.insert(seat.clone(), fan.clone());
    }
}

/// The tickets of a fan.
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Tickets {
    pub(crate) seats: Vec<String>,
}

impl View<Wallet> for Tickets {
    const NAME: &'static str = "tickets";

    fn update(&mut self, event: &EventEnvelope<Wallet>) {
        let WalletEvent::Recorded { seat } = &event.payload;
        self.seats.push(seat.clone());
    }
}

/// A step of the chain waits a second.
pub(crate) const STEP: PolicyName = PolicyName::new("step");
/// An e-mail backs off from a second to eight.
pub(crate) const MAIL: PolicyName = PolicyName::new("mail");

/// The box office: the shows and their halls.
pub(crate) fn shows(_: &Setup) -> Result<Module, SetupError> {
    Ok(Module::new("shows").aggregate::<Show>(()).view::<Hall, Show>())
}

/// The wallets: their tickets, the step that records one (`STEP`, three attempts) and its e-mail (`MAIL`, six), and a
/// mailbox of their own.
pub(crate) fn wallets(_: &Setup) -> Result<Module, SetupError> {
    Ok(Module::new("wallets")
        .aggregate::<Wallet>(())
        .view::<Tickets, Wallet>()
        .provide(Arc::new(Mailbox::default()))
        .policy(STEP, Retry::fixed(Duration::from_secs(1)), 3)
        .policy(MAIL, Retry::backoff(Duration::from_secs(1), Duration::from_secs(8)), 6)
        .task(RecordInWallet)
        .policy(STEP)
        .task(SendMail)
        .policy(MAIL))
}

/// Both modules in memory with a mailbox of the test instead of the wallets' own.
pub(crate) async fn engine() -> (Cqrs, Arc<Mailbox>) {
    let mailbox = Arc::new(Mailbox::default());
    let cqrs = Cqrs::in_memory()
        .module(shows)
        .module(wallets)
        .provide(mailbox.clone())
        .build()
        .await
        .unwrap();
    (cqrs, mailbox)
}

/// A booking of `seat` by `fan`, without an id of its own.
pub(crate) fn book(seat: &str, fan: &str) -> Book {
    Book {
        seat: seat.into(),
        fan: fan.into(),
        booking_id: None,
    }
}

/// The output of `future`, which must not wait: an engine in memory builds without one.
pub(crate) fn ready<F: std::future::Future>(future: F) -> F::Output {
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).poll(&mut context) {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("the future waits"),
    }
}
