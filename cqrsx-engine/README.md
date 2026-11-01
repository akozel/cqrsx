# cqrsx-engine

The engine of [cqrsx](../cqrsx/README.md): the application's modules assembled into one running application over
in-memory, PostgreSQL and MongoDB stores, with its settings read from the environment and, with the feature `http`,
served until a signal. The components it assembles, the contracts of an application and the rules are in the README of
the core.

The application registers its pieces in modules and builds `Cqrs` in memory or with PostgreSQL and MongoDB.
The [typical scenario](../doc/typical_scenario.md) follows a cancellation through a refund.

## Quick start

This complete example is adapted from [`tests/engine/modules.rs`](tests/engine/modules.rs) and
[`tests/engine/scenario.rs`](tests/engine/scenario.rs): a box office books a seat, a task records a ticket
in the fan's wallet, a second task mails it, and the hall view catches up. It runs without databases.

```rust
use cqrsx::{
    command::CommandError,
    domain::{aggregate::Aggregate, command::{Command, Decide}, event::{Event, EventEnvelope, Events}, rejection::Rejection},
    reactions::{Decided, Reactions, Then},
    task::{Task, policy::{PolicyName, RetryPause}},
    view::View,
    worker::{TaskResult, context::TaskContext, task_handler},
};
use cqrsx_engine::{Cqrs, module::Module, setup::{Setup, SetupError}};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    num::NonZeroU16,
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
    type Rejection = ShowError;
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
    type Rejection = WalletError;
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
                to: decided.aggregate_id.clone(),
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
        Err(CommandError::Rejected(WalletError::AlreadyRecorded { .. })) => TaskResult::done(),
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

    fn apply(&mut self, event: &EventEnvelope<Show>) {
        let ShowEvent::SeatBooked(booked) = &event.payload;
        self.seats.insert(booked.seat.clone(), booked.fan.clone());
    }
}

const STEP: PolicyName = PolicyName::try_new("step").unwrap();
const MAIL: PolicyName = PolicyName::try_new("mail").unwrap();
/// The budget of both policies without `ATTEMPTS`.
const ATTEMPTS: NonZeroU16 = NonZeroU16::new(3).unwrap();

/// The box office: the shows and their halls.
fn box_office(_: &Setup) -> Result<Module, SetupError> {
    Ok(Module::new("box_office").aggregate::<Show>(()).view::<Hall, Show>())
}

/// The wallets: the step (`STEP`, a second between attempts) and the e-mail (`MAIL`), both with the budget of
/// `ATTEMPTS`, and a mail service of their own.
fn wallets(setup: &Setup) -> Result<Module, SetupError> {
    let attempts = setup.env("ATTEMPTS", ATTEMPTS)?;
    let mail: Arc<Mailer> = Arc::new(Outbox::default());
    Ok(Module::new("wallets")
        .aggregate::<Wallet>(())
        .provide(mail)
        .policy(STEP, RetryPause::fixed(Duration::from_secs(1)), attempts)
        .policy(MAIL, RetryPause::backoff(Duration::from_secs(1), Duration::from_secs(8)), attempts)
        .task(record_ticket)
        .policy(STEP)
        .task(mail_ticket)
        .policy(MAIL))
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let outbox = Arc::new(Outbox::default());
    let mail: Arc<Mailer> = outbox.clone();
    let cqrs = Cqrs::in_memory().module(box_office).module(wallets)
        .provide(mail).build().await.unwrap();
    let booking_id = Uuid::now_v7();
    cqrs.source("ui").execute::<Show>(&"show-1".into(), BookSeat {
        seat: "A1".into(), fan: "fox".into(), booking_id,
    }).await.unwrap();
    assert_eq!(cqrs.run_tasks().await, 2); // record the ticket, then mail it
    assert_eq!(*outbox.0.lock().unwrap(), ["fox:A1"]);
    cqrs.catch_up().await;
    let hall = cqrs.view::<Hall>().get(&"show-1").await.unwrap().unwrap();
    assert_eq!(hall.view.seats.get("A1").map(String::as_str), Some("fox"));
}
```


## Running on PostgreSQL

The complete [`demo/back-end/src/main.rs`](../demo/back-end/src/main.rs) below uses the `postgres`, `mongodb`
and `http` features; it is application source, not a standalone example. `DEMO_DEFAULTS` and the module roots
are defined by `encore_backend`. The engine opens PostgreSQL pools and configures the MongoDB read side, then
serves the module routes alongside the workers and projection processors.

```text
//! Encore back-end: the box office on the cqrsx engine. The engine reads its settings from the `ENCORE_*` variables
//! (and the library's `OUTBOX_*`), keeps the journal in PostgreSQL and the read models in MongoDB, and serves the HTTP
//! API of the modules next to the task runtime and the projection processors until a signal.
use cqrsx_engine::{Cqrs, mongodb::Mongo, postgres::Postgres};
use encore_backend::{DEMO_DEFAULTS, concert, fan, operations};

fn fail(message: impl std::fmt::Display) -> ! {
    tracing::error!("{message}");
    eprintln!("encore-backend: {message}");
    std::process::exit(2)
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let served = Cqrs::from_env("ENCORE")
        .defaults(DEMO_DEFAULTS)
        .event_store(Postgres)
        .projections(Mongo)
        .module(concert::module)
        .module(fan::module)
        .module(operations::module)
        .serve()
        .await;
    if let Err(error) = served {
        fail(error)
    }
}
```

## Configuration

Every tunable is a checked value with a default. The engine reads its settings from variables under the
application's prefix (`Cqrs::from_env("SHOP")`: `SHOP_SNAPSHOT_EVERY`, …) and the task runtime's `OUTBOX_*` as they
are, with the application's `Defaults` where a variable is absent; `settings().description()` lists what it uses. A
module reads its own variables through its `Setup`, under the same prefix. Read by hand, a component's settings come
from two helpers that say so in their names.

| Variable (after the prefix) | Setting | Default |
|---|---|---|
| `SNAPSHOT_EVERY` | a snapshot every N events | 10 |
| `COMMAND_LOCK_MAX_WAIT_MS`, `COMMAND_LOCK_MAX_WAITERS` | the lock limits (`LockLimits`); the wait below `OUTBOX_HANDLER_TIMEOUT_SECS` | 1 s, 128 |
| `PROJECTION_POLL_MS`, `_PAGE`, `_LEASE_MS`, `_RETRY_BASE_MS`, `_RETRY_MAX_MS`, `_CONCURRENCY`, `_BUFFER`, `_MAX_BATCH`, `_REBUILD_PAGE`, `_SETTLE_MS` | every processor (`ProcessorConfig`, `DeliveryLimits`) | the table below |
| `PROJECTION_LAG_CAP`, `PROJECTION_STATUS_TIMEOUT_MS` | the cap of a view's lag, the wait of an operator's status | 100 000, 1 s |
| `NODE_NAME` | the owner of this node's checkpoints | a fresh id |
| `DATABASE_URL`, `DATABASE_POOL_SIZE`, `DATABASE_STATEMENT_TIMEOUT_MS`, `DATABASE_HISTORY_POOL_SIZE`, `DATABASE_PROJECTION_POOL_SIZE` | the three pools of PostgreSQL (`PostgresSettings::pool_plan`; 0 ms: no cap) | `postgresql://cqrsx:cqrsx@localhost:5433/cqrsx`, 10, 4 s, 1, 4 |
| `MONGODB_URI`, `MONGODB_POOL_SIZE`, `READ_DB` | the client of MongoDB (3 s to select a server and to connect) and the database of the views | `mongodb://localhost:27017`, 10, `read` |
| `HTTP_ADDR` | where `serve` listens | `127.0.0.1:8080` |

Processor configuration and the lower-level building blocks are described in
[`doc/components.md`](../doc/components.md). The task runtime reads `OUTBOX_WORKER_CONCURRENCY`,
`OUTBOX_QUEUE_CAPACITY`, `OUTBOX_QUEUE_REFILL_PERCENT`, `OUTBOX_POLL_INTERVAL_MS`,
`OUTBOX_HANDLER_TIMEOUT_SECS`, `OUTBOX_LEASE_SECS` and `OUTBOX_STORAGE_TIMEOUT_MS` without the
application prefix. Module-specific variables are read through `Setup` with that prefix.

## Testing

| Tool | For |
|---|---|
| `Cqrs::in_memory()` | Assemble application modules; `run_tasks()` runs due tasks and `catch_up()` waits for views. A test's `.provide` replaces a dependency by type. |

## Features

| Feature | Adds | Default |
|---|---|---|
| `postgres` | the journal, the outbox and the task queue in PostgreSQL (`Builder::event_store`) and the views in PostgreSQL (`Builder::projections`), over the crate `cqrsx-postgres` | no |
| `mongodb` | the views and the checkpoints of their processors in MongoDB, over the crate `cqrsx-mongodb`; includes `postgres` | no |
| `http` | the HTTP routes of the modules and `serve` (axum 0.8, `TraceLayer`): the process logger, the shutdown signals and the coordinated stop of the server, the task runtime and the processors | no |

Without a feature the engine runs on in-memory stores.

## Further reading

- [`../doc/components.md`](../doc/components.md): the inventory of components, the naming contract and the
  integration rules in full.
- [`../demo`](../demo/README.md): a complete application on the engine, with a UI, an X-ray of both databases and
  load tests.
- [`../README.md`](../README.md): building, the local database, the test suite and coverage.
