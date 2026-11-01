# cqrsx-engine

The engine of [cqrsx](../cqrsx/README.md): the application's modules assembled into one running application over
in-memory, PostgreSQL and MongoDB stores, with its settings read from the environment and, with the feature `http`,
served until a signal. The engine holds no logic of its own: the components it assembles, the contracts of an
application and the rules are in the README of the core, the crates and their boundaries in
[`doc/crates.md`](../doc/crates.md).

The application registers its pieces in modules and builds `Cqrs` in memory or with PostgreSQL and MongoDB.
The [typical scenario](../doc/typical_scenario.md) follows a cancellation through a refund.

## Quick start

The crates are not published: an application takes them from git (or by `path`). An application with its journal in
PostgreSQL, its views in MongoDB and an HTTP API, tested in memory:

```toml
[dependencies]
cqrsx = { git = "https://github.com/akozel/cqrsx" }
cqrsx-engine = { git = "https://github.com/akozel/cqrsx", features = ["postgres", "mongodb", "http"] }
serde = { version = "1", features = ["derive"] }
thiserror = "2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
uuid = { version = "1", features = ["serde", "v7"] }

[dev-dependencies]
cqrsx-testing = { git = "https://github.com/akozel/cqrsx" }
```

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
serves the module routes alongside the workers and projection processors. It creates no tables: the schemas are
applied before the first start ([Applying the schemas](#applying-the-schemas)).

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

## Features

| Feature | Adds | Default |
|---|---|---|
| `postgres` | the journal, the outbox and the task queue in PostgreSQL (`Builder::event_store`) and the views in PostgreSQL (`Builder::projections`), over the crate `cqrsx-postgres` | no |
| `mongodb` | the views and the checkpoints of their processors in MongoDB, over the crate `cqrsx-mongodb`; includes `postgres` | no |
| `http` | the HTTP routes of the modules and `serve` (axum 0.8, `TraceLayer`): the process logger, the shutdown signals and the coordinated stop of the server, the task runtime and the processors | no |

Without a feature the engine runs on in-memory stores: the choice for tests, which drive it with `run_tasks()` and
`catch_up()`. A durable application takes `postgres` and `http`: `serve` runs the routes, the task runtime and the
processors in one process. It adds `mongodb` to keep its views in MongoDB; the journal stays in PostgreSQL, since a
checkpoint holds a position of it (views in a database over a journal in memory are refused when the engine is built).

## Configuration

A `Builder` starts from the library's settings (`Cqrs::in_memory()`), from settings of one's own
(`Cqrs::with_settings`) or from variables (`Cqrs::from_env`, `Cqrs::from_lookup`). Every tunable is a checked value
with a default. The engine reads its settings from variables under the
application's prefix (`Cqrs::from_env("SHOP")`: `SHOP_SNAPSHOT_EVERY`, …) and the task runtime's `OUTBOX_*` as they
are, with the application's `Defaults` where a variable is absent. A variable of the prefix is trimmed and counts as
absent when blank; a value that does not parse or is out of its bounds fails the build and names its variable. A
module reads its own variables through its `Setup`, under the same prefix.

The table lists every variable with the library's default, as `Settings::description` writes it; a test of this crate
(`tests/readme.rs`) compares it with the code.

| Variable | Default | Setting |
|---|---|---|
| `SNAPSHOT_EVERY` | `10` | a snapshot of an aggregate every this many events |
| `COMMAND_LOCK_MAX_WAIT_MS` | `1 s` | how long a command waits for its aggregate's lock; below `OUTBOX_HANDLER_TIMEOUT_MS`, since a chain step waits inside its handler |
| `COMMAND_LOCK_MAX_WAITERS` | `128` | commands that may wait for one aggregate's lock; the next one is unavailable |
| `PROJECTION_POLL_MS` | `250 ms` | how long a processor waits for the next page when no commit woke it |
| `PROJECTION_PAGE` | `1000` | events per page of the journal |
| `PROJECTION_LEASE_MS` | `10 s` | the lease of a checkpoint (at least 1 s): another node replaces a killed owner after it |
| `PROJECTION_RETRY_BASE_MS` | `500 ms` | the pause after the first failed round; it doubles with every failure in a row |
| `PROJECTION_RETRY_MAX_MS` | `10 s` | the longest pause after a failed round |
| `PROJECTION_WRITE_CONCURRENCY` | `16` | documents written at once, live batches and rebuild writes together |
| `PROJECTION_KEY_BUFFER` | `64` | jobs one key may hold in the delivery (at least 2) |
| `PROJECTION_MAX_BATCH` | `32` | live jobs of one key one handler call takes at most; 1 turns the batches off |
| `PROJECTION_REBUILD_KEYS` | `100` | keys per page of a rebuild, between two live pages |
| `PROJECTION_WAKE_DELAY_MS` | `0 s` | how long a processor waits after a commit woke it, so that the commits of a burst land in one page |
| `PROJECTION_LAG_CAP` | `100000` | events counted at most for how far a view is behind its journal |
| `PROJECTION_STATUS_TIMEOUT_MS` | `1 s` | how long an operator's status waits for the checkpoints before it answers with what it saw last |
| `NODE_NAME` | `none` | the owner of this node's checkpoints; absent, a fresh id every start ([Several nodes](#several-nodes)) |
| `OUTBOX_WORKER_CONCURRENCY` | `128` | task workers of the process |
| `OUTBOX_BUFFER_CAPACITY` | `128` | claimed tasks waiting for a worker |
| `OUTBOX_BUFFER_REFILL_PERCENT` | `25 %` | the next claim comes once fewer than this share of the buffer waits |
| `OUTBOX_POLL_MS` | `1 s` | the pause between the claims of an idle runtime |
| `OUTBOX_HANDLER_TIMEOUT_MS` | `15 s` | how long a handler runs; then its attempt is retried |
| `OUTBOX_LEASE_MS` | `60 s` | how long a claim lasts; it must cover the wait in the buffer, the handler and the storage calls (checked, see `outbox.lease_budget` in the description) |
| `OUTBOX_STORAGE_TIMEOUT_MS` | `6 s` | the budget of one call to the task queue's storage: a claim, a settlement, a recovery |
| `DATABASE_URL` | `postgresql://cqrsx:cqrsx@localhost:5433/cqrsx` | the PostgreSQL database (feature `postgres`) |
| `DATABASE_MAIN_POOL_SIZE` | `10` | connections for the commands, the task queue, the checkpoints and the views in PostgreSQL |
| `DATABASE_STATEMENT_TIMEOUT_MS` | `4 s` | the statement timeout of the main and the projection pools; 0: no cap |
| `DATABASE_HISTORY_POOL_SIZE` | `1` | connections for whole-history reads: verify, the size of the journal, the history of one aggregate |
| `DATABASE_PROJECTION_POOL_SIZE` | `4` | connections for the processors' reads of the journal |
| `MONGODB_URI` | `mongodb://localhost:27017` | the MongoDB server (feature `mongodb`); the client waits 3 s to select a server and to connect |
| `MONGODB_POOL_SIZE` | `10` | connections of the MongoDB client |
| `MONGODB_DATABASE` | `read` | the database of the views and their checkpoints |
| `HTTP_ADDR` | `127.0.0.1:8080` | where `serve` listens (feature `http`) |

`Cqrs::description()` (and `Settings::description()`) lists the values a running engine uses under the same names,
with the values derived from them under dotted names in lower case (`outbox.lease_budget`, the plan of each pool:
`postgres.main.max_connections`, …) and the keys its modules describe. It holds `DATABASE_URL` as given, password
included.

## Testing

`Cqrs::in_memory()` assembles the application's modules over the stores in memory; the tools for an aggregate, a
use case and a handler are in the [testing table of the core](../cqrsx/README.md#testing).

## Operations

### Logs

`serve` installs the process logger: `RUST_LOG` is one level (`error`, `warn`, `info`, `debug`, `trace` or `off`,
default `info`), not a list of directives. An application that does not call `serve` installs a `tracing` subscriber of
its own. A log line names the failure by category and code (`storage_category`, `storage_code`), never by the message of a
driver or the data of a command.

### Applying the schemas

The engine creates no tables. Before the first start apply the schemas of
[`cqrsx-postgres/sql/`](../cqrsx-postgres/sql/) in the order of their numbers (the documentation of `cqrsx-postgres`
says how); each view kept in PostgreSQL also needs a table of its own, whose shape `PostgresViewRepository::new`
gives. MongoDB needs nothing: a collection appears with its first document.

### Retrying a failed task

A task that failed (its handler answered `fail`, or its last attempt `retry`) keeps its row in `transactional_outbox`
with `status = 'failed'` and its `last_error`. Once the cause is fixed, the operator script
[`requeue-failed-outbox.sql`](../cqrsx-postgres/sql/requeue-failed-outbox.sql) (the constant
`cqrsx_postgres::REQUEUE_FAILED_OUTBOX_TASK`) returns it to `pending` with a fresh attempt budget; it keeps the id and
the payload, so an idempotent receiver recognises the repeat. The failed tasks:

```sql
SELECT id, task_type, attempts, last_error FROM transactional_outbox WHERE status = 'failed';
```

The script takes the task's id as its one parameter: from Rust
`sqlx::query(REQUEUE_FAILED_OUTBOX_TASK).bind(task_id)`, in `psql` 16 or later the statement followed by
`\bind <id> \g`.

### Rebuilding and verifying views

`cqrs.views()` operates the views by name; the application exposes what it needs (the demo routes them over HTTP).

- `status()`: the checkpoint rows, with position, owner and lease, `failures` and `last_error` of a failing key, and
  the progress of a rebuild. A failing key stops its checkpoint before it and is retried after a growing pause; once
  its cause is fixed the processor goes on by itself.
- `lag(name)`: the events above the checkpoint, counted up to `PROJECTION_LAG_CAP`.
- `request_rebuild(name, pace)`: every document from its history, `pace` between the events of a key;
  `request_rebuild_key(name, id)`: one document. The processor that holds the checkpoint takes the request on any
  node; live delivery goes on meanwhile.
- `verify(name)`: every document against the fold of the part of its history it applied; documents behind are
  counted apart, a difference is a wrong document.

### Resetting the databases

A checkpoint holds a position of the PostgreSQL journal, so the journal (events, snapshots, the outbox) and the
checkpoints are reset together: a journal recreated under old checkpoints starts below their positions, and its events
never reach the views. The read side alone may be dropped, documents first and checkpoints second (in PostgreSQL the
view tables, then `projection_checkpoints` and `projection_rebuild_keys`; in MongoDB the view collections, then
`projection_checkpoints`): the processors register their rows again and rebuild every document from the start of the
journal. A dropped document restarts its revision, so a write of an owner that lost its lease before the drop can land
on the recreated document: reset with one node running, or with the writes stopped.

### Several nodes

Several processes may run the same application over one PostgreSQL. Their commands meet only at the store's
optimistic check (the conflict retry); each process keeps one executor per aggregate type. The task queue is shared:
a claim leases its tasks, and any node recovers an expired lease. Each projection's checkpoint has one owner at a time
under its lease; the other nodes take it over when a lease runs out. Give every node a stable, distinct `NODE_NAME`:
it names the owner in `status()`, and without it a restarted node is a new owner. A commit wakes the processors of its
own process only; the others find it at their next poll (`PROJECTION_POLL_MS`).

### Errors of building and serving

`build` fails with a `BuildError`: a setting (`Settings`, the variable named), a module's refusal (`Setup`), an
aggregate type, a view or an observer registered twice, one key of the description with two values, a policy or the
task handlers refused, views in a database over a journal in memory, a URL or URI that does not parse, a database not
reachable (each pool named). `serve` adds `ServeError`: the logger, the address taken, or the read side stopped (a
processor panicked); a `BuildError` of a setting, a module's variable, a URL or URI or the store choice is written as
`invalid configuration: …`.

## Further reading

- [`../doc/components.md`](../doc/components.md): the inventory of components, the naming contract and the
  integration rules in full.
- [`../doc/guarantees.md`](../doc/guarantees.md): every guarantee of the engine, with the code that holds it and its
  tests.
- [`../demo`](../demo/README.md): a complete application on the engine, with a UI, an X-ray of both databases and
  load tests.
- [`../README.md`](../README.md): building, the local database, the test suite and coverage.
