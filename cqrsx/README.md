# cqrsx

CQRS and Event Sourcing for Rust services: aggregates and command execution, event and snapshot
persistence, read-model projections, a transactional outbox and a durable task queue. PostgreSQL stores
the data, Tokio runs the background work. The application owns the business rules; the library owns
consistency, ordering and retries.

## At a glance

- **One command, one lock, one transaction.** Inside a process the commands to one aggregate run one at
  a time, in arrival order. The event, an optional snapshot and the outbox tasks planned from the decision
  commit in a single statement; a conflict with another process is retried with backoff, and the retry
  plans its tasks again from the fresh state.
- **Projections from the journal, with a checkpoint.** A projection processor reads the journal of its
  aggregate type page by page from a checkpoint row it holds under a lease, applies the events through a
  worker per key and moves the checkpoint behind what is applied; a commit wakes it. Nothing is lost on the
  way to the read model, two nodes never apply at once, and a rebuild (the whole journal, one key, or a
  document that refused a gap) runs through the same workers without stopping the writes. The views and the
  checkpoint may live in MongoDB, next to each other (feature `mongodb`).
- **Durable follow-ups, planned from the decision.** The aggregate decides events only; its reactions say what
  follows each event, one `match` the compiler checks for completeness, and the tasks commit with the event.
  Anything that must survive a crash is an outbox task: claimed under a lease, retried after the pause and within
  the attempt budget of its type's policy, run by a worker pool that drains on shutdown; its handler is a function
  of the task, its context and its dependencies (`#[task_handler]`).
- **Testable without a database.** A given/when/then framework for aggregates, an in-memory store and
  journal with an outbox and its task queue, a command executor that records the tasks a use case publishes, and
  the engine in memory: the modules of an application assembled as the process assembles them, whose tasks run to
  the end of their chains and whose views catch up with the journal on demand.

```text
command ─► CommandExecutor ─► CqrsFramework ─► Aggregate ─► Reactions ─► EventStore ──┐ one transaction:
               │  aggregate lock                 (event)     (event → tasks)         ├─ event, snapshot,
               └─► CommitWakeup                                                       └─ outbox tasks

events (tx) ─► ProjectionProcessor (checkpoint, lease) ─► worker per key ─► ViewProjection ─► read model
                       ▲ CommitWakeup after a commit, or the poll

transactional_outbox ─► TaskRuntime ─► Worker ─► TaskDispatcher ─► TaskHandler ─► e-mail, next command, …
```

## Quick start

A fan's ticket ledger: an aggregate with one command and one event, a read model and a follow-up task
its reactions plan from the event. Everything below runs in memory, and the same types run unchanged on PostgreSQL.

```rust
use cqrsx::cqrs::{
    aggregate::Aggregate,
    command::{
        CommandExecutor,
        causation::Causation,
        decide::Decide,
        wakeup::CommitWakeup,
    },
    event::EventEnvelope,
    projection::{
        View, ViewProjection,
        processor::{ProjectionProcessor, config::ProcessorConfig},
        repository::ViewRepository,
    },
    reactions::{Decided, Reactions, ReactionsPlanner, Then},
    testing::{
        TestFramework, command::TestCommandExecutor, mem_checkpoint_store::MemCheckpointStore,
        mem_event_repository::MemEventRepository, mem_view_repository::MemViewRepository,
    },
};
use cqrsx::{Event, Events, Task, task::policy::Policies};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

// The aggregate. Business rules live in the command's `decide`, state changes in `apply`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Ledger {
    tickets: u32,
}

#[derive(Clone, Debug)]
enum LedgerCommand {
    RecordTicket { seat: String },
}

// An event is a fact with the name and version it is stored with; the aggregate's enum of events takes them
// from each fact.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Event)]
#[event(name = "ticket_recorded", version = "1")]
struct TicketRecorded {
    seat: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Events)]
enum LedgerEvent {
    TicketRecorded(TicketRecorded),
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("ticket limit of {limit} reached")]
struct TicketLimit {
    limit: u32,
}

/// What the rules need from the application; here, the ticket limit.
#[derive(Clone)]
struct Rules {
    limit: u32,
}

impl Aggregate for Ledger {
    const TYPE: &'static str = "ledger";
    type Id = String; // a validated newtype works too: its `Display` is the key in the journal
    type Command = LedgerCommand;
    type Event = LedgerEvent;
    type Error = TicketLimit;
    type Rules = Rules;

    fn apply(&mut self, event: LedgerEvent) {
        let LedgerEvent::TicketRecorded(_) = event;
        self.tickets += 1;
    }
}

// A command decides on the committed state: one event or a rejection.
impl Decide<Ledger> for LedgerCommand {
    fn decide(self, ledger: &Ledger, rules: &Rules) -> Result<LedgerEvent, TicketLimit> {
        let LedgerCommand::RecordTicket { seat } = self;
        if ledger.tickets >= rules.limit {
            return Err(TicketLimit { limit: rules.limit });
        }
        Ok(LedgerEvent::TicketRecorded(TicketRecorded { seat }))
    }
}

// The read model: a wallet, kept up to date by a projection processor that reads the journal.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Wallet {
    seats: Vec<String>,
}

impl View<Ledger> for Wallet {
    const NAME: &'static str = "wallets"; // its collection or table and its projection's checkpoint row

    fn update(&mut self, event: &EventEnvelope<Ledger>) {
        let LedgerEvent::TicketRecorded(recorded) = &event.payload;
        self.seats.push(recorded.seat.clone());
    }
}

// A follow-up that must survive a crash: an outbox task, committed together with the event. Its type and
// version are stored with it and route it to its handler.
#[derive(Serialize, Deserialize, Task)]
#[task(name = "send_e_ticket", version = 1)]
struct SendETicket {
    seat: String,
}

// What follows each fact of the ledger: every recorded ticket is mailed. The core runs the reactions after
// every decision, under the aggregate's lock, and commits their tasks with the event; a new event without a
// branch does not compile.
impl Reactions for Ledger {
    fn react(decided: &Decided<'_, Self>, then: &mut Then) {
        match decided.event {
            LedgerEvent::TicketRecorded(recorded) => then.task(SendETicket { seat: recorded.seat.clone() }),
        }
    }
}

fn main() {
    // The aggregate on its own: given, when, then (synchronous, no runtime needed).
    TestFramework::<Ledger>::with(Rules { limit: 1 })
        .given(vec![LedgerEvent::TicketRecorded(TicketRecorded { seat: "A1".into() })])
        .when(LedgerCommand::RecordTicket { seat: "A2".into() })
        .then_expect_error_message("ticket limit of 1 reached");

    // The command path over an in-memory journal: aggregate locks, conflict retry, the reactions and a
    // record of the tasks every commit published. A projection processor reads the same journal into the
    // wallets; the executor wakes it after every commit.
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let (journal, wakeup) = (MemEventRepository::default(), CommitWakeup::new());
        let wallets = Arc::new(MemViewRepository::<Wallet, Ledger>::default());
        let processor = ProjectionProcessor::new(
            Wallet::NAME,
            Arc::new(journal.clone()),
            Arc::new(MemCheckpointStore::default()),
            Arc::new(ViewProjection::new(wallets.clone())),
            ProcessorConfig::default(),
        )
        .with_wakeup(wakeup.clone());
        let shutdown = CancellationToken::new();
        let running = tokio::spawn(processor.run(shutdown.clone()));
        let ledgers = TestCommandExecutor::<Ledger, _>::over_journal(journal.clone(), Rules { limit: 4 })
            // No policies: the tasks get the engine's attempt budget.
            .with_task_planner(ReactionsPlanner::<Ledger>::new(Policies::default()))
            .with_commit_wakeup(wakeup);

        // Who caused the command travels in its metadata; the core stamps it on every task it plans.
        let causation = Causation::root("ui");
        let command = LedgerCommand::RecordTicket { seat: "B4".into() };
        let report = ledgers
            .execute_with_metadata(&"fox".into(), command, causation.event_metadata())
            .await
            .unwrap();
        // The task was planned from the event and published with it: on PostgreSQL, in the same statement…
        assert_eq!(report.tasks[0].payload()["seat"], "B4");
        assert_eq!(report.tasks[0].metadata()["correlation_id"], causation.correlation_id.to_string());
        assert_eq!(journal.outbox::<Ledger>().len(), 1);

        // …and the processor brings the wallet up to date in the background, a moment later.
        let mut wallet = None;
        for _ in 0..500 {
            wallet = wallets.load("fox").await.unwrap();
            if wallet.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(wallet.map(|wallet| wallet.seats), Some(vec!["B4".to_owned()]));
        shutdown.cancel();
        running.await.unwrap();
    });
}
```

## Running on PostgreSQL

Apply the three schemas once, `postgres::EVENT_STORE_SCHEMA`, `postgres::TRANSACTIONAL_OUTBOX_SCHEMA` and
`postgres::PROJECTION_CHECKPOINTS_SCHEMA` (the compose file in the repository root does it for a local
database; a read-model table in PostgreSQL is the application's, with the columns documented on
`PostgresViewRepository::new`). Then let the engine assemble the process from the modules of the application: an
executor per aggregate type with its reactions, a projection processor per view, the task runtime with every handler,
the HTTP routes of the modules (feature `http`). This is a sketch; `../demo/back-end/src/main.rs` and the module roots
next to it (`concert.rs`, `fan.rs`, `operations.rs`) are the complete, tested version.

```rust,ignore
use cqrsx::{
    engine::{
        Cqrs,
        module::Module,
        mongodb::MongoDb,
        postgres::Postgres,
        settings::Defaults,
        setup::{Setup, SetupError},
    },
    task::{
        context::TaskContext,
        policy::{PolicyName, Retry},
        result::TaskResult,
    },
    task_handler,
};
use std::sync::Arc;

/// The handlers' dependency, by its type: the mail service, a trait object the module provides once.
type Mailer = dyn MailService;

/// The task handler, a function: the task, its context, its dependencies. A task arrives at least once: the same one
/// may come back after a crash or a timeout, so the effect is keyed by the task id and repeating it is harmless. A
/// failed send is retried after the pause of the task type's policy; the handler names none.
#[task_handler]
async fn send_e_ticket(task: SendETicket, ctx: &TaskContext, mail: &Mailer) -> TaskResult {
    match mail.send(ctx.task_id(), &task.seat).await {
        Ok(()) => TaskResult::done(),
        Err(error) => TaskResult::retry(error.to_string()),
    }
}

const MAIL: PolicyName = PolicyName::new("mail");

/// The root of the module: what it plugs into the engine, its own variables under the application's prefix
/// (`SHOP_TICKET_LIMIT`, `SHOP_MAIL_ATTEMPTS`) and its HTTP routes.
fn wallets(setup: &Setup) -> Result<Module, SetupError> {
    let mail: Arc<Mailer> = Arc::new(HttpMailer::new(setup.engine().handler_timeout()).map_err(SetupError::failed)?);
    Ok(Module::new("wallets")
        .aggregate::<Ledger>(Rules { limit: setup.env("TICKET_LIMIT", 4)? })
        .view::<Wallet, Ledger>() // the collection `wallets`
        .provide(mail)
        .policy(MAIL, Retry::backoff(setup.millis("MAIL_BASE_MS", 1000)?, setup.millis("MAIL_MAX_MS", 8000)?), 6)
        .task(send_e_ticket)
        .policy(MAIL)
        .routes(http::routes())) // axum::Router<Arc<Cqrs>>: the handlers take the engine as their state
}

/// The application's values where a variable is absent, and the name of its connections.
const SHOP_DEFAULTS: Defaults = Defaults::new("shop", &[("DATABASE_URL", "postgresql://shop@localhost/shop")]);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // SHOP_DATABASE_URL, SHOP_SNAPSHOT_EVERY, SHOP_PROJECTION_*, SHOP_HTTP_ADDR, …, the library's OUTBOX_* as they are.
    Cqrs::from_env("SHOP")
        .defaults(SHOP_DEFAULTS)
        .event_store(Postgres) // the journal, the snapshots, the outbox and the task queue: three pools
        .projections(MongoDb)  // the views and their checkpoints; or .projections(Postgres)
        .module(wallets)
        .serve() // RUST_LOG, the pools, the HTTP API, the task runtime and the processors until SIGINT or SIGTERM
        .await?;
    Ok(())
}
```

`serve` stops in order: the server takes no more requests, the task runtime drains, the processors finish what is in
flight and give their checkpoints back, then the pools close. The engine opens its pools when it is built, one
connection each (an unreachable database fails the start); the client of MongoDB connects when it is first used. The
pieces the engine assembles stay public for an assembly of one's own: `PostgresCommandExecutor`,
`ProjectionProcessor`, `prepare_task_runtime`, `host::lifecycle::run_with_processors`.

## Projections from the journal

A `ProjectionProcessor` reads the journal of its aggregate type page by page from a checkpoint row (`projection_checkpoints`) that it holds under a lease, hands
every key's events to a worker of that key and moves the checkpoint behind what is applied; a commit wakes the
processors of its type, and on another node the poll finds it. Nothing is lost between the commit and the read
model, two nodes never apply at once, and a rebuild runs through the same workers: the whole journal key by key
(`request_rebuild`, with a pace between the events of a key for people to watch), one key
(`request_rebuild_key`), or a document that refused a live event as a gap, healed on the spot.

The engine wires one per view: a `CommitWakeup` per aggregate type, shared by the executor
(`with_commit_wakeup`) and the type's processors (`with_wakeup`), and a `ProjectionRuntime` that runs the
processors of the process until the shutdown token.

`CheckpointStore::status` shows where a projection stands: the position, the owner and its lease, the failed
rounds in a row with the last error, and the state of a rebuild. How far it is behind is
`PostgresEventRepository::count_after(position, cap)`: the events of its type above the checkpoint, counted up to a
cap. The processor's observer reports every applied
batch, every move of the checkpoint, every failure and rebuild, for an event feed or a status page.

### Read models in MongoDB

With the `mongodb` feature the checkpoint moves next to the read models: the journal stays in PostgreSQL, the
views and the checkpoint live in one MongoDB database (`.projections(MongoDb)`: `SHOP_MONGODB_URI`, `SHOP_READ_DB`,
`SHOP_MONGODB_POOL_SIZE`). Built by hand, the processor of a view takes the repositories of MongoDB:

```rust,ignore
use cqrsx::mongodb::{projection_checkpoints::MongoCheckpointStore, view_repository::MongoViewRepository};

let read_db = mongodb::Client::with_options(options)?.database("read");
let wallets = Arc::new(MongoViewRepository::<Wallet, Ledger>::new(&read_db)); // the collection `wallets`
let processor = ProjectionProcessor::new(
    Wallet::NAME,
    Arc::new(PostgresEventRepository::new(pool.clone())), // the journal
    Arc::new(MongoCheckpointStore::new(&read_db)),       // collection `projection_checkpoints`
    Arc::new(ViewProjection::new(wallets)),
    ProcessorConfig::default(),
)
.with_wakeup(wakeup);
```

A view is one document `{ _id: view id, version, last_sequence, view }`. Every view repository answers three queries:
one document by id (`load_with_context`), every document (`list`) and a range of ids (`range`), in the order of the
ids' bytes, alike in memory, in PostgreSQL and in MongoDB. Each checkpoint is one document, and every operation on
it is one atomic statement by the server's clock (MongoDB 4.2 or later).

## Configuration

Every tunable is a checked value with a default. The engine reads its settings from variables under the
application's prefix (`Cqrs::from_env("SHOP")`: `SHOP_SNAPSHOT_EVERY`, …) and the task runtime's `OUTBOX_*` as they
are, with the application's `Defaults` where a variable is absent; `settings().describe()` lists what it uses. A
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

| What | How | Default |
|---|---|---|
| How long, and how many, commands wait for one aggregate's lock | `LockLimits::new(max_wait, max_waiters)` | 1 s, 128 |
| Conflict retry: attempts, base and maximum backoff, jitter | `AggregateConflictRetryPolicy::new(..)` | 3, 5 ms, 50 ms, 0.2 |
| Snapshot interval, conflict retry and lock limits of a PostgreSQL executor | `PostgresCommandConfig::new(..)` | your interval, the defaults above |
| Projection processor: poll interval, page size, lease, key buffer; retry pauses, keys per rebuild page, documents written at once, pause after a wake-up | `ProcessorConfig::new(poll, page, lease, delivery_limits)?`, `.with_retry(base, max)?`, `.with_rebuild_page(n)?`, `.with_concurrency(n)?`, `.with_settle(d)?` | 250 ms, 1000, 10 s, the limits below; 500 ms / 10 s, 100, 16, 0 |
| A processor's buffer per aggregate key (jobs, at least 2; the pages in flight are one fewer) and live jobs per handler call | `DeliveryLimits::new(capacity, max_batch)` | 64, 32 |
| Server-side cap on every statement of a pool | `StatementTimeout::{Default, Custom, Unlimited}` | 4 s |
| Connection check before hand-out | `ConnectionPingPolicy::{Default, Idle(d)}` | SQLx's; `Idle(100 ms)` recommended |
| Task runtime | `TaskRuntimeConfig::from_env()` | see below |
| Logging | `LoggingConfig::from_env()`: `RUST_LOG`, one level (`serve` installs it once) | `info` |

The task runtime reads `OUTBOX_WORKER_CONCURRENCY` (128), `OUTBOX_QUEUE_CAPACITY` (128),
`OUTBOX_QUEUE_REFILL_PERCENT` (25), `OUTBOX_POLL_INTERVAL_MS` (1000), `OUTBOX_HANDLER_TIMEOUT_SECS` (15),
`OUTBOX_LEASE_SECS` (60) and `OUTBOX_STORAGE_TIMEOUT_MS` (6000), and refuses a lease shorter than the worst
case of one claim: `2·storage + handler + 5 s + ceil(capacity / concurrency)·(handler + storage)`. Claimed
tasks wait in a local buffer of `OUTBOX_QUEUE_CAPACITY`; the runtime claims again once fewer than
`OUTBOX_QUEUE_REFILL_PERCENT` of it wait, and then fills it. The defaults keep the capacity equal to the
concurrency: a larger buffer makes claims bigger but needs a lease for one more queued wave.

## Rules worth knowing

- **The aggregate decides events, its reactions plan tasks.** `Reactions::react` is a pure, synchronous
  function of what was decided (the id, the state after the event, the command, the event, the metadata): it
  runs under the aggregate's lock on every attempt, so it does no I/O, and data from outside comes in the
  command. A task that cannot be built refuses the command with nothing written. The executor runs them through
  `ReactionsPlanner`, a `TaskPlanner`; the attempt budget of a task is the policy of its type (`Policies`).
- **The command id is a publication key.** With the causation keys in the metadata
  (`Causation::event_metadata`) the core stamps them on every planned task and derives the task's id from the
  command id, the aggregate and the task's format: every attempt plans the same ids, and the same command
  run twice on one aggregate cannot publish twice (the second commit is refused whole). Without them the
  tasks keep random ids.
- **An event handler never executes a command**, not even from a task it spawns: a projection writes its
  read model and nothing else, and a follow-up started there would be recorded nowhere and repeated by every
  rebuild. Plan the follow-up as a task from the command's decision instead and let a task handler run it.
- **A failed projection does not undo the commit.** The command has answered already; the worker logs the
  cause once per batch and the processor retries (next rule). Rebuild a read model from history with
  `request_rebuild` or, for one key, `request_rebuild_key`.
- **A projection processor never skips an event.** A failed key stops the checkpoint before it, visibly
  (`failures`, `last_error`, the observer), and the round is retried after a growing pause; only a document
  that refused an event as a gap is rebuilt from its history on the spot.
- **A checkpoint belongs to its journal.** It holds a position of the PostgreSQL journal: recreate the journal
  and the checkpoints together, or the new events stay below the old checkpoint. The read side alone may be
  dropped, documents first and checkpoints second: the processors register their rows again and rebuild every
  document from the start of the journal.
- **A view document takes only the next event of its aggregate.** It remembers `last_sequence`; a repeated
  event is skipped, a batch that jumps ahead fails with `SequenceGap` and writes nothing, so a document that
  fell behind stays a correct prefix of its history until the processor rebuilds it from that history.
- **A projection gets a key's events in journal order**, possibly several commands per call;
  `max_batch = 1` turns the batches off.
- **Task handlers are at-least-once.** Make the effect repeat-safe (the task id is a ready idempotency
  key), answer `TaskResult::retry` when it is worth another attempt and `TaskResult::fail` when it is not. The
  pause before the next attempt is the policy of the task's type, applied by the runtime; the handler only tells it
  (`ctx.retry_pause()`). A command to another aggregate is `ctx.execute::<A>(..)`: it continues the task's
  causation, and its conflict or unavailability retries the task.
- **One executor per aggregate type and process.** Two executors of one type order nothing between them;
  the store's optimistic check still decides, like between two processes.
- **Do not cancel a command's future while its commit is in flight.** The events may land while the caller
  learns nothing, and the processors find them at their next poll instead of at once. An HTTP server that
  drops handler futures on client disconnect should run the command in a task of its own.

## Testing

| Tool | For |
|---|---|
| `engine::Cqrs::in_memory()` | An application end to end: `.module(root)…build().await` assembles the modules over stores in memory (a test's `.provide` replaces a module's dependency; `Cqrs::from_lookup` gives the modules and the engine variables of the test); `cqrs.from("ui").execute::<A>(..)`, then `run_tasks()` (the due tasks to the end of their chains; a retry waits for the Tokio clock), `catch_up()` and `view::<V>().get(..)`; `checkpoints()` and `documents::<V, A>()` reach the stores in memory, `router()` the HTTP routes (feature `http`) |
| `task::testing::mem_task_queue::MemTaskQueue` | The task queue over the outbox of a `MemEventRepository`, by the rules of the PostgreSQL queue and the Tokio clock; `tasks()` shows every delivery, `lose_settlements(true)` a process that stops before its acknowledgements |
| `cqrs::testing::TestFramework` | An aggregate's rules: `with(rules).given(events).when(command).then_expect_event(..)` or `then_expect_error_message(..)` |
| `cqrs::testing::command::TestCommandExecutor` | A use case: the production command path, with locks, retry, the task planner and a commit wake-up, over `MemStore` or (`over_journal`) over a `MemEventRepository` that projection processors read; `take_published_tasks()` shows what the committed calls published |
| `cqrs::testing::mem_event_repository::MemEventRepository::outbox` | The tasks published with the events of an aggregate type, each with the sequence of its event, by the rules of the PostgreSQL outbox |
| `cqrs::reactions::Decided` | An aggregate's reactions on their own: build what was decided by hand, react into a `Then` and look at its tasks |
| `cqrs::testing::mem_view_repository::MemViewRepository` | A projection and the queries over it |
| `cqrs::testing::mem_checkpoint_store::MemCheckpointStore` | A projection processor without a database, next to `MemEventRepository` and `MemViewRepository` (leases follow the Tokio clock, so a paused test clock drives them) |
| `cqrs::testing::unavailable_store::UnavailableStore` | The storage-down path |
| `task::context::TaskContext::new(..)` | A task handler, called as the function it is (`handler.call(task, &ctx, &dependency)`), with a first claim (`TaskAttempt::first(..)`) and the pause of its retry; `with_executors` over a `TestCommandExecutor` for a handler that runs a command |

## Features

| Feature | Adds | Default |
|---|---|---|
| `postgres` | the PostgreSQL event store, outbox publication, task queue, view repository and pools | yes |
| `worker` | the task runtime: worker pool, polling, supervision, graceful drain | yes |
| `host` | process signals and the coordinated shutdown of inbound server and task runtime | no |
| `logging` | `LoggingConfig`: an explicit `tracing` subscriber from `RUST_LOG` | no |
| `mongodb` | the MongoDB read side: a view repository and the checkpoint store of projection processors | no |
| `http` | the HTTP routes of the modules and `serve` of an engine (axum 0.8, `TraceLayer`); implies `host` | no |

Without PostgreSQL the core still compiles and tests: aggregates, the framework, the in-memory stores,
projections, the projection processor with its checkpoints and the task contracts.

## Further reading

- [`doc/components.md`](doc/components.md): the inventory of components, the naming contract and the
  integration rules in full.
- [`../demo`](../demo/README.md): a complete application on the library, with a UI, an X-ray of both
  databases and load tests.
- [`../README.md`](../README.md): building, the local database, the test suite and coverage.
- [`sql/`](sql/): the canonical schemas and the operator script that requeues a failed task.

## Attribution

The CQRS framework and the PostgreSQL event and view repositories include modified code from
[`cqrs-es` and `postgres-es` 0.5.0](https://github.com/serverlesstechnology/cqrs), Apache-2.0,
copyright 2023 Serverless Technology Consulting, LLC; see [`licenses/`](licenses/).
