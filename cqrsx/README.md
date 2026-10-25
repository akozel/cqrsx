# cqrsx

CQRS and Event Sourcing for Rust services: aggregates and command execution, event and snapshot
persistence, read-model projections, a transactional outbox and a durable task queue. PostgreSQL stores
the data, Tokio runs the background work. The application owns the business rules; the library owns
consistency, ordering and retries.

## At a glance

- **One command, one lock, one transaction.** Inside a process the commands to one aggregate run one at
  a time, in arrival order. The event, an optional snapshot and the command's outbox tasks commit in a
  single statement; a conflict with another process is retried with backoff.
- **Projections from the journal, with a checkpoint.** A projection processor reads the journal of its
  aggregate type page by page from a checkpoint row it holds under a lease, applies the events through a
  worker per key and moves the checkpoint behind what is applied; a commit wakes it. Nothing is lost on the
  way to the read model, two nodes never apply at once, and a rebuild (the whole journal, one key, or a
  document that refused a gap) runs through the same workers without stopping the writes. The views and the
  checkpoint may live in MongoDB, next to each other (feature `mongodb`).
- **Durable follow-ups.** Anything that must survive a crash is an outbox task: claimed under a lease,
  retried with backoff, run by a worker pool that drains on shutdown.
- **Testable without a database.** A given/when/then framework for aggregates, an in-memory store and
  journal, and a command executor that records the tasks a use case publishes and feeds the projection
  processors under test.

```text
command ─► CommandExecutor ─► CqrsFramework ─► Aggregate ─► EventStore ──┐ one transaction:
               │  aggregate lock                                         ├─ event, snapshot,
               └─► CommitWakeup                                          └─ outbox tasks

events (tx) ─► ProjectionProcessor (checkpoint, lease) ─► worker per key ─► ViewProjection ─► read model
                       ▲ CommitWakeup after a commit, or the poll

transactional_outbox ─► TaskRuntime ─► Worker ─► TaskDispatcher ─► TaskHandler ─► e-mail, next command, …
```

## Quick start

A fan's ticket ledger: an aggregate with one command and one event, a read model and a follow-up task.
Everything below runs in memory, and the same types run unchanged on PostgreSQL.

```rust
use cqrsx::cqrs::{
    aggregate::Aggregate,
    command::{CommandExecutor, wakeup::CommitWakeup},
    event::{DomainEvent, EventEnvelope},
    event_sink::EventSink,
    projection::{
        View, ViewProjection,
        processor::{ProjectionProcessor, config::ProcessorConfig},
        repository::ViewRepository,
    },
    testing::{
        TestFramework, command::TestCommandExecutor, mem_checkpoint_store::MemCheckpointStore,
        mem_event_repository::MemEventRepository, mem_view_repository::MemViewRepository,
    },
};
use cqrsx::task::{NewTask, format::TaskPayload};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

// The aggregate. Business rules live in `handle`, state changes in `apply`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Ledger {
    tickets: u32,
}

#[derive(Clone, Debug)]
enum LedgerCommand {
    RecordTicket { seat: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum LedgerEvent {
    TicketRecorded { seat: String },
}

impl DomainEvent for LedgerEvent {
    fn event_type(&self) -> String {
        "ticket_recorded".into()
    }

    fn event_version(&self) -> String {
        "1".into()
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("ticket limit of {limit} reached")]
struct TicketLimit {
    limit: u32,
}

/// What command handling needs from the application; here, the ticket limit.
#[derive(Clone)]
struct Rules {
    limit: u32,
}

impl Aggregate for Ledger {
    const TYPE: &'static str = "ledger";
    type Command = LedgerCommand;
    type Event = LedgerEvent;
    type Error = TicketLimit;
    type Services = Rules;

    async fn handle(&mut self, command: LedgerCommand, rules: &Rules, sink: &EventSink<Self>) -> Result<(), TicketLimit> {
        let LedgerCommand::RecordTicket { seat } = command;
        if self.tickets >= rules.limit {
            return Err(TicketLimit { limit: rules.limit });
        }
        sink.write(LedgerEvent::TicketRecorded { seat }, self).await;
        Ok(())
    }

    fn apply(&mut self, event: LedgerEvent) {
        let LedgerEvent::TicketRecorded { .. } = event;
        self.tickets += 1;
    }
}

// The read model: a wallet, kept up to date by a projection processor that reads the journal.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Wallet {
    seats: Vec<String>,
}

impl View<Ledger> for Wallet {
    fn update(&mut self, event: &EventEnvelope<Ledger>) {
        let LedgerEvent::TicketRecorded { seat } = &event.payload;
        self.seats.push(seat.clone());
    }
}

// A follow-up that must survive a crash: an outbox task, committed together with the event.
#[derive(Serialize, Deserialize)]
struct SendETicket {
    seat: String,
}

impl TaskPayload for SendETicket {
    const TASK_TYPE: &'static str = "send_e_ticket";
    const TASK_VERSION: i32 = 1;
}

fn main() {
    // The aggregate on its own: given, when, then (synchronous, no runtime needed).
    TestFramework::<Ledger>::with(Rules { limit: 1 })
        .given(vec![LedgerEvent::TicketRecorded { seat: "A1".into() }])
        .when(LedgerCommand::RecordTicket { seat: "A2".into() })
        .then_expect_error_message("ticket limit of 1 reached");

    // The command path over an in-memory journal: aggregate locks, conflict retry and a record of the
    // tasks every call carries. A projection processor reads the same journal into the wallets; the
    // executor wakes it after every commit.
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let (journal, wakeup) = (MemEventRepository::default(), CommitWakeup::new());
        let wallets = Arc::new(MemViewRepository::<Wallet, Ledger>::default());
        let processor = ProjectionProcessor::new(
            "wallets",
            Arc::new(journal.clone()),
            Arc::new(MemCheckpointStore::default()),
            Arc::new(ViewProjection::new(wallets.clone())),
            ProcessorConfig::default(),
        )
        .with_wakeup(wakeup.clone());
        let shutdown = CancellationToken::new();
        let running = tokio::spawn(processor.run(shutdown.clone()));
        let ledgers =
            TestCommandExecutor::<Ledger, _>::over_journal(journal, Rules { limit: 4 }).with_commit_wakeup(wakeup);

        let task = NewTask::new(SendETicket { seat: "B4".into() }).unwrap();
        let command = LedgerCommand::RecordTicket { seat: "B4".into() };
        ledgers.execute("fox", command, vec![task]).await.unwrap();
        // The task was published with the event: on PostgreSQL, in the same transaction…
        assert_eq!(ledgers.take_published_tasks().len(), 1);

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
database; a read-model table is the application's, with the columns documented on
`PostgresViewRepository::new`). Then assemble the process with the types of the quick
start. This is a sketch; the demo back-end in `../demo/back-end/src/process.rs` and
`../demo/back-end/src/bootstrap/` is the complete, tested version.

```rust,ignore
use cqrsx::{
    cqrs::{
        command::{locks::LockLimits, retry::AggregateConflictRetryPolicy, wakeup::CommitWakeup},
        projection::{
            ViewProjection,
            processor::{ProjectionProcessor, config::ProcessorConfig},
            runtime::ProjectionRuntime,
        },
    },
    host::{lifecycle, signal::shutdown_signal},
    logging::LoggingConfig,
    postgres::{
        command::{PostgresCommandConfig, PostgresCommandExecutor},
        event_repository::PostgresEventRepository,
        pool::{ConnectionPingPolicy, DEFAULT_IDLE_PING_AFTER, StatementTimeout, connect_pool},
        projection_checkpoints::PostgresCheckpointStore,
        task_runtime::prepare_task_runtime,
        view_repository::PostgresViewRepository,
    },
    task::{
        adapter::boxed::BoxedTaskAdapter, attempt::TaskAttempt, dispatcher::TaskDispatcher,
        handler::{TaskHandler, TaskHandlerError}, runtime::config::TaskRuntimeConfig,
    },
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// The task handler. A task arrives at least once: the same one may come back after a crash or a
/// timeout, so the effect is keyed by the task id and repeating it is harmless.
struct SendETicketHandler {
    mailer: Mailer,
}

impl TaskHandler<SendETicket> for SendETicketHandler {
    async fn handle(&mut self, payload: SendETicket, attempt: &TaskAttempt) -> Result<(), TaskHandlerError> {
        self.mailer
            .send(attempt.task_id(), &payload.seat)
            .await
            .map_err(|error| TaskHandlerError::Retry { reason: error.to_string(), retry_after: None })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    LoggingConfig::from_env()?.try_init()?; // RUST_LOG, one level

    // One pool for the commands and the task queue: short statements, each under a statement timeout.
    let pool = connect_pool(
        "postgresql://cqrsx:cqrsx@localhost:5433/cqrsx".parse()?,
        ConnectionPingPolicy::Idle(DEFAULT_IDLE_PING_AFTER),
        StatementTimeout::Default,
    )
    .await?;

    // One executor per aggregate type and process: it owns the aggregate locks. Share it; a second
    // executor of the same type behaves like another process. A commit wakes the type's processors.
    let wakeup = CommitWakeup::new();
    let config = PostgresCommandConfig::new(10, AggregateConflictRetryPolicy::default(), LockLimits::default())?;
    let ledgers = Arc::new(
        PostgresCommandExecutor::<Ledger>::new(pool.clone(), config, Rules { limit: 4 }).with_commit_wakeup(wakeup.clone()),
    );

    // The read side: a processor per read model reads the journal from its checkpoint row.
    let wallets = Arc::new(PostgresViewRepository::<Wallet, Ledger>::new("wallets", pool.clone()));
    let processor = ProjectionProcessor::new(
        "wallets",                                            // the row: projection name + aggregate type
        Arc::new(PostgresEventRepository::new(pool.clone())),
        Arc::new(PostgresCheckpointStore::new(pool.clone())), // PROJECTION_CHECKPOINTS_SCHEMA
        Arc::new(ViewProjection::new(wallets).with_name("wallets")),
        ProcessorConfig::default(),
    )
    .with_wakeup(wakeup);

    // The task handlers: a fresh set for every worker, one registration line per task type.
    let mailer = Mailer::new()?;
    let dispatcher = Arc::new(move || {
        TaskDispatcher::try_new(vec![BoxedTaskAdapter::json(SendETicketHandler { mailer: mailer.clone() })?])
    });
    let runtime = prepare_task_runtime(pool.clone(), dispatcher, TaskRuntimeConfig::from_env()?)?;

    // Serve until a signal: your inbound server, the task runtime and the read side side by side. On
    // the signal the processors finish what is in flight and release their checkpoint rows.
    let shutdown = CancellationToken::new();
    let read_side = tokio::spawn(ProjectionRuntime::new().with_processor(processor).run(shutdown.clone()));
    let inbound = serve_http(ledgers, shutdown.clone()); // axum, tonic, …: ends when `shutdown` is cancelled
    lifecycle::run(inbound, runtime.run(shutdown.clone()), shutdown, shutdown_signal()).await?;
    read_side.await??;
    pool.close().await;
    Ok(())
}
```

## Projections from the journal

A `ProjectionProcessor` reads the journal of its aggregate type page by page from a checkpoint row (`projection_checkpoints`) that it holds under a lease, hands
every key's events to a worker of that key and moves the checkpoint behind what is applied; a commit wakes the
processors of its type, and on another node the poll finds it. Nothing is lost between the commit and the read
model, two nodes never apply at once, and a rebuild runs through the same workers: the whole journal key by key
(`request_rebuild`, with a pace between the events of a key for people to watch), one key
(`request_rebuild_key`), or a document that refused a live event as a gap, healed on the spot.

The sketch above wires one: a `CommitWakeup` per aggregate type, shared by the executor
(`with_commit_wakeup`) and the type's processors (`with_wakeup`), and a `ProjectionRuntime` that runs the
processors of the process until the shutdown token.

`CheckpointStore::status` shows where a projection stands: the position, the owner and its lease, the failed
rounds in a row with the last error, and the state of a rebuild. How far it is behind is
`PostgresEventRepository::count_after(position, cap)`: the events of its type above the checkpoint, counted up to a
cap. The processor's observer reports every applied
batch, every move of the checkpoint, every failure and rebuild, for an event feed or a status page.

### Read models in MongoDB

With the `mongodb` feature the checkpoint moves next to the read models: the journal stays in PostgreSQL, the
views and the checkpoint live in one MongoDB database. The application builds the client with its own options.

```rust,ignore
use cqrsx::mongodb::{projection_checkpoints::MongoCheckpointStore, view_repository::MongoViewRepository};

let read_db = mongodb::Client::with_options(options)?.database("read");
let wallets = Arc::new(MongoViewRepository::<WalletView, Ledger>::new(&read_db, "wallets"));
let processor = ProjectionProcessor::new(
    "wallets",
    Arc::new(PostgresEventRepository::new(pool.clone())), // the journal
    Arc::new(MongoCheckpointStore::new(&read_db)),       // collection `projection_checkpoints`
    Arc::new(ViewProjection::new(wallets).with_name("wallets")),
    ProcessorConfig::default(),
)
.with_wakeup(wakeup);
```

A view is one document `{ _id: view id, version, last_sequence, view }`; the repository writes and loads by id,
and the application queries the collection by that shape. Each checkpoint is one document, and every operation on
it is one atomic statement by the server's clock (MongoDB 4.2 or later).

## Configuration

Every tunable is a checked value with a default. Only two helpers read the environment, and they say so
in their names.

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
| Logging | `LoggingConfig::from_env()`: `RUST_LOG`, one level | `info` |

The task runtime reads `OUTBOX_WORKER_CONCURRENCY` (128), `OUTBOX_QUEUE_CAPACITY` (128),
`OUTBOX_POLL_INTERVAL_MS` (1000), `OUTBOX_HANDLER_TIMEOUT_SECS` (15), `OUTBOX_LEASE_SECS` (60) and
`OUTBOX_STORAGE_TIMEOUT_MS` (6000), and refuses a lease shorter than the worst case of one claim:
`2·storage + handler + 5 s + ceil(capacity / concurrency)·(handler + storage)`.

## Rules worth knowing

- **An event handler never executes a command**, not even from a task it spawns: a projection writes its
  read model and nothing else, and a follow-up started there would be recorded nowhere and repeated by every
  rebuild. Pass a `NewTask` with the command instead and let a task handler run the follow-up.
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
  key), answer `Retry` when it is worth another attempt and `Permanent` when it is not.
- **One executor per aggregate type and process.** Two executors of one type order nothing between them;
  the store's optimistic check still decides, like between two processes.
- **Do not cancel a command's future while its commit is in flight.** The events may land while the caller
  learns nothing, and the processors find them at their next poll instead of at once. An HTTP server that
  drops handler futures on client disconnect should run the command in a task of its own.

## Testing

| Tool | For |
|---|---|
| `cqrs::testing::TestFramework` | An aggregate's rules: `with(services).given(events).when(command).then_expect_events(..)` or `then_expect_error_message(..)` |
| `cqrs::testing::command::TestCommandExecutor` | A use case: the production command path, with locks, retry and a commit wake-up, over `MemStore` or (`over_journal`) over a `MemEventRepository` that projection processors read; `take_bound_tasks()` and `take_published_tasks()` show what it carried and what committed |
| `cqrs::testing::mem_view_repository::MemViewRepository` | A projection and the queries over it |
| `cqrs::testing::mem_checkpoint_store::MemCheckpointStore` | A projection processor without a database, next to `MemEventRepository` and `MemViewRepository` (leases follow the Tokio clock, so a paused test clock drives them) |
| `cqrs::testing::unavailable_store::UnavailableStore` | The storage-down path |
| `task::attempt::TaskAttempt::first(..)` | A task handler, called directly with a first claim |

## Features

| Feature | Adds | Default |
|---|---|---|
| `postgres` | the PostgreSQL event store, outbox publication, task queue, view repository and pools | yes |
| `worker` | the task runtime: worker pool, polling, supervision, graceful drain | yes |
| `host` | process signals and the coordinated shutdown of inbound server and task runtime | no |
| `logging` | `LoggingConfig`: an explicit `tracing` subscriber from `RUST_LOG` | no |
| `mongodb` | the MongoDB read side: a view repository and the checkpoint store of projection processors | no |

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
