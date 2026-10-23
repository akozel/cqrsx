# cqrsx

CQRS and Event Sourcing for Rust services: aggregates and command execution, event and snapshot
persistence, read-model projections, a transactional outbox and a durable task queue. PostgreSQL stores
the data, Tokio runs the background work. The application owns the business rules; the library owns
consistency, ordering and retries.

## At a glance

- **One command, one lock, one transaction.** Inside a process the commands to one aggregate run one at
  a time, in arrival order. The event, an optional snapshot and the command's outbox tasks commit in a
  single statement; a conflict with another process is retried with backoff.
- **Projections in commit order, off the lock.** Committed events reach every event handler through a
  bounded buffer per aggregate key. The command releases its lock first and then waits for the handler's
  receipt, so a read model is up to date when the client gets its answer.
- **Durable follow-ups.** Anything that must survive a crash is an outbox task: claimed under a lease,
  retried with backoff, run by a worker pool that drains on shutdown.
- **Testable without a database.** A given/when/then framework for aggregates, an in-memory store and a
  command executor that records the tasks a use case publishes.

```text
command ─► CommandExecutor ─► CqrsFramework ─► Aggregate ─► EventStore ──┐ one transaction:
               │  aggregate lock                                         ├─ event, snapshot,
               │                                                         └─ outbox tasks
               └─► ordered delivery per aggregate key ─► EventHandler ─► ViewProjection ─► read model

transactional_outbox ─► TaskRuntime ─► Worker ─► TaskDispatcher ─► TaskHandler ─► e-mail, next command, …
```

## Quick start

A fan's ticket ledger: an aggregate with one command and one event, a read model and a follow-up task.
Everything below runs in memory, and the same types run unchanged on PostgreSQL.

```rust
use cqrsx::cqrs::{
    aggregate::Aggregate,
    command::CommandExecutor,
    event::{DomainEvent, EventEnvelope},
    event_handler::delivery::DeliveryConfig,
    event_sink::EventSink,
    projection::{View, ViewProjection, repository::ViewRepository},
    testing::{TestFramework, command::TestCommandExecutor, mem_store::MemStore, mem_view_repository::MemViewRepository},
};
use cqrsx::task::{NewTask, format::TaskPayload};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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

// The read model: a wallet, kept up to date by a projection from the committed events.
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

    // The command path over an in-memory store: aggregate locks, conflict retry, the projection behind
    // an ordered buffer per aggregate key, and a record of the tasks every call carries.
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let wallets = Arc::new(MemViewRepository::<Wallet, Ledger>::default());
        let ledgers = TestCommandExecutor::new(MemStore::<Ledger>::default(), Rules { limit: 4 })
            .with_event_handler(Arc::new(ViewProjection::new(wallets.clone())), DeliveryConfig::default());

        let task = NewTask::new(SendETicket { seat: "B4".into() }).unwrap();
        let command = LedgerCommand::RecordTicket { seat: "B4".into() };
        ledgers.execute("fox", command, vec![task]).await.unwrap();

        // The command returned after the projection applied its event…
        let wallet = wallets.load("fox").await.unwrap().unwrap();
        assert_eq!(wallet.seats, ["B4"]);
        // …and the task was published with the event: on PostgreSQL, in the same transaction.
        assert_eq!(ledgers.take_published_tasks().len(), 1);
    });
}
```

## Running on PostgreSQL

Apply the two schemas once, `postgres::EVENT_STORE_SCHEMA` and `postgres::TRANSACTIONAL_OUTBOX_SCHEMA`
(the compose file in the repository root does it for a local database; a read-model table is the
application's, see `PostgresViewRepository`). Then assemble the process with the types of the quick
start. This is a sketch; the demo back-end in `../demo/back-end/src/process.rs` and
`../demo/back-end/src/bootstrap/` is the complete, tested version.

```rust,ignore
use cqrsx::{
    cqrs::{
        command::{locks::LockLimits, retry::AggregateConflictRetryPolicy},
        event_handler::delivery::DeliveryConfig,
        projection::ViewProjection,
    },
    host::{lifecycle, signal::shutdown_signal},
    logging::LoggingConfig,
    postgres::{
        command::{PostgresCommandConfig, PostgresCommandExecutor},
        pool::{ConnectionPingPolicy, DEFAULT_IDLE_PING_AFTER, StatementTimeout, connect_pool},
        task_runtime::prepare_task_runtime,
        view_repository::PostgresViewRepository,
    },
    task::{
        adapter::boxed::BoxedTaskAdapter, attempt::TaskAttempt, dispatcher::TaskDispatcher,
        handler::{TaskHandler, TaskHandlerError}, runtime::config::TaskRuntimeConfig,
    },
};
use std::{sync::Arc, time::Duration};
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

    // One executor per aggregate type and process: it owns the aggregate locks and the buffers of its
    // projections. Share it; a second executor of the same type behaves like another process.
    let wallets = Arc::new(PostgresViewRepository::<Wallet, Ledger>::new("wallets", pool.clone()));
    let config = PostgresCommandConfig::new(10, AggregateConflictRetryPolicy::default(), LockLimits::default())?;
    let ledgers = Arc::new(
        PostgresCommandExecutor::<Ledger>::new(pool.clone(), config, Rules { limit: 4 })
            .with_event_handler(Arc::new(ViewProjection::new(wallets.clone())), DeliveryConfig::default()),
    );
    let backlog = ledgers.backlog(); // what the projection has not applied yet; taken before `ledgers` moves away

    // The task handlers: a fresh set for every worker, one registration line per task type.
    let mailer = Mailer::new()?;
    let dispatcher = Arc::new(move || {
        TaskDispatcher::try_new(vec![BoxedTaskAdapter::json(SendETicketHandler { mailer: mailer.clone() })?])
    });
    let runtime = prepare_task_runtime(pool.clone(), dispatcher, TaskRuntimeConfig::from_env()?)?;

    // Serve until a signal: your inbound server and the task runtime side by side. Then give the
    // projections a moment to apply what is left in their buffers, and close the pool.
    let shutdown = CancellationToken::new();
    let inbound = serve_http(ledgers, shutdown.clone()); // axum, tonic, …: ends when `shutdown` is cancelled
    lifecycle::run(inbound, runtime.run(shutdown.clone()), shutdown, shutdown_signal()).await?;
    let _ = tokio::time::timeout(Duration::from_secs(5), backlog.drained()).await;
    pool.close().await;
    Ok(())
}
```

## Configuration

Every tunable is a checked value with a default. Only two helpers read the environment, and they say so
in their names.

| What | How | Default |
|---|---|---|
| How long, and how many, commands wait for one aggregate's lock | `LockLimits::new(max_wait, max_waiters)` | 1 s, 128 |
| Commands buffered per aggregate key for one handler, and per handler call | `DeliveryConfig::new(capacity, max_batch)` | 64, 32 |
| Conflict retry: attempts, base and maximum backoff, jitter | `AggregateConflictRetryPolicy::new(..)` | 3, 5 ms, 50 ms, 0.2 |
| Snapshot interval, conflict retry and lock limits of a PostgreSQL executor | `PostgresCommandConfig::new(..)` | your interval, the defaults above |
| Server-side cap on every statement of a pool | `StatementTimeout::{Default, Custom, Unlimited}` | 4 s |
| Connection check before hand-out | `ConnectionPingPolicy::{Default, Idle(d)}` | SQLx's; `Idle(100 ms)` recommended |
| Task runtime | `TaskRuntimeConfig::from_env()` | see below |
| Logging | `LoggingConfig::from_env()`: `RUST_LOG`, one level | `info` |

The task runtime reads `OUTBOX_WORKER_CONCURRENCY` (128), `OUTBOX_QUEUE_CAPACITY` (128),
`OUTBOX_POLL_INTERVAL_MS` (1000), `OUTBOX_HANDLER_TIMEOUT_SECS` (15), `OUTBOX_LEASE_SECS` (60) and
`OUTBOX_STORAGE_TIMEOUT_MS` (6000), and refuses a lease shorter than the worst case of one claim:
`2·storage + handler + 5 s + ceil(capacity / concurrency)·(handler + storage)`.

## Rules worth knowing

- **An event handler never executes a command**, not even from a task it spawns: the follow-up would be
  recorded nowhere, its failure would reach the client of a command that succeeded, and the handler
  would wait for its own receipt. Pass a `NewTask` with the command instead and let a task handler run
  the follow-up.
- **A failed handler does not undo the commit.** The client gets `CommittedWithEventHandlerFailure` with
  the handler's name; the worker logs the cause once per batch. Rebuild the read model from history
  with `EventReplay`.
- **The handlers of one command run side by side.** Inside one handler, a key's events arrive in commit
  order, possibly several commands per call; `max_batch = 1` turns the batches off.
- **Task handlers are at-least-once.** Make the effect repeat-safe (the task id is a ready idempotency
  key), answer `Retry` when it is worth another attempt and `Permanent` when it is not.
- **One executor per aggregate type and process.** Two executors of one type order nothing between them;
  the store's optimistic check still decides, like between two processes.
- **Do not cancel a command's future while its commit is in flight.** The events may land without the
  hand-over to the handlers. An HTTP server that drops handler futures on client disconnect should run
  the command in a task of its own.

## Testing

| Tool | For |
|---|---|
| `cqrs::testing::TestFramework` | An aggregate's rules: `with(services).given(events).when(command).then_expect_events(..)` or `then_expect_error_message(..)` |
| `cqrs::testing::command::TestCommandExecutor` | A use case: the production command path over `MemStore`, with locks, retry and ordered handlers; `take_bound_tasks()` and `take_published_tasks()` show what it carried and what committed |
| `cqrs::testing::mem_view_repository::MemViewRepository` | A projection and the queries over it |
| `cqrs::testing::unavailable_store::UnavailableStore` | The storage-down path |
| `task::attempt::TaskAttempt::first(..)` | A task handler, called directly with a first claim |

## Features

| Feature | Adds | Default |
|---|---|---|
| `postgres` | the PostgreSQL event store, outbox publication, task queue, view repository and pools | yes |
| `worker` | the task runtime: worker pool, polling, supervision, graceful drain | yes |
| `host` | process signals and the coordinated shutdown of inbound server and task runtime | no |
| `logging` | `LoggingConfig`: an explicit `tracing` subscriber from `RUST_LOG` | no |

Without PostgreSQL the core still compiles and tests: aggregates, the framework, the in-memory store,
projections, replay and the task contracts.

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
