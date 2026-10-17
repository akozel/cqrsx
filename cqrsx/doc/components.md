# Components and terminology

This is the current inventory and naming contract for `cqrsx`.
Names follow CQRS, DDD and Event Sourcing. A command expresses intent; an aggregate
checks invariants; a domain event records an accepted fact. A query reads a read
model. An event handler reacts to committed events; a projection builds a read model.
An aggregate event history remains the source of truth; snapshots are checkpoints.

CqrsFramework coordinates command handling, EventStore commit and event-handler
notification. The logical command path is:

```text
CommandExecutor → CqrsFramework → Aggregate → EventSink → EventStore
                                                        ↓ commit
                                                   EventHandler
                                                        ↓
                                                  ViewProjection → View
```

The durable task path is:

```text
NewTask → atomic outbox publication → TaskQueue
                                        ↓ claim
TaskRuntime → Worker → TaskExecutor → TaskDispatcher → TaskAdapter → TaskHandler
                         ↓ settle
                      TaskQueue
```

## Inventory

The table lists concrete components and implementation roles. A role may be a
function, module, callback or channel; it does not require a dedicated struct.
Related configuration and validation errors stay with their owning boundary.

| Role | Components | Responsibility |
| --- | --- | --- |
| Aggregate contract | `Aggregate`, associated `Command`, `Event`, `Error`, `Services`; `handle`, `apply` | Validate commands and deterministically transition aggregate state. `TYPE` identifies an aggregate type, not an instance. |
| Domain events | `DomainEvent`, `EventEnvelope` | Domain facts; typed payload plus aggregate ID, sequence and metadata. |
| Event emission | `EventSink` | Apply events to the working aggregate immediately and buffer them before persistence. |
| Command orchestration | `CqrsFramework`, `AggregateError` | Load committed state, handle a command on a copy, commit, then invoke event handlers. `Domain` is a business rejection. |
| Post-commit failures | `CommittedWithEventHandlerFailure`, `EventHandlerFailure` | Record the committed sequence, registration indices and original handler failures without retrying the command. |
| Command boundary | `CommandExecutor`, `CommandGatewayError` | Technical execution port and safe domain/conflict/unavailable/committed outcome categories. `execute_with_metadata` stores one metadata map with every committed event; `execute` defaults to an empty map. |
| Command implementation | `CqrsCommandExecutor`, `BuildFramework` callback | One framework per invocation with immutable prebuilt tasks and bounded conflict retry. The callback is a factory. |
| Conflict retry | `AggregateConflictRetryPolicy`, `AggregateConflictRetryPolicyError`, `retry_on_aggregate_conflict` | Repeat only confirmed optimistic conflicts; independent of durable task redelivery. |
| Event store | `EventStore`, `AggregateContext`, `PersistenceError` | Typed reconstruction and commit against the last committed context. |
| Repository-backed store | `PersistedEventStore`, `PersistedEventRepository`, `EventStoreAggregateContext` | Serialized repository access, history replay, snapshot acceleration and optimistic metadata. |
| Snapshot decisions | private `SnapshotPolicy::{Disabled, Every}`, `snapshot_prefix_len`, `checked_counter_add` | Choose checkpoints and check event-sequence/snapshot-version arithmetic. |
| Snapshot records | `SerializedSnapshot`, `SnapshotUpdate` | Saved checkpoint and prepared update: state, exact `last_sequence` watermark and snapshot version. |
| Event serialization | `SerializedEvent`, serialization/decoding functions, private `EnvelopeMismatch` | Persistence representation and discriminator/version validation. |
| Event compatibility | `EventUpcaster`, `NumericVersion`, `NumericVersionError`, `NumericVersionUpcaster`, `EventPayloadTransform` | Upcast serialized payloads; numeric version comparison preserves the existing parser, rather than promising SemVer. |
| Saved-history streaming | `ReplayStream`, `ReplayFeed`, PostgreSQL stream producer | Bounded asynchronous reading of existing events, for one ID or all IDs of an aggregate type. No subscription to future commits. |
| Event reaction | `EventHandler` | Handle committed events outside their original transaction. It is not a CQRS query or a durable task. |
| Event replay | `EventReplay` | Read saved history into an event handler, stopping at the first failure; separate from aggregate reconstruction. |
| Read-model data | `View`, `ViewContext` | Materialized read-model state and its repository ID/version. |
| Read-model projection | `ViewProjection`, `ViewRepository`, `PersistenceErrorObserver` | Update stored views with conflict retry, read views, and observe borrowed persistence errors without consuming them. |
| Publication data | `NewTask`, `TaskSchedule`, `TaskValidationError`, shared payload/metadata validation | Checked immutable task envelope, initial eligibility and claim-attempt limit. |
| Wire format | `TaskPayload`, `TaskFormat`, `TaskFormatError` | Versioned business DTO contract and complete type/version routing key. |
| Handler input | `TaskAttempt` | Stable `task_id`, payload, metadata, format and positive claim `number`; no lease credentials. |
| Lease ownership | `TaskLease`, `LeasedTask`, `LEASE_SAFETY_MARGIN` | Platform lock token and original monotonic deadline; attempt plus ownership. |
| Business operation | `TaskHandler<P>`, `TaskHandlerError` | One repeat-safe operation, classified as success, retry or permanent rejection. |
| Wire adaptation | `TaskAdapter`, `JsonTaskAdapter<P,H>`, private `safe_format` | Validate/decode representations and invoke business work. Mutable handler state remains worker-owned. |
| Task routing | `TaskDispatcher<A>`, `TaskDispatcherError` | Checked nonempty, nonduplicate format registration and routing to one adapter. |
| Durable queue | `TaskQueue`, `QueueRequestError`, `TaskProcessingError`, `redelivery_backoff` | Atomic claim, ownership fencing, settlement and expired-lease recovery. |
| Attempt executor | `TaskExecutor`, `DefaultTaskExecutor<Q>` | Bound one leased invocation and settle its result in a separate storage operation. |
| Execution decision | private `ExecutionPlan::{Run, ScheduleRetry, AwaitRecovery}`, `execution_plan` | Decide whether the remaining lease permits invocation, only retry scheduling, or neither. |
| Settlement | `SettlementRequest`, `SettlementResult`, private result mapping | Requested transition versus confirmed completion, retry scheduling, failure or lost ownership. |
| Confirmed failure | `TaskFailure`, `TaskFailureCause`, `LeaseRecoveryReport` | Terminal transition and recovery report; `retry_scheduled_count` counts tasks returned to pending. |
| Prepared runtime | `TaskRuntime<Q,A>`, private `RuntimeContext<Q,E>`, `prepare`, `TaskRuntimeError` | Own queue/executor dependencies and retained startup dispatchers; consume them on run. |
| Dispatcher factory | `TaskDispatcherFactory<A>`, `build_checked_dispatcher`, `WorkerSetupError` | Construct fresh mutable handlers for each worker/restart and compare formats with the initial dispatcher. |
| Polling | private `Poller<Q>` | Rotating bounded multi-format claims, partial confirmed batches and expired-lease recovery. |
| Supervision | `supervisor::{run, run_prepared}` | Single-producer admission, polling, restart, cancellation and drain. |
| Worker ownership | private `WorkerPool<E>`, `WorkerExit`, `run_task_worker` | Own permanent Tokio workers and their dispatchers; observe, replace and drain them. |
| Local coordination | bounded `async_channel` (DispatchBuffer role), `Notify`, `JoinSet`, `CancellationToken`, `DropGuard` | Waiting capacity, notifications, worker observation and shared shutdown ownership. |
| Runtime configuration | `TaskRuntimeConfig`, `TaskWorkerPoolConfig`, `TaskStorageConfig`, corresponding `*ConfigError` types | Checked storage and worker limits plus cross-budget validation. `buffer_capacity` limits the local channel. |
| Event persistence | `PostgresEventRepository`, `PostgresViewRepository`, private `PostgresRepositoryError` | PostgreSQL events, snapshots and JSON read models; shared repository error conversion. |
| SQL operations | private `SqlQueryFactory`, read/write/stream modules, `decode_event_row`, `decode_snapshot_row`, numeric helpers | SQL generation, checked row conversion and complete transactions. `get_events_after` uses an exclusive lower watermark. |
| Atomic publication | `PostgresEventRepositoryWithOutbox`, private `WritePlan`, `SnapshotWrite`, `WriteValidationError`, writer | One event, optional checkpoint and explicit tasks in one atomic SQL statement. |
| PostgreSQL queue | `PostgresTaskQueue`, `PostgresTaskQueueConfig`, `PostgresTaskQueueConfigError`, `ClaimRow`, `RecoveryRow`, `SettlementParameters` | Complete claim/settlement/recovery transactions, row decoding and SQL statement timeout. |
| Backend assembly | `PostgresCommandExecutor`, `command_executor`, `PostgresCommandConfig`, `PostgresCommandConfigError`, `prepare_task_runtime`, `connect_pool` | Explicit PostgreSQL composition; prepared runtime construction performs no background work. |
| SQL assets | `EVENT_STORE_SCHEMA`, `TRANSACTIONAL_OUTBOX_SCHEMA`, `REQUEUE_FAILED_OUTBOX_TASK` | Canonical schemas and operator requeue with stable task identity. |
| Process lifecycle | `host::lifecycle::run`, `HostError`, private `StopReason`, `TaskRuntimeExit`, `shutdown_signal` | Signal/shared cancellation, inbound/runtime failure priority and coordinated drain. |
| Logging | `LoggingConfig`, `LoggingInitError` | Explicit process-wide subscriber installation; imports/defaults do not initialize it. |
| Technical support | private `diagnostics`, `env`, `log_task_failure` | Sanitized diagnostics, confirmed task-failure logging shared by execution/recovery, and integer parsing. |
| Public testing tools | `TestFramework`, `AggregateTestExecutor`, `AggregateResultValidator`, `MemStore`, `MemStoreAggregateContext`, private locked event map | Downstream given/when/then, assertions and in-memory test storage. |
| Private test support | inline CQRS fixtures, `task::test_support`, integration-suite fixtures, log capture and isolated-process helpers | Test-owned fakes and observation; no production service or singleton. |

There is no generic production Helper or Service to merge. Constructors and
fallible `with_*` methods provide fluent configuration without a separate Builder.
`Arc` denotes shared ownership, not singleton lifetime. The logging subscriber is
explicit process-global infrastructure. PostgreSQL is the production event-store
backend; MemStore remains a public testing tool.

## Source layout

```text
src/
├── cqrs.rs                         CqrsFramework and command execution failures
├── cqrs/
│   ├── aggregate.rs, event.rs, event_sink.rs
│   ├── command.rs, command/{executor,retry}.rs
│   ├── event_handler.rs            EventHandler
│   ├── event_handler/replay.rs     EventReplay
│   ├── projection.rs              View and ViewProjection
│   ├── projection/repository.rs    ViewRepository and ViewContext
│   ├── store.rs                    EventStore, AggregateContext, errors/observer
│   ├── store/
│   │   ├── persisted.rs            Store, snapshot policy, context and restoration
│   │   ├── repository.rs, snapshot.rs, serialized_event.rs, stream.rs, upcaster.rs
│   └── testing.rs, testing/         Public downstream testing toolkit
├── task.rs                         NewTask, schedule and structural validation
├── task/
│   ├── format.rs, attempt.rs, handler.rs, adapter.rs, adapter/json.rs, dispatcher.rs
│   ├── queue.rs, queue/{lease,settlement,storage_config,retry}.rs
│   ├── executor.rs, executor/{default_executor,lease_budget}.rs
│   ├── runtime.rs                  Prepared runtime, context, errors and prepare
│   ├── runtime/
│   │   ├── config.rs, worker_pool_config.rs, dispatcher_factory.rs
│   │   ├── supervisor.rs           Public custom-executor run, private engine
│   │   └── poller.rs, worker_pool.rs, worker.rs
│   └── test_support.rs             Private unit fixtures
├── postgres.rs                     SQL assets and adapter categories
├── postgres/
│   ├── pool.rs, command.rs, task_runtime.rs, error.rs
│   ├── event_repository.rs, event_repository/{read,write,stream,sql_query}.rs
│   ├── view_repository.rs
│   ├── outbox.rs, outbox/{write_plan,write}.rs
│   └── task_queue.rs, task_queue/{claim,settlement,recovery}.rs
├── host.rs, host/{lifecycle,error,signal}.rs
└── logging.rs, diagnostics.rs, env.rs
```

A component root owns its contract/state and related small errors. Children own
independent operations. Snapshot DTOs do not own restoration of a concrete store
context. Queue operations own their complete transactions; atomic outbox publication
has its own transaction boundary. Unit tests stay inline; integration fixtures stay
under `tests/`. No compatibility namespaces or flattening re-exports are provided.

## Naming and integration rules

- Use Query for reading, EventHandler for reacting to events, ViewProjection for
  building a materialized View. EventHandler has no durable-delivery guarantee;
  required durable reactions use outbox tasks. Replaying events must be safe for
  the selected handler, including its external effects.
- Use Stream for actual event/history transport, workflow for business sequencing,
  and aggregate phases for domain consistency. ReplayStream reads finite history.
- Use Task for durable work and attempt for a claim. `TaskAttempt::task_id()` is
  stable; `number()` counts claims, including skipped invocations. Use
  `tokio_task_id` for Tokio identities and handles for spawned execution.
- Handler performs the operation, Adapter translates representations, Dispatcher
  routes, Executor bounds one invocation, Worker consumes repeatedly, Supervisor
  manages workers, Runtime assembles the subsystem. EventHandler and TaskHandler
  share the handler role with different inputs and delivery contracts.
- Queue denotes durable ownership; Buffer denotes local waiting. The external
  environment name `OUTBOX_QUEUE_CAPACITY` maps to Rust `buffer_capacity` and stays
  supported. SQL names, task formats and other environment keys are unchanged.
- Config contains checked values; Policy chooses behavior; Factory constructs
  components; Plan precedes effects; Result/Report describe confirmed outcomes.
  Recovery scheduling is not successful task execution.
- Outbox denotes atomic event/task publication and its schema/environment contracts.
  Task execution and settlement use separate commits; they are not a UnitOfWork.
- Prefer `task::runtime::prepare` or
  `postgres::task_runtime::prepare_task_runtime`. Preparation retains C checked
  dispatchers and performs no storage/background work. Run starts them on first poll.
  Low-level `task::runtime::supervisor::run` accepts custom executors using the same
  lifecycle engine. Import contracts from their defining modules.

## Local PostgreSQL

From the workspace root, run `docker compose up -d postgres` (or
`podman compose up -d postgres`). Compose initializes the event-store and outbox
schemas in a dedicated `cqrsx` database and persistent volume. Initialization SQL
runs only for a fresh volume. Tests provision their own PostgreSQL containers.

Connect with `postgresql://cqrsx:cqrsx@localhost:5433/cqrsx`.
Set `CQRSX_POSTGRES_PORT` to override the default host port `5433`.
The credentials are intended for local development.
Stop PostgreSQL with `docker compose down`; the database volume is retained.

## Upstream attribution

The CQRS framework and PostgreSQL event/view repositories include modified code
from `cqrs-es` 0.5.0 and `postgres-es` 0.5.0, respectively, licensed under Apache-2.0.
Upstream repository: <https://github.com/serverlesstechnology/cqrs>.
The recorded source revisions are `b099f99fd2904ccfc428bd8f7f39f5b9f182227b`
and `9ff335592fb5aed1ee11bd7980fdeac195bc1909`, respectively.
Copyright 2023 Serverless Technology Consulting, LLC.

The local changes include the module layout, CQRS command execution, checked
snapshot/serialization contracts, fallible event handlers, replay error handling,
transactional outbox, durable tasks and runtime integration. Source headers retain
upstream attribution and modification notices. See the retained
[upstream license](../licenses/Apache-2.0.txt); this notice does not relicense
unrelated project code.
