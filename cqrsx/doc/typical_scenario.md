# A typical application on cqrsx

This guide describes the API implemented by the engine. A complete running application is
[Encore](../../demo/README.md); the standalone [quick start](../README.md#quick-start) exercises the same
shape in memory. The detailed contracts and lower-level components are in [Components](components.md).

## Overview and module boundaries

An application contains modules such as a concert box office and a fan's ledger. Each module owns its
domain (aggregate, commands, events and rules), application (reactions, tasks, views), and presentation
(HTTP routes). A file inside a module is named by its role (`domain/aggregate.rs`, `domain/commands/`,
`domain/events/`), never by a name its path already says. A module root registers its pieces. Shared
contracts and infrastructure live outside the modules; modules do not reach into each other's
internals. The engine assembles their components; it does not decide business rules. See the [concert root](../../demo/back-end/src/concert.rs),
[fan root](../../demo/back-end/src/fan.rs), and [architecture tests](../../demo/back-end/tests/architecture.rs).

## Aggregate, events and typed ID

An `Aggregate` has an `Id`, command, event, error and rules type. It starts with `Default` and `apply`
folds a committed event into its state. A typed ID's `Display` supplies the journal key. `Event` and
`Events` derive the persisted name and version for facts and their aggregate enum. A snapshot and the
events after it restore the state. See [Concert](../../demo/back-end/src/concert/domain/aggregate.rs)
and [Fan](../../demo/back-end/src/fan/domain/aggregate.rs).

## Commands and decisions

Each command implements `Decide<A>`: given the current aggregate and its rules it returns one event or
a rejection, without side effects. `Command` and `Decide` derive, for an aggregate's enum of commands, the information the engine needs and the routing of the decision;
`Rejection` supplies a code and parameters. A caller uses `cqrs.from("ui").execute::<A>(id, command)`;
inside a task, `ctx.execute::<A>` continues the task's causation. The executor serializes commands per
key in a process; an optimistic conflict across processes reloads and retries the decision. See
[BookSeat](../../demo/back-end/src/concert/domain/commands/book_seat.rs).

## Reactions and commit

`Reactions::react` is a pure, synchronous match over an aggregate's events. A reaction receives a
`Decided` and adds follow-up tasks with `Then::task`; it does not execute them or do I/O. The engine
plans again on a conflict and commits the event, optional snapshot and tasks atomically. Rejections
publish no tasks. Command metadata carries causation and correlation through the chain. See
[concert reactions](../../demo/back-end/src/concert/application/reactions.rs) and
[fan reactions](../../demo/back-end/src/fan/application/reactions.rs).

## Task handlers, policies and dependencies

A versioned `Task` and its `#[task_handler]` live near the step they describe. The handler receives
the task, a `TaskContext` when needed, and dependencies by type. Module roots supply dependencies and
bind policies to handlers: the policy provides the attempt budget and retry pause. The shared handler
returns done, retry or fail; an external effect must be repeat-safe because delivery is at least once.
The task ID is suitable as an idempotency key. See
[record_cancellation](../../demo/back-end/src/fan/application/record_cancellation.rs),
[issue_refund](../../demo/back-end/src/fan/application/issue_refund.rs), and
[fan module](../../demo/back-end/src/fan.rs).

## Views and queries

A `View<A>` names a collection and folds events from one aggregate stream. The engine keeps a
checkpoint and applies events per key in order. A view document remembers its last applied sequence:
duplicate delivery is skipped and a gap stops the write; the processor can rebuild from history.
One collection has one projection. The engine offers get, list and range queries and operations such
as status, rebuild and verify. Verify compares each document with the fold of its applied history.
See [concert hall](../../demo/back-end/src/concert/application/views/concert_hall.rs) and
[fan wallet](../../demo/back-end/src/fan/application/views/fan_wallet.rs).

## Bootstrap and configuration

`Cqrs::in_memory()` assembles the same module registrations over in-memory stores. For a durable
application, `Cqrs::from_env("ENCORE")` reads engine and module keys with that prefix; `OUTBOX_*`
remain unprefixed. `.defaults(DEMO_DEFAULTS)` supplies application defaults only when a variable is
absent. Register `Postgres`, `MongoDb` and module roots, then `serve()` with the `http` feature. `Setup`
provides module-local configuration and engine settings, not shared infrastructure. The exact running
assembly is [main.rs](../../demo/back-end/src/main.rs); configuration and feature flags are in the
[README](../README.md#configuration).

## Cancellation to refund

1. The HTTP endpoint sends `CancelBooking` to the concert. Its decision emits `BookingCancelled`; the
   concert reaction plans `RecordCancellation`, committed with that event.
2. The task handler commands the fan ledger. If the cancellation arrives before its ticket, the step
   retries; once accepted it emits `TicketCancelled` and the fan reaction plans `IssueRefund`.
3. The refund handler invokes the mail service with an idempotency key derived from the cancellation ID. Its type's policy
   determines retry pauses and budget. Read-model processors independently fold the journal into the
   hall and wallet; retrying a task or rebuilding a view must not send duplicate effects.

The code is in [cancel booking](../../demo/back-end/src/concert/domain/commands/cancel_booking.rs),
[concert reactions](../../demo/back-end/src/concert/application/reactions.rs),
[record cancellation](../../demo/back-end/src/fan/application/record_cancellation.rs),
[fan reactions](../../demo/back-end/src/fan/application/reactions.rs), and
[issue refund](../../demo/back-end/src/fan/application/issue_refund.rs).

## Testing and rules

`TestFramework` (crate `cqrsx-testing`) tests an aggregate's decisions; `Cqrs::in_memory()` tests modules and their chains;
`run_tasks()` runs due work and `catch_up()` waits for views. A task context and `MemTaskQueue` permit
focused task tests. Modules register one executor per aggregate and one owner per view name; handlers
must tolerate repeat delivery, while reactions must stay pure. The tests in
[`tests/engine/scenario.rs`](../../cqrsx-engine/tests/engine/scenario.rs) cover the full in-memory path.
