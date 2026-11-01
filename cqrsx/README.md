# cqrsx

CQRS and event sourcing for Rust: the application owns its business rules, and the components of this core
(journal, transactional outbox, task runtime, projections) are assembled by the engine, the crate
[`cqrsx-engine`](../cqrsx-engine/README.md).

## At a glance

1. An **aggregate** holds state and applies named, versioned events.
2. A **command** decides one event or a domain rejection against that state and its rules.
3. The aggregate's **reactions** describe durable tasks following each event; the event and tasks commit atomically.
4. A **task handler** runs a follow-up command or external effect with its context and dependencies.
5. A **view** folds the aggregate's journal into a queryable document; its processor keeps a checkpoint.

The application registers these pieces in modules and builds `Cqrs` (crate [`cqrsx-engine`](../cqrsx-engine/README.md))
in memory or with PostgreSQL and MongoDB.
The [typical scenario](../doc/typical_scenario.md) follows a cancellation through a refund.

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
  read model and nothing else, and a follow-up started there would be recorded nowhere, lost by a crash before it
  commits and repeated by every rebuild. Plan the follow-up as a task from the command's decision instead (it commits
  with the command's event and only with it) and let a task handler run it, deciding from the current state whether
  it still applies. The framework does not detect a command executed from an event handler; this rule is the only
  guard.
- **A failed projection does not undo the commit.** The command has answered already; the worker logs the
  cause once per batch and the processor retries (next rule). Rebuild a read model from history with
  `request_rebuild` or, for one key, `request_rebuild_key`.
- **A projection processor never skips an event.** A failed key stops the checkpoint before it, visibly
  (`failures`, `last_error`, the observer), and the round is retried after a growing pause; only a document
  that refused an event as a gap is rebuilt from its history on the spot.
- **A checkpoint belongs to its journal.** It holds a position of the journal, so the two are reset together
  ([resetting the databases](../cqrsx-engine/README.md#resetting-the-databases)).
- **A view document takes only the next event of its aggregate.** It is the fold of `View::apply` over the
  contiguous prefix `1..=last_sequence` of its aggregate's events, and the document, not the sender, decides what is
  new: delivery may repeat events and come from several processes (a rebuild next to live delivery, two nodes). A
  repeated event is skipped, a batch that jumps ahead fails with `SequenceGap` and writes nothing, so a document that
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
- **Do not cancel a command's future while its commit is in flight.** The events and their tasks may land while
  the caller learns nothing, and the processors find them at their next poll instead of at once. An HTTP server that
  drops handler futures on client disconnect should run the command in a task of its own.

## Outcomes and errors

A command answers `Result<CommitReport<A>, CommandError<A::Rejection>>`; its observer (`CommandObserver`) gets the
same answer as an `Outcome`. A task handler answers a `TaskResult`.

| Answer | What happened | What to do |
|---|---|---|
| `Ok(CommitReport)`, `Outcome::Accepted` | the event committed, with the tasks planned from it | nothing |
| `CommandError::Rejected`, `Outcome::Rejected` | a business rule refused the command (`Rejection::code` and its parameters); nothing was written | do not repeat the same command; a task handler fails the task |
| `CommandError::Conflict`, `Outcome::Conflict` | every attempt of the conflict retry (`AggregateConflictRetryPolicy`) met a concurrent change; nothing was written | repeat later; a task handler retries |
| `CommandError::Unavailable`, `Outcome::Unavailable` | not run, or its outcome is unknown: the lock was not acquired within its limits (`LockLimits`), the store failed or timed out, the tasks could not be planned; the reason is logged | repeat later: with a command id (`#[command_id]`) a repeat cannot publish its tasks twice |
| `TaskResult::done()` | the effect happened, now or by an earlier attempt | the task is completed |
| `TaskResult::retry(reason)` | not now | another attempt after the pause of the task type's policy; the last attempt of its budget fails the task |
| `TaskResult::fail(reason)` | no attempt will help | the task stays failed until an operator requeues it ([retrying a failed task](../cqrsx-engine/README.md#retrying-a-failed-task)) |

The answer of `ctx.execute::<A>(..)` converts into a `TaskResult` with `.into()`: accepted is done, a rejection fails
the task with its code, a conflict or unavailability retries it. A handler that panics or outlives the handler timeout
is retried like `retry`, with the reason `handler panicked` or `handler timed out`. The errors of building and running
an engine are in [its README](../cqrsx-engine/README.md#errors-of-building-and-serving).

## Testing

| Tool | Crate | For |
|---|---|---|
| `TestFramework` | `cqrsx-testing` | Aggregate given/when/then tests of a decision and a rejection, without a runtime. |
| `TestCommandExecutor` | `cqrsx-testing` | Use-case tests over a test store or a journal in memory. |
| `MemoryStore`, `UnavailableStore` | `cqrsx-testing` | An event store in memory; one that is never reachable, for the unavailable branches. |
| `TaskContext::new` | `cqrsx` | Exercise a handler with an attempt, a retry pause, dependencies and executors. |
| `MemoryTaskQueue` | `cqrsx` | Inspect the in-memory outbox queue, delivery and settlement. |
| `Cqrs::in_memory()` | `cqrsx-engine` | A whole application: `run_tasks()` runs the due tasks, `catch_up()` waits for the views, a test's `.provide` replaces a dependency by type. |

## Crates

The core has no features and does not know a database. The stores (`cqrsx-postgres`, `cqrsx-mongodb`), the engine,
the test tools and the derive macros are crates of their own: [Crates and their boundaries](../doc/crates.md).

## Further reading

- [`doc/components.md`](../doc/components.md): the inventory of components, the naming contract and the
  integration rules in full.
- [`doc/guarantees.md`](../doc/guarantees.md): every guarantee of the engine, with the code that holds it and its tests.
- [Command path](../doc/command-path.html), [read path](../doc/read-path.html), [task path](../doc/task-path.html): how
  a command, an event and a task travel through the engine, step by step, with their guarantees and weak spots.
- [`../demo`](../demo/README.md): a complete application on the library, with a UI, an X-ray of both
  databases and load tests.
- [`../README.md`](../README.md): building, the local database, the test suite and coverage.
- [`../cqrsx-postgres/sql/`](../cqrsx-postgres/sql/): the canonical schemas and the operator script that requeues a failed task.

## Attribution

Parts of this crate are modified code of [`cqrs-es` 0.5.0](https://github.com/serverlesstechnology/cqrs) (Apache-2.0,
[`licenses/`](licenses/)); the notice is in [Upstream attribution](../doc/components.md#upstream-attribution).
