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
| `TestFramework` | Aggregate given/when/then tests for a decision and rejection (crate `cqrsx-testing`). |
| `TestCommandExecutor` | Use-case tests over a test store or a journal in memory (crate `cqrsx-testing`). |
| `TaskContext::new` | Exercise a handler with an attempt, retry pause, dependencies and executors. |
| `MemoryTaskQueue` | Inspect the in-memory outbox queue, delivery and settlement. |

Whole applications are tested with `Cqrs::in_memory()` of [`cqrsx-engine`](../cqrsx-engine/README.md).

The lower-level building blocks and their contracts are in [`doc/components.md`](../doc/components.md).

## Stores

The core has no features and does not know a database: aggregates, command execution, the in-memory stores,
projections, the projection processor with its checkpoints and the task contracts. The stores are crates of their own,
which implement its ports:

| Crate | Holds |
|---|---|
| [`cqrsx-postgres`](../cqrsx-postgres/src/lib.rs) | the PostgreSQL event store, outbox publication, task queue, checkpoints, view repository and pools |
| [`cqrsx-mongodb`](../cqrsx-mongodb/src/lib.rs) | the MongoDB read side: a view repository and the checkpoint store of projection processors |

A store that wraps the error of its driver in `cqrsx::diagnostics::StorageFailure` gives the core's logs the category and
the code of the failure. The engine ([`cqrsx-engine`](../cqrsx-engine/README.md)) assembles the stores by its features.

## Further reading

- [`doc/components.md`](../doc/components.md): the inventory of components, the naming contract and the
  integration rules in full.
- [`../demo`](../demo/README.md): a complete application on the library, with a UI, an X-ray of both
  databases and load tests.
- [`../README.md`](../README.md): building, the local database, the test suite and coverage.
- [`../cqrsx-postgres/sql/`](../cqrsx-postgres/sql/): the canonical schemas and the operator script that requeues a failed task.

## Attribution

The CQRS framework and (in `cqrsx-postgres`) the PostgreSQL event and view repositories include modified code from
[`cqrs-es` and `postgres-es` 0.5.0](https://github.com/serverlesstechnology/cqrs), Apache-2.0,
copyright 2023 Serverless Technology Consulting, LLC; see [`licenses/`](licenses/), for the repositories
[`../cqrsx-postgres/licenses/`](../cqrsx-postgres/licenses/) and, for the test tools,
[`../cqrsx-testing/licenses/`](../cqrsx-testing/licenses/).
