# CLAUDE.md

## Project

`cqrsx` is a Rust library (edition 2024, toolchain pinned in `rust-toolchain.toml`) for CQRS and
Event Sourcing: aggregates and command handling, event and snapshot persistence, projections,
a transactional outbox, and a durable task queue with retries. PostgreSQL stores data (MongoDB may
hold the read side), Tokio runs background tasks. No binary: `cargo run` is unavailable.

- `cqrsx/src/domain` — aggregates, events, how a command decides, rejections.
- `cqrsx/src/journal` — event store, repositories, snapshots, feed, commit wake-up.
- `cqrsx/src/command` — command execution: attempt, executor, locks, retry, causation, task planning.
- `cqrsx/src/reactions.rs` — the tasks that follow each event of an aggregate.
- `cqrsx/src/projection` — event handlers, projection checkpoints, the processor that feeds them from the journal.
- `cqrsx/src/view` — read models: `View`, their projection, repository and verification.
- `cqrsx/src/memory` — the backend in memory: journal, checkpoints, views, task queue.
- `cqrsx/src/testing` — downstream test tools: aggregate given/when/then, test executor, in-memory store.
- `cqrsx/src/task` — a task as data: `NewTask`, format, attempt, policies.
- `cqrsx/src/worker` — running tasks: handlers, dispatcher, adapters, executor, queue port, runtime.
- `cqrsx/src/diagnostics.rs` — the one public part of the log module: `safe_type`, `StorageFailure` (category and code
  of a failed storage operation, which a store's crate gives the core's logs); the log lines are private.
- `cqrsx/src/mutex.rs` — `lock`, `get_mut`: the one public helper that takes a `std::sync::Mutex` (rule 10).
- `cqrsx-engine/src` — the engine, a crate of its own on top of `cqrsx` (the core does not know it): `lib.rs` (`Cqrs`),
  `build.rs` (`Builder`, the assembly over the stores), `executor.rs`, `settings.rs`, `module.rs`, `setup.rs`, `outcome.rs`,
  `views.rs`, `memory.rs`; `postgres.rs` (feature `postgres`), `mongodb.rs` (feature `mongodb`), `http.rs` and `serve.rs` with
  `serve/{lifecycle,signal,error,logging}.rs` (feature `http`).
- `cqrsx-engine/tests` — integration tests of the engine; `engine.rs` runs it on PostgreSQL and MongoDB containers
  (from `cqrsx-fixtures`).
- `cqrsx/tests` — integration tests of the core, in memory.
- `cqrsx-postgres` — the PostgreSQL stores: event repository, outbox, task queue, checkpoints, views, pools, `sql/`
  (no features); `tests/postgres.rs` starts PostgreSQL via `testcontainers`, `tests/log_fallback.rs` checks the logs
  without a `tracing` subscriber. Derived in part from postgres-es: `licenses/`.
- `cqrsx-mongodb` — views and projection checkpoints in MongoDB (no features); `tests/mongo.rs` starts MongoDB,
  `journal_in_postgres` also PostgreSQL.
- `cqrsx-fixtures` — `publish = false`, a dev-dependency that does not depend on `cqrsx`: PostgreSQL and MongoDB
  containers on leased ports, captured logs, isolated test processes.
- `cqrsx/doc/components.md` — architecture and naming contract. Read it before adding components.
- `demo/PLAN.md` — plan for the demo application (not yet a workspace member).

## Code guidelines

Rust code follows `.claude/rules/rust-style.md`: the Google Rust Style Guide adapted to this project
(no panics outside tests, borrow before clone, newtypes only for values easy to mix up, a parent module
holds its contract, no `#[non_exhaustive]`), with the list of Google rules not adopted. `/rust-style-review`
checks code against it. The rules apply to new and changed code: old code is not rewritten only for style,
defects are fixed wherever found.

## Tests and coverage

Every change ships with tests. Cover new code paths, including error branches, with unit tests
next to the code, integration tests in the crate's `tests`, or doctests.

Verification target: CI enforces **97% line coverage** for the workspace and for `cqrsx`, `cqrsx-postgres`,
`cqrsx-mongodb` and `cqrsx-engine` alone (`cqrsx-fixtures` counts only in the workspace total). Keep every feature of
the engine compiling and tested on its own and with `--no-default-features`: `postgres`, `mongodb` and `http`; the
other crates have none.

Integration tests need the Podman API socket; `DOCKER_HOST` must point to it
(`unix:///run/podman/podman.sock` in this environment). Fix failures, never relax thresholds or
ignore tests.

## Commands

```bash
cargo build --locked --workspace --all-features
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
RUST_TEST_THREADS=2 PROPTEST_RNG_SEED=42 cargo test --locked --workspace --all-features
cargo test --locked -p cqrsx -p cqrsx-postgres -p cqrsx-mongodb   # the crates without features, one by one
cargo test --locked -p cqrsx-engine --no-default-features     # engine in memory
cargo test --locked -p cqrsx-engine --no-default-features --features postgres   # also: mongodb, http
cargo llvm-cov --locked --workspace --all-features --fail-under-lines 97
cargo llvm-cov --locked -p cqrsx --fail-under-lines 97     # also: -p cqrsx-postgres, -p cqrsx-mongodb
cargo llvm-cov --locked -p cqrsx-engine --all-features --fail-under-lines 97
```

Before finishing: fmt, clippy, full tests and all coverage runs must pass.
