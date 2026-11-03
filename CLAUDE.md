# CLAUDE.md

## Project

`cqrsx` is a Rust library (edition 2024, toolchain pinned in `rust-toolchain.toml`) for CQRS and
Event Sourcing: aggregates and command handling, event and snapshot persistence, projections,
a transactional outbox, and a durable task queue with retries. PostgreSQL stores data (MongoDB may
hold the read side), Tokio runs background tasks. No binary: `cargo run` is unavailable.

- `cqrsx/src/domain` — aggregates, events, how a command decides, rejections.
- `cqrsx/src/journal` — the aggregate store, event repositories, snapshots, feed, commit wake-up.
- `cqrsx/src/command` — command execution: attempt, executor, locks, retry, causation, task planning.
- `cqrsx/src/reactions.rs` — the tasks that follow each event of an aggregate.
- `cqrsx/src/projection` — event handlers, projection checkpoints, the processor that feeds them from the journal.
- `cqrsx/src/view` — read models: `View`, their projection, repository and verification.
- `cqrsx/src/memory` — the backend in memory: journal, checkpoints, views, task queue (feature `test-utils`; the crate's
  own tests always have it).
- `cqrsx/src/testing` — the test tools of an application: `framework.rs` (`TestFramework`, aggregate given/when/then),
  `validator.rs`, `command_executor.rs` (use-case tests over a journal in memory), `unavailable_store.rs` (feature
  `test-utils`, as `memory`); `tests/testing.rs` runs them on an example application.
- `cqrsx/src/task` — a task as data: `NewTask`, format, attempt, policies.
- `cqrsx/src/worker` — running tasks: handlers, dispatcher, adapters, executor, queue port, runtime.
- `cqrsx/src/diagnostics.rs` — the one public part of the log module: `safe_type`, `StorageFailure` (category and code
  of a failed storage operation, which a store's crate gives the core's logs); the log lines are private.
- `cqrsx/src/mutex.rs` — `lock`, `get_mut`: the one public helper that takes a `std::sync::Mutex` (rule 10).
- `cqrsx-framework/src` — the framework, a crate of its own on top of `cqrsx`: `lib.rs` (`Cqrs`),
  `build.rs` (`Builder`, the assembly over the stores), `stores.rs` (the store switch: `Journal`, the executor and the
  task queue over the chosen stores, what every store gives the views), `settings.rs`, `module.rs`, `setup.rs`, `source.rs`,
  `views.rs`, `memory.rs` (feature `test-utils`: the marker `InMemory` and the stores in memory, over `cqrsx::memory`),
  `diagnostics.rs` (the framework's own log line);
  `postgres.rs` + `postgres/` (feature `event-store-postgres`: the event repository, outbox, task queue, pools;
  `checkpoint_store` and `view_repository` with `projections-postgres`), `mongodb.rs` + `mongodb/` (feature
  `projections-mongodb`: views and projection checkpoints in MongoDB), `process.rs` with
  `process/{lifecycle,error,logging}.rs` (the process: the task runtime and the processors next to any server, the
  logger, the shutdown signal; no feature), `axum.rs` (feature `axum`: the HTTP API over axum), `operation_utils.rs`
  (feature `operation-utils`: the operations that read without a bound or change the read side), `test_utils.rs`
  (feature `test-utils`: the helpers of a test that drives a framework in memory).
- `cqrsx-framework/sql` — the canonical PostgreSQL schemas. `cqrsx-framework/licenses` — the license of postgres-es, from
  which the PostgreSQL stores are derived in part.
- `cqrsx-framework/tests` — integration tests of the framework; `framework.rs` runs it on PostgreSQL and MongoDB containers
  (from `cqrsx-fixtures`); `postgres.rs` starts PostgreSQL via `testcontainers` and runs the stores against it,
  `log_fallback.rs` checks the logs without a `tracing` subscriber; `mongodb.rs` starts MongoDB, `journal_in_postgres`
  also PostgreSQL.
- `cqrsx/tests` — integration tests of the core, in memory.
- `cqrsx-macros` — the derive macros (`Event`, `Events`, `Command`, `Decide`, `Task`, `Rejection`) and `#[task_handler]`.
- `cqrsx-fixtures` — a dev-dependency of the crates' tests: PostgreSQL and MongoDB containers on leased ports, captured
  logs, isolated test processes.
- `doc/crates.md` — the crates, what each may and must not know, the rules of their boundaries (no crate is published).
  `doc/components.md` — the components and the naming contract: read both before adding a component or a crate.
- `demo/` — Encore, the demo application on the library: a workspace of its own (`exclude` in the root `Cargo.toml`);
  `demo/README.md` is its entry and the map of its documents, `demo/DESIGN.md` its design.

## Documentation

| What | Where |
|---|---|
| The library: overview, environment for the tests; the commands | `README.md`; `CLAUDE.md` (Commands) |
| Architecture and naming, crates and their boundaries, an application step by step, guarantees | `doc/components.md`, `doc/crates.md`, `doc/typical_scenario.md`, `doc/guarantees.md` |
| How the mechanisms work, explained with diagrams: command path, read path, task path | `doc/*.html` |
| A crate | its `README.md` (`cqrsx`, `cqrsx-framework`, `cqrsx-macros`) or one `//!` in its `lib.rs` (the others) |
| The demo: entry and map of its documents, design, load tests | `demo/README.md`, `demo/DESIGN.md`, `demo/load/README.md` |
| Unfinished work, findings without an epic, what was decided not to do; the journal of finished work | `doc/backlog/`, `doc/backlog/journal.md` (ignored by git) |
| Known problems and inaccuracies of the present code, why they stay | `doc/known_issues/` |

The documentation stays true only if it changes with the code:
- **A change updates the documents of what it changes, in the same commit:** a renamed type, function, column, log
  field or variable, a new variable or default, a new guarantee, a changed behaviour. Before finishing, search the old
  names in `*.md`, `*.html`, `.env.example` and doc comments.
- **One topic, one place.** Every other document links to it and does not retell it: the list of crates, the rules, the
  commands, the variables. A copy is the first text to go stale.
- **A document describes the present.** No history ("since task 10", "formerly", "was renamed"), no plans: the history
  is git and the journal, plans are the backlog.
- **What mirrors the code is checked by a test** where it can be: the doctests of the crate READMEs (compiled, not
  `ignore`), the golden files of the demo, the crate graph of `doc/crates.md`. A new table of names or defaults gets such
  a test or is not written.
- **Links resolve:** `python3 .github/scripts/check-links.py` (also run by CI) checks every relative link and anchor in
  the tracked `.md` and `.html`; run it before every commit that touches a document or moves a file. A tracked file
  never links into `doc/backlog/`: a clone has no such directory; naming a task in plain text ("task 05 of epic 09")
  is fine. The crate graph of `doc/crates.md` is checked against `cargo metadata` by
  `python3 .github/scripts/check-crate-graph.py` (also run by CI); run it when a dependency between crates changes.
- **Finished work leaves the backlog:** the task file is deleted and the journal gets one line (epic and task, date,
  result, commit).
- **English.** The documentation is in English, the demo's included. HTML pages are standalone files (`<!doctype html>`,
  `<meta charset="utf-8">`, no external resources) committed to the project.

## Code guidelines

Rust code follows `.claude/rules/rust-style.md`: the Google Rust Style Guide adapted to this project
(no panics outside tests, borrow before clone, newtypes only for values easy to mix up, a parent module
holds its contract, no `#[non_exhaustive]`), with the list of Google rules not adopted. `/rust-style-review`
checks code against it. The rules apply to new and changed code: old code is not rewritten only for style,
defects are fixed wherever found.

Application code on cqrsx (the demo) follows `.claude/rules/cqrsx-application.md`: modules, aggregates, tasks, command
ids and entry points. It is the one place of these rules: a decision about how an application uses cqrsx becomes a rule
there in the same change, a changed decision edits its rule, and the code follows the file.

## Tests and coverage

Every change ships with tests. Cover new code paths, including error branches, with unit tests
next to the code, integration tests in the crate's `tests`, or doctests.

Verification target: CI enforces **97% line coverage** for the workspace, for `cqrsx` and for `cqrsx-framework` alone
(`cqrsx-fixtures` counts only in the workspace total). The core has one feature, `test-utils` (the backend in memory and
the test tools): its own tests always have it, the tests of the other crates enable it. Keep every feature of the framework
(`event-store-postgres`, `projections-postgres`, `projections-mongodb`, `axum`, `operation-utils`, `test-utils`)
compiling and tested on its own together with a store feature, `event-store-postgres` (the default) or `test-utils`:
without either the framework does not compile. The other crates have no features. The framework's own tests get `test-utils`
from its dev-dependency on itself.

Integration tests need the Podman API socket; `DOCKER_HOST` must point to it
(`unix:///run/podman/podman.sock` in this environment). Fix failures, never relax thresholds or
ignore tests.

## Commands

```bash
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo clippy --locked -p cqrsx -p cqrsx-framework --no-default-features --all-targets -- -D warnings
# the library alone: no test-utils
cargo clippy --locked -p cqrsx-framework --no-default-features --features event-store-postgres --lib -- -D warnings
for p in cqrsx cqrsx-macros cqrsx-fixtures; do cargo check --locked -p $p --lib; done
cargo check --locked -p cqrsx --lib --features test-utils
cargo check --locked -p cqrsx-framework --no-default-features --features event-store-postgres --lib
# also: projections-postgres, projections-mongodb, test-utils, test-utils,axum, test-utils,operation-utils
RUSTDOCFLAGS=-Dwarnings cargo doc --locked --workspace --all-features --no-deps
python3 .github/scripts/check-links.py                       # relative links and anchors of the documentation
python3 .github/scripts/check-crate-graph.py                 # the crate graph of doc/crates.md
RUST_TEST_THREADS=2 PROPTEST_RNG_SEED=42 cargo test --locked --workspace --all-features
cargo test --locked -p cqrsx                                  # the core alone
# the framework in memory (without the flag the default feature `event-store-postgres` runs the container tests too)
cargo test --locked -p cqrsx-framework --no-default-features
cargo test --locked -p cqrsx-framework --no-default-features --features event-store-postgres
# also: projections-postgres, projections-mongodb, test-utils, test-utils,axum, test-utils,operation-utils
cargo llvm-cov --locked --workspace --all-features --fail-under-lines 97
cargo llvm-cov --locked -p cqrsx --fail-under-lines 97
cargo llvm-cov --locked -p cqrsx-framework --all-features --fail-under-lines 97
```

The demo is a separate workspace with a CI job of its own; a change that touches it also needs, from `demo/`:

```bash
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo clippy --locked --workspace -- -D warnings   # lib and bins alone: no test-utils
cargo test --locked --workspace   # with the architecture rule and the golden files
git diff --stat -- back-end/tests/golden   # empty
```

Before finishing: fmt, clippy (also without features and the docs), the link check, full tests, all coverage runs and,
for a change that touches the demo, its checks must pass.
