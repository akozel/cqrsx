# Crates and their boundaries

The workspace (`Cargo.toml`) has four crates; the demo application `demo/` is a workspace of its own. Every crate is
`publish = false`: the library is unreleased, an application takes it by path or from git, and its API changes freely.

```text
cqrsx-macros ← cqrsx ← cqrsx-framework ← the application
cqrsx-fixtures: a dev-dependency of the crates' own tests; depends on no crate of the workspace
```

An arrow points from a dependency to the crate that uses it. The tests of the application take `cqrsx` with the feature
`test-utils` as a dev-dependency. The stores are modules of the framework behind its features, so a build takes the
database drivers it asks for and no others.

| Crate | Purpose | Knows | Must not know | Public surface |
|---|---|---|---|---|
| [`cqrsx-macros`](../cqrsx-macros/README.md) | the derives `Event`, `Events`, `Command`, `Decide`, `Task`, `Rejection` and the attribute `task_handler` | `syn`, `quote`; the paths of `::cqrsx` its expansions name | anything of `cqrsx` at compile time | no features; 6 derives, 1 attribute |
| [`cqrsx`](../cqrsx/README.md) | the core: aggregates, command execution, reactions, projections and their processor, views, tasks and their runtime, the backend in memory and the test tools of an application | `cqrsx-macros` | a database, a store crate, the framework, the application | one feature, `test-utils` (the backend in memory and the test tools, for the tests of the other crates and of the application; the core's own tests always have it); about 145 public types |
| [`cqrsx-framework`](../cqrsx-framework/README.md) | the assembly of an application from modules over the stores in PostgreSQL and MongoDB (its modules `postgres` and `mongodb`, behind features), its settings, its process | `cqrsx`; `sqlx`, `mongodb` and `axum` by feature | business rules; the logic of the components it assembles | six features: `event-store-postgres` (the default: the journal, the snapshots, the outbox and the task queue in PostgreSQL), `projections-postgres` (the views and their checkpoints in PostgreSQL), `projections-mongodb` (the same in MongoDB), `axum` (the HTTP API over axum), `operation-utils` (the operations that read without a bound or change the read side), `test-utils` (the framework in memory, `InMemory`, and the helpers of a test of it; enables `cqrsx/test-utils`); about 40 public types |
| [`cqrsx-fixtures`](../cqrsx-fixtures/src/lib.rs) | test fixtures of the workspace: database containers on leased ports, captured logs, isolated test processes | `testcontainers`, `sqlx`, `mongodb`, `tracing` | `cqrsx` and the framework | no features; 2 public types, 4 functions |

The counts are of the types (structs, enums, traits, aliases) in `cargo doc --all-features --no-deps`, rounded where
they move often.

## Dependency graph

Every edge between the crates of both workspaces, from the dependency to the crate that takes it; `dev` marks a
dev-dependency and `feature` the features the dependant asks for. `.github/scripts/check-crate-graph.py` (run by CI)
checks the list against `cargo metadata`.

```text
cqrsx -> cqrsx [dev; feature test-utils]
cqrsx -> cqrsx-framework
cqrsx -> encore-backend
cqrsx -> encore-backend [dev; feature test-utils]
cqrsx-fixtures -> cqrsx [dev]
cqrsx-fixtures -> cqrsx-framework [dev]
cqrsx-framework -> cqrsx-framework [dev; no-default; feature test-utils]
cqrsx-framework -> encore-backend [dev; feature projections-mongodb, axum, operation-utils, test-utils]
cqrsx-framework -> encore-backend [feature projections-mongodb, axum, operation-utils]
cqrsx-macros -> cqrsx
```

## Boundary rules

- **The core has no database, and one feature, for tests.** Everything a store does goes through a port of the core
  (`EventRepository`, `CheckpointStore`, `ViewRepository`, `TaskQueue`), and the core proves its rules on its own
  backend in memory. That backend (`memory`) and the test tools (`testing`) are behind the feature `test-utils`: the core's
  own tests always see them, the tests of the other crates and of the application enable it, a working build does not
  contain them. The feature adds nothing to a working build and so multiplies no builds; any other feature in the core
  would.
- **A store knows the core; the core never knows a store.** A store module of the framework implements the ports, and where a driver error
  becomes an error of the core it wraps it in `cqrsx::diagnostics::StorageFailure`: the core's logs carry the category
  and the code of the failure, never the driver's message. So the core stays testable without a database, and a new
  store needs no change in it.
- **The framework assembles and holds no logic of its own.** Executors, processors, the task runtime and their rules stay
  in the core; the framework reads the settings, sets up the modules, checks what they register and opens the stores.
  Logic placed in the framework would be missing for an application that assembles the core by hand. The stores are its
  modules behind features: a store module implements the ports of the core and reaches the builder only through its
  marker.
- **The derive macros name only `::cqrsx`.** An expansion refers to the core's public items and to `cqrsx::__private`
  (what the expansions need, not an API); the core names itself `cqrsx` (`extern crate self as cqrsx`) so its own code
  uses the derives too. A macro crate cannot depend on the crate whose traits it implements.
- **The test tools (`cqrsx::testing`) are written on the public API of the core.** A tool that needs a private part
  points at a gap in that API, which an application would meet as well; the review checks it, the compiler cannot. A
  failed check of a tool is a panic, which the modules of the tools allow for themselves.
- **`cqrsx-fixtures` does not depend on `cqrsx` or on the framework.** What is specific to a crate (its schemas, how its
  pool is made, the log lines to warm up) the caller passes in. So a crate uses the fixtures in its own unit tests
  without a second copy of its types in the test binary.
- **No facades.** A type is imported from the module that defines it; there are no flattening re-exports, no
  compatibility namespaces and no `pub use` of another crate's API, with one exception: each derive macro is
  re-exported by the core next to the trait it implements (`cqrsx::domain::event::{Event, Events}`), so one `use`
  brings both.
- **A crate's documentation is one text.** `cqrsx`, `cqrsx-framework` and `cqrsx-macros` keep a `README.md`, which
  `lib.rs` includes as the crate's documentation; `cqrsx-fixtures` has a single `//!` in `lib.rs`. The rules of an
  application are in the [README of the core](../cqrsx/README.md), the components and the names in
  [Components and terminology](components.md).
