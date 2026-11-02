# Crates and their boundaries

The workspace (`Cargo.toml`) has six crates; the demo application `demo/` is a workspace of its own. Every crate is
`publish = false`: the library is unreleased, an application takes it by path or from git, and its API changes freely.

```text
cqrsx-macros ← cqrsx ← { cqrsx-postgres, cqrsx-mongodb } ← cqrsx-engine ← the application
cqrsx-fixtures: a dev-dependency of the crates' own tests; depends on no crate of the workspace
```

An arrow points from a dependency to the crate that uses it. The tests of the application take `cqrsx` with the feature
`test-utils` as a dev-dependency. `cqrsx-mongodb` takes `cqrsx-postgres` only as a
dev-dependency (its test of a journal in PostgreSQL); the engine takes the stores only with its features.

| Crate | Purpose | Knows | Must not know | Public surface |
|---|---|---|---|---|
| [`cqrsx-macros`](../cqrsx-macros/README.md) | the derives `Event`, `Events`, `Command`, `Decide`, `Task`, `Rejection` and the attribute `task_handler` | `syn`, `quote`; the paths of `::cqrsx` its expansions name | anything of `cqrsx` at compile time | no features; 6 derives, 1 attribute |
| [`cqrsx`](../cqrsx/README.md) | the core: aggregates, command execution, reactions, projections and their processor, views, tasks and their runtime, the backend in memory and the test tools of an application | `cqrsx-macros` | a database, a store crate, the engine, the application | one feature, `test-utils` (the backend in memory and the test tools, for the tests of the other crates and of the application; the core's own tests always have it); about 145 public types |
| [`cqrsx-postgres`](../cqrsx-postgres/src/lib.rs) | the PostgreSQL stores: journal, outbox, task queue, checkpoints, views, pools; the canonical SQL in `sql/` | `cqrsx` | the engine, MongoDB | no features; 8 public types, the schema constants |
| [`cqrsx-mongodb`](../cqrsx-mongodb/src/lib.rs) | the MongoDB read side: views and the checkpoints of their processors | `cqrsx` | the engine, PostgreSQL | no features; 2 public types |
| [`cqrsx-engine`](../cqrsx-engine/README.md) | the assembly of an application from modules over the stores, its settings, its HTTP process | `cqrsx`; the stores by feature | business rules; the logic of the components it assembles | features `postgres` (the default), `mongodb` (includes `postgres`), `http`, `operation-utils` (the operations that read without a bound or change the read side), `test-utils` (the engine in memory, `InMemory`, and the helpers of a test of it; enables `cqrsx/test-utils`); about 30 public types |
| [`cqrsx-fixtures`](../cqrsx-fixtures/src/lib.rs) | test fixtures of the workspace: database containers on leased ports, captured logs, isolated test processes | `testcontainers`, `sqlx`, `mongodb`, `tracing` | `cqrsx` and the stores | no features; 2 public types, 4 functions |

The counts are of the types (structs, enums, traits, aliases) in `cargo doc --all-features --no-deps`, rounded where
they move often.

## Boundary rules

- **The core has no database, and one feature, for tests.** Everything a store does goes through a port of the core
  (`EventRepository`, `CheckpointStore`, `ViewRepository`, `TaskQueue`), and the core proves its rules on its own
  backend in memory. That backend (`memory`) and the test tools (`testing`) are behind the feature `test-utils`: the core's
  own tests always see them, the tests of the other crates and of the application enable it, a working build does not
  contain them. The feature adds nothing to a working build and so multiplies no builds; any other feature in the core
  would.
- **A store knows the core; the core never knows a store.** A store implements the ports, and where a driver error
  becomes an error of the core it wraps it in `cqrsx::diagnostics::StorageFailure`: the core's logs carry the category
  and the code of the failure, never the driver's message. So the core stays testable without a database, and a new
  store needs no change in it.
- **The engine assembles and holds no logic of its own.** Executors, processors, the task runtime and their rules stay
  in the core; the engine reads the settings, sets up the modules, checks what they register and opens the stores.
  Logic placed in the engine would be missing for an application that assembles the core by hand.
- **The derive macros name only `::cqrsx`.** An expansion refers to the core's public items and to `cqrsx::__private`
  (what the expansions need, not an API); the core names itself `cqrsx` (`extern crate self as cqrsx`) so its own code
  uses the derives too. A macro crate cannot depend on the crate whose traits it implements.
- **The test tools (`cqrsx::testing`) are written on the public API of the core.** A tool that needs a private part
  points at a gap in that API, which an application would meet as well; the review checks it, the compiler cannot. A
  failed check of a tool is a panic, which the modules of the tools allow for themselves.
- **`cqrsx-fixtures` does not depend on `cqrsx` or on the stores.** What is specific to a crate (its schemas, how its
  pool is made, the log lines to warm up) the caller passes in. So a crate uses the fixtures in its own unit tests
  without a second copy of its types in the test binary.
- **No facades.** A type is imported from the module that defines it; there are no flattening re-exports, no
  compatibility namespaces and no `pub use` of another crate's API, with one exception: each derive macro is
  re-exported by the core next to the trait it implements (`cqrsx::domain::event::{Event, Events}`), so one `use`
  brings both.
- **A crate's documentation is one text.** `cqrsx`, `cqrsx-engine` and `cqrsx-macros` keep a `README.md`, which
  `lib.rs` includes as the crate's documentation; the other crates have a single `//!` in `lib.rs`. The rules of an
  application are in the [README of the core](../cqrsx/README.md), the components and the names in
  [Components and terminology](components.md).
