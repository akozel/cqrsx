# cqrsx-macros

Procedural macros for [cqrsx](../cqrsx/README.md). Each derive is re-exported by `cqrsx` next to the trait it
implements (`cqrsx::domain::event::{Event, Events}`, `cqrsx::domain::command::{Command, Decide}`,
`cqrsx::task::Task`, `cqrsx::domain::rejection::Rejection`, `cqrsx::worker::task_handler`), so one `use` brings
the trait and its derive. The macro crate has no feature flags. Examples of the derives and handler together
are in [`cqrsx-engine/tests/engine/modules.rs`](../cqrsx-engine/tests/engine/modules.rs).

## Events

`#[derive(Event)]` and `#[event(name = "...", version = "...")]` give a fact its persisted type and
version. `#[derive(Events)]` delegates the aggregate's event enum to the single event in each variant.

## Commands

`#[derive(Command)]` implements command information on a struct; `#[command_id]` marks at most one UUID
and `#[label]` identifies a named displayable field. On an enum of single-command variants it delegates to
each command's implementation. `#[derive(Decide)]` (an enum only) delegates the decision to each
command's `Decide`; an aggregate's enum of commands derives `Command, Decide`.

## Tasks and labels

`#[derive(Task)]` with `#[task(name = "...", version = 1)]` defines a versioned durable task.
`#[label]` on a named field stores that field as metadata and restores it when the task is decoded.

## Rejections

`#[derive(Rejection)]` on an enum exposes snake-case codes and named fields as parameters; a variant can
override its code with `#[rejection(code = "...")]`.

## Task handlers and dependencies

`#[task_handler]` turns an async function into a registered handler. Its arguments identify the task,
optional context and dependencies supplied by type when the engine is built. The function returns a
`TaskResult`. See the [handler guide](../cqrsx/doc/typical_scenario.md#task-handlers-policies-and-dependencies).

## Validation

Invalid shapes and attributes produce compile errors. The precise requirements and expansion contracts
are documented in [components](../cqrsx/doc/components.md) and the macro source; compile-fail doctests
and unit tests cover invalid inputs.
