# cqrsx-macros

Procedural macros for [cqrsx](../cqrsx/README.md), re-exported by `cqrsx`; applications normally import
them from that crate. The macro crate has no feature flags. Examples of the derives and handler together
are in [`cqrsx/tests/engine/modules.rs`](../cqrsx/tests/engine/modules.rs).

## Events

`#[derive(Event)]` and `#[event(name = "...", version = "...")]` give a fact its persisted type and
version. `#[derive(Events)]` delegates the aggregate's event enum to the single event in each variant.

## Commands

`#[derive(Command)]` implements command information on a struct; `#[command_id]` marks at most one UUID
and `#[label]` identifies a named displayable field. `#[derive(Commands)]` delegates an enum of
single-command variants to each command's `Decide` and information implementations.

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
