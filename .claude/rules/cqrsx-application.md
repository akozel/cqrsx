---
paths:
  - "demo/**"
---

# Writing an application on cqrsx

How application code uses cqrsx: its modules, aggregates, tasks, command ids and entry points. The demo follows these
rules; a rule marked *checked* is enforced by the named test of `demo/back-end/tests/architecture.rs`. A decision about
how an application uses cqrsx is written here as a rule in the same change, and a changed decision edits its rule. Rust
code style is `.claude/rules/rust-style.md`.

## Modules and layers

1. A module is a directory with its layers (`domain`, `application`, `presentation`, `infrastructure` where needed)
   and a root file next to it (`concert.rs`). The root registers what the module plugs into the framework, one line each:
   `Module::new("fan").aggregate::<Fan>(rules).provide(..).task(handler, policy).view::<V, A>().routes(..)`.
2. Inside a module a layer sees the layers below it and the same layers of `shared`; one module reaches another only
   through `crate::` and only where the rules allow. *Checked:* `every_layer_imports_only_what_it_may`.
3. The tree reads as the business model: one file per command, event, task handler, view and endpoint. A file is named
   by its role, never by what its path already says (`concert/domain/aggregate.rs`, not `concert/domain/concert.rs`).

## Domain

4. An aggregate has no command handler. `impl Aggregate` names its commands, events, rejection and rules
   (`type Command = ConcertCommand;`), and `apply` is its state machine: the command decides, the aggregate remembers.
5. Every command decides itself, in its own file: `impl Decide<Concert> for BookSeat`. The rules a decision depends on
   are `type Rules`, given at registration (`aggregate::<Concert>(ConcertRules { .. })`) and never read from history.
6. A command carries business data only; its id is not one of its fields (rule 9).

## Application

7. A task's policy is a value given where the task is registered: `.task(record_ticket, policies.step)`. The module
   builds its policies from the setup (`fan/application/policies.rs`); a task type registered without one gets
   `TaskPolicy::default()`.
8. An application function returns data: domain or framework types, or a type of its own where it computes something
   (a sum, the worst state). It never builds the wire shape of a transport, such as the `json!` of an HTTP answer.

## Command ids

9. The id of a command that starts a chain is the argument of `cqrs.source("ui").execute_with_id(command_id, &id, command)`,
   chosen by an entry point that answers with it; `execute` generates one. A fact identified by its command gets the id
   as an explicit line where the command is built (`booking_id: command_id`). The id keys the tracing of the chain and
   the publication of its tasks (the same id twice on one aggregate publishes them once); it is not an idempotency key.

## Presentation

10. A presentation is laid out by transport, then by kind of framework entry, then by action:
    `<module>/presentation/<transport>/<kind>/<action>.rs`, with one URL table per module in `presentation/http.rs`
    (a section per kind, a line per endpoint). The kinds are the four entries of `Cqrs`:

    | Kind | What it does | Framework entry |
    |---|---|---|
    | `commands` | changes the state | `cqrs.source(..).execute::<A>(..)`, `execute_with_id` |
    | `queries` | reads a projection | `cqrs.view::<V>()` with `get`, `list`, `range` |
    | `history` | replays the journal | `cqrs.history::<A>(..)` (`operation-utils`) |
    | `maintenance` | serves the read side | `cqrs.views()`, `cqrs.view::<V>().rebuild(..)` |

    `history` reads a stream without a bound and `maintenance` changes the read side or counts without a bound, so their
    calls (`history`, `views().{request_rebuild, request_rebuild_key, verify, count_after, totals}`,
    `view::<V>().rebuild(..)`) exist only with the framework's feature `operation-utils`, which the application enables in
    its `Cargo.toml`; `views().{keys, status, lag}` need no feature. The criterion is the framework's API rule in
    `doc/components.md`.

    An endpoint that uses none of the four (health, settings, an event stream) lies directly in `presentation/http/`.
    Another transport (a queue, a scheduler) is a sibling directory of `http`. *Checked:*
    `a_presentation_file_names_only_the_framework_entry_of_its_kind`.
11. The input and the answer of an endpoint are types named `<Subject><Action>Request` and `<Subject><Action>Response`:
    the subject is a noun of the domain (the aggregate or the view the endpoint serves), the action is added only when
    the subject has several endpoints: `ConcertCommandRequest`, `ConcertHallResponse`, `FanWalletResponse`,
    `ProjectionRebuildRequest`. A request type exists for a body or a query string; an id in the path stays
    `Path<String>`, parsed by `parse_*_id`. Every endpoint answers with its response type: a `#[derive(Serialize)]`
    body, or, where the outcome picks the status, a type with `impl IntoResponse` over a private body; errors go
    through `ApiError`. A part of a body is a private type named after the field it fills (`last_result` → `LastResult`,
    `zones` → `Zone`); the domain or application type of the same name keeps its module there (`rebuild::Running`,
    `venue::Zone`), and a word an axum import takes gets the subject (`ConcertState` next to the extractor `State`). No
    presentation code holds `json!` except the one error shape (`shared/presentation/http/error.rs`).
12. A conversion lives with its type, not in the handler: a request becomes a command by
    `into_command(self, command_id) -> Result<_, ApiError>`, which also normalises the input (trim, non-empty); a
    response is built `From` the data of the application; an outcome becomes a status by `impl IntoResponse`. Business
    rules stay in the aggregate. An endpoint with a conversion or a check is a directory, `<action>.rs` (the handler)
    with `<action>/{request,response}.rs`; otherwise its types lie in the endpoint's file. A handler parses, calls and
    answers.

## Names

13. A new name or a rename that the decision being implemented does not give is agreed before it is written: an
    application type, a part of a body or a function keeps the word the wire, the domain or the existing code already
    uses for the same thing.

## Tests

14. An application names its stores when it builds the framework: `event_store(Postgres)` and, with views,
    `projections(Postgres)` or `projections(MongoDb)`; without them `build()` refuses (`BuildError::NoJournal`,
    `BuildError::ViewsWithoutStore`). Its tests build the framework in memory with `event_store(InMemory)` and
    `projections(InMemory)` and reach its helpers (`run_tasks`, `catch_up`, and the stores themselves: `journal`,
    `outbox`, `checkpoints`, `documents`) through the framework's feature `test-utils`, which the application enables only
    in the `[dev-dependencies]` of the crate that tests (`cqrsx-framework = { workspace = true, features = ["test-utils"] }`).
    `InMemory` and the helpers exist only with that feature, so they lie only in test code (a `#[cfg(test)]` module or
    `tests/*`) and a production build cannot start in memory. The criterion is the framework's API rule in
    `doc/components.md`.
