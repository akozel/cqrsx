//! Reusable CQRS, transactional outbox and durable task execution.
//!
//! - [`domain`]: what the domain writes: aggregates, events, how a command decides, rejections.
//! - [`journal`]: the event store, its repositories, snapshots, upcasters, the feed and the commit wake-up.
//! - [`command`]: executing a command: executors, aggregate locks, conflict retry, causation, observers and task planning.
//! - [`reactions`]: the tasks that follow each event of an aggregate.
//! - [`projection`]: the event handler, the checkpoint of a projection and the processor that feeds handlers from the journal.
//! - [`view`]: read models, their projection, repository and verification.
//! - [`memory`]: the backend in memory: journal, checkpoints, views and a task queue.
//! - [`task`]: a task as data: what a command publishes, a delivery attempt and the policies of the task types.
//! - [`worker`]: running tasks: handlers, dispatcher, adapters, queue port, executor and the worker runtime.
//! - [`mutex`]: the one helper that takes a `std::sync::Mutex` (shared with `cqrsx-engine`).
//! - [`diagnostics`]: what the crates of the framework share about log lines: a safe name as a log field and the
//!   category and code of a failed storage operation.
//! - Derive macros (crate `cqrsx-macros`) live next to the trait each implements: `Event` and `Events`
//!   ([`domain::event`]: an event type's stored name and version, an aggregate's enum of events), `Command` and
//!   `Decide` ([`domain::command`]: what is known of a command, how an enum of commands decides), `Rejection`
//!   ([`domain::rejection`]: a rejection's code and parameters), `Task` ([`task`]: a task's wire format) and the
//!   attribute `task_handler` ([`worker`]: a task handler written as a function).
//!
//! Applications own business transitions and repeat-safe task effects. The application is assembled from these
//! components by the engine, the crate `cqrsx-engine`. The stores live in crates of their own, which the core does not
//! know: `cqrsx-postgres` (journal, outbox, task queue, checkpoints, views) and `cqrsx-mongodb` (views and checkpoints).
//! The README below describes the core and the rules.
//!
#![doc = include_str!("../README.md")]

extern crate self as cqrsx;

pub mod command;
pub mod diagnostics;
pub mod domain;
mod env;
pub mod journal;
pub mod memory;
pub mod mutex;
pub mod projection;
pub mod reactions;
pub mod task;
mod unwinding;
pub mod view;
pub mod worker;

/// What the expansions of the derive macros name; not an API.
#[doc(hidden)]
pub mod __private {
    pub use serde_json;
    pub use uuid::Uuid;
}
