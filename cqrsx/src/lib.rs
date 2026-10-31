//! Reusable CQRS, transactional outbox and durable task execution.
//!
//! - [`domain`]: what the domain writes: aggregates, events, how a command decides, rejections.
//! - [`journal`]: the event store, its repositories, snapshots, upcasters, the feed and the commit wake-up.
//! - [`command`]: executing a command: executors, aggregate locks, conflict retry, causation, observers and task planning.
//! - [`reactions`]: the tasks that follow each event of an aggregate.
//! - [`projection`]: the event handler, the checkpoint of a projection and the processor that feeds handlers from the journal.
//! - [`view`]: read models, their projection, repository and verification.
//! - [`memory`]: the backend in memory: journal, checkpoints, views and a task queue.
//! - [`testing`]: tools for testing aggregates and commands downstream.
//! - [`task`]: a task as data: what a command publishes, a delivery attempt and the policies of the task types.
//! - [`worker`]: running tasks: handlers, dispatcher, adapters, queue port, executor and the worker runtime.
//! - [`engine`]: an application of modules assembled from these components: commands, views and tasks; in memory or
//!   over PostgreSQL and MongoDB, its settings read from the environment, served with its HTTP routes (`http`).
//! - `postgres`: PostgreSQL adapters, atomic outbox publication and canonical SQL assets.
//! - `mongodb`: optional MongoDB read side: read-model documents and projection checkpoints.
//! - `host`: optional process signals and coordinated component shutdown.
//! - `http`: optional HTTP routes of the modules and `serve` of an engine (axum 0.8).
//! - `logging`: optional explicit logger configuration and installation.
//! - Derive macros (crate `cqrsx-macros`) live next to the trait each implements: `Event` and `Events`
//!   ([`domain::event`]: an event type's stored name and version, an aggregate's enum of events), `Command` and
//!   `Decide` ([`domain::command`]: what is known of a command, how an enum of commands decides), `Rejection`
//!   ([`domain::rejection`]: a rejection's code and params), `Task` ([`task`]: a task's wire format) and the
//!   attribute `task_handler` ([`worker`]: a task handler written as a function).
//!
//! Applications own business transitions and repeat-safe task effects. The README below is the
//! guide: a complete in-memory example (compiled with the doctests), the PostgreSQL assembly, the
//! configuration and the rules.
//!
#![doc = include_str!("../README.md")]

extern crate self as cqrsx;

pub mod command;
mod diagnostics;
pub mod domain;
pub mod engine;
mod env;
#[cfg(feature = "host")]
pub mod host;
pub mod journal;
mod locking;
#[cfg(feature = "logging")]
pub mod logging;
pub mod memory;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod projection;
pub mod reactions;
pub mod task;
pub mod testing;
mod unwinding;
pub mod view;
pub mod worker;

/// What the expansions of the derive macros name; not an API.
#[doc(hidden)]
pub mod __private {
    pub use serde_json;
    pub use uuid::Uuid;
}
