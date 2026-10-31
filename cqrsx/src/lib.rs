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
//! - [`task`]: durable work, handlers, adapters, queues and the optional worker runtime.
//! - [`engine`]: an application of modules assembled from these components: commands, views and tasks; in memory or
//!   over PostgreSQL and MongoDB, its settings read from the environment, served with its HTTP routes (`http`).
//! - `postgres`: PostgreSQL adapters, atomic outbox publication and canonical SQL assets.
//! - `mongodb`: optional MongoDB read side: read-model documents and projection checkpoints.
//! - `host`: optional process signals and coordinated component shutdown.
//! - `http`: optional HTTP routes of the modules and `serve` of an engine (axum 0.8).
//! - `logging`: optional explicit logger configuration and installation.
//! - [`Command`], [`Commands`], [`Event`], [`Events`], [`Task`], [`Rejection`]: derive macros (crate
//!   `cqrsx-macros`) for what is known of a command, an aggregate's enum of commands, an event type's
//!   stored name and version, an aggregate's enum of events, a task's wire format and a rejection's
//!   code and params; [`task_handler`]: a task handler written as a function. The contracts they implement
//!   live in their modules ([`domain::command::CommandInfo`], [`domain::command::Decide`],
//!   [`domain::event::EventKind`], [`domain::event::DomainEvent`], [`task::format::TaskPayload`],
//!   [`domain::rejection::Rejection`], [`task::handler::TaskHandler`]).
//!
//! Applications own business transitions and repeat-safe task effects. The README below is the
//! guide: a complete in-memory example (compiled with the doctests), the PostgreSQL assembly, the
//! configuration and the rules.
//!
#![doc = include_str!("../README.md")]

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

pub use cqrsx_macros::{Command, Commands, Event, Events, Rejection, Task, task_handler};

/// What the expansions of the derive macros name; not an API.
#[doc(hidden)]
pub mod __private {
    pub use serde_json;
    pub use uuid::Uuid;
}
