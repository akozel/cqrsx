//! Reusable CQRS, transactional outbox and durable task execution.
//!
//! - [`cqrs`]: aggregates, commands, event persistence, projections and testing tools.
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
//!   live in their modules ([`cqrs::command::CommandInfo`], [`cqrs::command::decide::Decide`],
//!   [`cqrs::event::EventKind`], [`cqrs::event::DomainEvent`], [`task::format::TaskPayload`],
//!   [`cqrs::rejection::Rejection`], [`task::handler::TaskHandler`]).
//!
//! Applications own business transitions and repeat-safe task effects. The README below is the
//! guide: a complete in-memory example (compiled with the doctests), the PostgreSQL assembly, the
//! configuration and the rules.
//!
#![doc = include_str!("../README.md")]

pub mod cqrs;
mod diagnostics;
pub mod engine;
mod env;
#[cfg(feature = "host")]
pub mod host;
#[cfg(feature = "logging")]
pub mod logging;
#[cfg(feature = "mongodb")]
pub mod mongodb;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod task;
mod unwinding;

pub use cqrsx_macros::{Command, Commands, Event, Events, Rejection, Task, task_handler};

/// What the expansions of the derive macros name; not an API.
#[doc(hidden)]
pub mod __private {
    pub use serde_json;
    pub use uuid::Uuid;
}
