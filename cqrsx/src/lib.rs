//! Reusable CQRS, transactional outbox and durable task execution.
//!
//! - [`cqrs`]: aggregates, commands, event persistence, projections and testing tools.
//! - [`task`]: durable work, handlers, adapters, queues and the optional worker runtime.
//! - `postgres`: PostgreSQL adapters, atomic outbox publication and canonical SQL assets.
//! - `mongodb`: optional MongoDB read side: read-model documents and projection checkpoints.
//! - `host`: optional process signals and coordinated component shutdown.
//! - `logging`: optional explicit logger configuration and installation.
//! - [`Command`], [`Commands`], [`Event`], [`Events`], [`Task`], [`Rejection`]: derive macros (crate
//!   `cqrsx-macros`) for what is known of a command, an aggregate's enum of commands, an event type's
//!   stored name and version, an aggregate's enum of events, a task's wire format and a rejection's
//!   code and params. The contracts they implement live in their modules
//!   ([`cqrs::command::CommandInfo`], [`cqrs::command::decide::Decide`], [`cqrs::event::EventKind`],
//!   [`cqrs::event::DomainEvent`], [`task::format::TaskPayload`], [`cqrs::rejection::Rejection`]).
//!
//! Applications own business transitions and repeat-safe task effects. The README below is the
//! guide: a complete in-memory example (compiled with the doctests), the PostgreSQL assembly, the
//! configuration and the rules.
//!
#![doc = include_str!("../README.md")]

pub mod cqrs;
mod diagnostics;
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

pub use cqrsx_macros::{Command, Commands, Event, Events, Rejection, Task};

/// What the expansions of the derive macros name; not an API.
#[doc(hidden)]
pub mod __private {
    pub use serde_json;
    pub use uuid::Uuid;
}
