//! Reusable CQRS, transactional outbox and durable task execution.
//!
//! - [`cqrs`]: aggregates, commands, event persistence, projections and testing tools.
//! - [`task`]: durable work, handlers, adapters, queues and the optional worker runtime.
//! - `postgres`: PostgreSQL adapters, atomic outbox publication and canonical SQL assets.
//! - `mongodb`: optional MongoDB read side: read-model documents and projection checkpoints.
//! - `host`: optional process signals and coordinated component shutdown.
//! - `logging`: optional explicit logger configuration and installation.
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
