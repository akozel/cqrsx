//! Reusable CQRS, transactional outbox and durable task execution.
//!
//! - [`cqrs`]: aggregates, commands, event persistence, projections and testing tools.
//! - [`task`]: durable work, handlers, adapters, queues and the optional worker runtime.
//! - `postgres`: PostgreSQL adapters, atomic outbox publication and canonical SQL assets.
//! - `host`: optional process signals and coordinated component shutdown.
//! - `logging`: optional explicit logger configuration and installation.
//!
//! Applications own business transitions and repeat-safe task effects.

pub mod cqrs;
mod diagnostics;
mod env;
#[cfg(feature = "host")]
pub mod host;
#[cfg(feature = "logging")]
pub mod logging;
#[cfg(feature = "postgres")]
pub mod postgres;
pub mod task;
