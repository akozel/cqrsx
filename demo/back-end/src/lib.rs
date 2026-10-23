//! Encore back-end library in four layers:
//!
//! - [`domain`]: aggregates, their commands, events, rules and value objects;
//! - [`application`]: use cases, the workflows driven by outbox tasks, read models and operations;
//! - [`presentation`]: the HTTP API;
//! - [`infrastructure`]: PostgreSQL, MongoDB, the HTTP client of the external mail service and the
//!   task runtime wiring.
//!
//! [`process`] starts and stops the binary: logging, configuration, connections, the HTTP server and
//! the task runtime. [`bootstrap`] is the composition root shared by the binary and the tests: one
//! registry per kind of component (aggregates, projections, tasks, routes).
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
// `ApiError` is an HTTP response payload built only on error paths; its size is irrelevant.
#![allow(clippy::result_large_err)]
pub mod application;
pub mod bootstrap;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod presentation;
pub mod process;
#[cfg(test)]
pub mod testing;
