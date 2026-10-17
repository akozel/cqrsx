//! Encore back-end library in four layers:
//!
//! - [`domain`]: aggregates, their commands, events, rules and value objects;
//! - [`application`]: use cases, the workflows driven by outbox tasks, read models and operations;
//! - [`presentation`]: the HTTP API;
//! - [`infrastructure`]: PostgreSQL, MongoDB, the simulated mail service and the task runtime wiring.
//!
//! [`bootstrap`] is the composition root shared by the binary (`main.rs`) and the tests.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
// `ApiError` is an HTTP response payload built only on error paths; its size is irrelevant.
#![allow(clippy::result_large_err)]
pub mod application;
pub mod bootstrap;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod presentation;
#[cfg(test)]
pub mod testing;
