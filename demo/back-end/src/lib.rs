//! Encore back-end library: one module per part of the box office, each a directory with its clean-architecture
//! layers inside (`domain`, `application`, `presentation`) and its root file next to it:
//!
//! - [`concert`]: the box office, the `Concert` aggregate: its commands, what follows each of its facts (the
//!   tasks of a fan's ledger), the concert halls and the time machine;
//! - [`fan`]: a fan's ticket ledger, the `Fan` aggregate: the tasks it runs (the ledger's steps, the e-mails), the
//!   fan wallets;
//! - [`operations`]: no aggregate: health, settings, the activity log and the operations over every read model
//!   (Encore!, status, verify);
//! - [`shared`]: what several modules need: the ids and the venue, the ports and the metadata of a command, the HTTP
//!   error shape, PostgreSQL, MongoDB and the HTTP client of the external mail service.
//!
//! [`process`] starts and stops the binary: logging, configuration, connections, the HTTP server and
//! the task runtime. [`bootstrap`] is the composition root shared by the binary and the tests: one
//! registry per kind of component (aggregates, projections, tasks, routes).
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
// `ApiError` is an HTTP response payload built only on error paths; its size is irrelevant.
#![allow(clippy::result_large_err)]
pub mod bootstrap;
pub mod concert;
pub mod config;
pub mod fan;
pub mod operations;
pub mod process;
pub mod shared;
#[cfg(test)]
pub mod testing;
