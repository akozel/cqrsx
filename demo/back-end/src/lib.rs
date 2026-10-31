//! Encore back-end library: one module per part of the box office, each a directory with its clean-architecture layers
//! inside (`domain`, `application`, `presentation`) and its root file next to it, which registers what the module plugs
//! into the cqrsx engine:
//!
//! - [`concert`]: the box office, the `Concert` aggregate: its commands, what follows each of its facts (the tasks of a
//!   fan's ledger), the concert halls and the time machine;
//! - [`fan`]: a fan's ticket ledger, the `Fan` aggregate: the tasks it runs (the ledger's steps, the e-mails), the fan
//!   wallets;
//! - [`operations`]: no aggregate: health, settings, the activity log and the operations over every read model
//!   (Encore!, status, verify);
//! - [`shared`]: what several modules need: the ids and the venue, the ports and the names of the task policies, the
//!   HTTP error shape, the HTTP client of the external mail service.
//!
//! The binary (`main.rs`) lists the modules for the engine with [`DEMO_DEFAULTS`]; the engine does the rest: settings,
//! connections, executors, processors, task runtime, HTTP server, start and stop.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
// `ApiError` is an HTTP response payload built only on error paths; its size is irrelevant.
#![allow(clippy::result_large_err)]
#[cfg(test)]
mod composition;
pub mod concert;
pub mod fan;
pub mod operations;
pub mod shared;
#[cfg(test)]
pub mod testing;

use cqrsx_engine::settings::Defaults;

/// The demo's values of the engine's settings where they differ from the library's, used only when the variable is
/// absent (`demo/.env.example` shows each), and the name of its connections (PostgreSQL's `application_name`, MongoDB's
/// `appName`) and log lines.
///
/// - The databases of `demo/compose.yaml`; the owner name the checkpoints of this process carry.
/// - `PROJECTION_SETTLE_MS`: the pause between a commit's wake-up and the processor's read; the library reads at once.
///   Measured in epic 03/04: on a hot key at 700 commands a second it brings the read side's cost per command back to
///   about the cost of the handler inside the command (PostgreSQL +11 % instead of +42 %, MongoDB and the back-end below
///   it), and the read models trail the journal by about ten events. 50 ms saves no more PostgreSQL.
/// - `MONGODB_POOL_SIZE`: the driver's own default. Measured in epic 04/06 with two projection processors writing up to
///   16 documents each at once: 32 or 64 connections take no more announces or bookings a second than 10, and cost
///   MongoDB 11–17 % more CPU per command.
/// - The task runtime (`OUTBOX_*`): two workers and a buffer of two (Q = C), a 300 ms poll, a handler timeout of 5 s, a
///   lease of 30 s, a storage budget of 3 s; the lease exceeds 2·S + H + 5 s + ⌈Q/C⌉·(H + S) = 24 s.
pub const DEMO_DEFAULTS: Defaults = Defaults::new(
    "encore-backend",
    &[
        ("DATABASE_URL", "postgresql://cqrsx:cqrsx@localhost:5434/encore"),
        ("MONGODB_URI", "mongodb://localhost:27018"),
        ("READ_DB", "encore_read"),
        ("NODE_NAME", "encore-backend"),
        ("PROJECTION_SETTLE_MS", "10"),
        ("MONGODB_POOL_SIZE", "10"),
        ("OUTBOX_WORKER_CONCURRENCY", "2"),
        ("OUTBOX_QUEUE_CAPACITY", "2"),
        ("OUTBOX_POLL_INTERVAL_MS", "300"),
        ("OUTBOX_HANDLER_TIMEOUT_SECS", "5"),
        ("OUTBOX_LEASE_SECS", "30"),
        ("OUTBOX_STORAGE_TIMEOUT_MS", "3000"),
    ],
);
