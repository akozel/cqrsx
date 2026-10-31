//! The engine: modules assembled in memory and run end to end; over PostgreSQL and MongoDB on containers.
// Test code: a panic is the failed check (rule 8 of .claude/rules/rust-style.md); clippy.toml allows it only inside
// `#[test]` functions, not in the helpers of this crate.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing
)]
#[cfg(all(feature = "postgres", feature = "mongodb"))]
#[path = "engine/database.rs"]
mod database;
#[path = "engine/modules.rs"]
mod modules;
#[cfg(all(feature = "postgres", feature = "mongodb"))]
#[path = "engine/postgres.rs"]
mod postgres;
#[path = "engine/scenario.rs"]
mod scenario;
