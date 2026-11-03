//! The framework: modules assembled in memory and run end to end; over PostgreSQL and MongoDB on containers.
// Test code: a panic is the failed check (rule 8 of .claude/rules/rust-style.md); clippy.toml allows it only inside
// `#[test]` functions, not in the helpers of this crate.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing
)]
#[cfg(all(feature = "projections-postgres", feature = "projections-mongodb"))]
#[path = "framework/database.rs"]
mod database;
#[path = "framework/modules.rs"]
mod modules;
#[cfg(all(feature = "projections-postgres", feature = "projections-mongodb"))]
#[path = "framework/postgres.rs"]
mod postgres;
#[path = "framework/scenario.rs"]
mod scenario;
