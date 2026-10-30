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
#[path = "engine/modules.rs"]
mod modules;
#[cfg(all(feature = "postgres", feature = "mongodb"))]
#[path = "support/mongo.rs"]
mod mongo_container;
#[cfg(all(feature = "postgres", feature = "mongodb"))]
#[path = "support/port_lease.rs"]
mod port_lease;
#[cfg(all(feature = "postgres", feature = "mongodb"))]
#[path = "engine/postgres.rs"]
mod postgres;
#[cfg(all(feature = "postgres", feature = "mongodb"))]
#[path = "support/postgres.rs"]
mod postgres_container;
#[path = "engine/scenario.rs"]
mod scenario;
