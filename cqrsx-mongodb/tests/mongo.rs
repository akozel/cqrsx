//! The MongoDB read side against a real server: the view repository, the checkpoint store and the
//! projection processor whose views and checkpoint live in MongoDB. Every test starts its own container.
// Test code: a panic is the failed check (rule 8 of .claude/rules/rust-style.md); clippy.toml allows it only inside
// `#[test]` functions, not in the helpers of this crate.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing
)]

#[path = "mongo/checkpoints.rs"]
mod checkpoints;
#[path = "mongo/journal_in_postgres.rs"]
mod journal_in_postgres;
#[path = "mongo/processor.rs"]
mod processor;
#[path = "mongo/support.rs"]
mod support;
#[path = "mongo/view_repository.rs"]
mod view_repository;
