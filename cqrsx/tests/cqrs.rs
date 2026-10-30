//! Public CQRS composition, histories and downstream testing contracts.
// Test code: a panic is the failed check (rule 8 of .claude/rules/rust-style.md); clippy.toml allows it only inside
// `#[test]` functions, not in the helpers of this crate.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing
)]
#[path = "cqrs/command.rs"]
mod command;
#[path = "cqrs/execution.rs"]
mod execution;
#[path = "cqrs/history.rs"]
mod history;
#[path = "cqrs/planning.rs"]
mod planning;
#[path = "cqrs/projection.rs"]
mod projection;
#[path = "cqrs/reactions.rs"]
mod reactions;
#[path = "cqrs/support.rs"]
mod support;
#[path = "cqrs/testing.rs"]
mod testing;
