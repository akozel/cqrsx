//! Integration tests on the backend in memory: commands, the history of the journal, task planning and reactions,
//! projections, and the downstream testing tools.
// Test code: a panic is the failed check (rule 8 of .claude/rules/rust-style.md); clippy.toml allows it only inside
// `#[test]` functions, not in the helpers of this crate.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing
)]
#[path = "in_memory/command.rs"]
mod command;
#[path = "in_memory/execution.rs"]
mod execution;
#[path = "in_memory/history.rs"]
mod history;
#[path = "in_memory/planning.rs"]
mod planning;
#[path = "in_memory/projection.rs"]
mod projection;
#[path = "in_memory/reactions.rs"]
mod reactions;
#[path = "in_memory/support.rs"]
mod support;
#[path = "in_memory/testing.rs"]
mod testing;
