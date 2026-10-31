//! Public task adaptation, execution and worker-runtime contracts.
// Test code: a panic is the failed check (rule 8 of .claude/rules/rust-style.md); clippy.toml allows it only inside
// `#[test]` functions, not in the helpers of this crate.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing
)]
#[path = "worker/adapter.rs"]
mod adapter;
#[path = "worker/executor.rs"]
mod executor;
#[path = "worker/runtime.rs"]
mod runtime;
