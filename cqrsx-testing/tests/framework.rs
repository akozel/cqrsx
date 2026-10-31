//! The downstream tools on an example application: `TestFramework` given/when/then on two aggregates.
// Test code: a panic is the failed check (rule 8 of .claude/rules/rust-style.md); clippy.toml allows it only inside
// `#[test]` functions, not in the helpers of this crate.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::indexing_slicing
)]
#[path = "framework/scenarios.rs"]
mod scenarios;
#[path = "framework/support.rs"]
mod support;
