//! Test fixtures shared by the crates of the framework: PostgreSQL and MongoDB containers, leased host ports, captured
//! log lines and tests that run in a process of their own. Not published; a crate takes it as a dev-dependency.
//!
//! It does not depend on `cqrsx` or on the adapters: what is specific to them (the schemas, how a pool is made, the log
//! lines to warm up, the environment variables to unset) the caller passes in. So a crate can use it in its own unit
//! tests without a second copy of its types in the test binary.
//!
//! - [`postgres::Postgres`], [`mongo::Mongo`]: a container of one test, started through `testcontainers` on a host port
//!   from a range of its own (`DOCKER_HOST` must point to the container engine's API socket).
//! - [`logs::capture`]: the log lines of one task.
//! - [`process::isolated`]: one unit test run again in a child process.
//!
//! A panic is the failed check here (rule 8 of `.claude/rules/rust-style.md`): the crate holds test tools only.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test tooling: a panic is the failed check (rule 8 of .claude/rules/rust-style.md)"
)]

pub mod logs;
pub mod mongo;
mod port_lease;
pub mod postgres;
pub mod process;
