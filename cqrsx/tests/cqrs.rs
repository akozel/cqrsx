//! Public CQRS composition, histories and downstream testing contracts.
#[path = "cqrs/command.rs"]
mod command;
#[path = "cqrs/execution.rs"]
mod execution;
#[path = "cqrs/history.rs"]
mod history;
#[path = "cqrs/support.rs"]
mod support;
#[path = "cqrs/testing.rs"]
mod testing;
