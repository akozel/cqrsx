//! Encore back-end library: domain, application services, projections, outbox handlers and HTTP.
//! The binary (`main.rs`) is the composition root; integration tests use the library directly.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
// `ApiError` is an HTTP response payload built only on error paths; its size is irrelevant.
#![allow(clippy::result_large_err)]
pub mod app;
pub mod config;
pub mod domain;
pub mod http;
pub mod outbox;
pub mod projections;
#[cfg(test)]
pub mod testing;
