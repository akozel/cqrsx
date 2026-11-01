#![doc = include_str!("../README.md")]

extern crate self as cqrsx;

pub mod command;
pub mod diagnostics;
pub mod domain;
mod env;
pub mod journal;
pub mod memory;
pub mod mutex;
pub mod projection;
pub mod reactions;
pub mod task;
mod unwinding;
pub mod view;
pub mod worker;

/// What the expansions of the derive macros name; not an API.
#[doc(hidden)]
pub mod __private {
    pub use serde_json;
    pub use uuid::Uuid;
}
