#![doc = include_str!("../README.md")]

extern crate self as cqrsx;

pub mod command;
pub mod diagnostics;
pub mod domain;
mod env;
pub mod journal;
// The backend in memory and the test tools of an application, for tests: the crate's own tests always see them, the
// tests of the other crates take the feature `test-utils`; a working build has no use for them.
#[cfg(any(test, feature = "test-utils"))]
pub mod memory;
pub mod mutex;
pub mod projection;
pub mod reactions;
pub mod task;
// The test tools of an application, under the same condition as `memory`.
#[cfg(any(test, feature = "test-utils"))]
pub mod testing;
mod unwinding;
pub mod view;
pub mod worker;

/// What the expansions of the derive macros name; not an API.
#[doc(hidden)]
pub mod __private {
    pub use serde_json;
}
