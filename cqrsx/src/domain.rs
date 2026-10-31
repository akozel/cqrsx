//! What the domain writes and the framework reads: an aggregate, its events, how a command decides, and the rejection
//! that reports a business refusal.
pub mod aggregate;
pub mod command;
pub mod event;
pub mod rejection;
