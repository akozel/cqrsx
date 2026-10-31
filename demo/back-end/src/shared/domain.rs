//! Shared kernel: value objects used by both aggregates and by the task contracts.
pub mod ids;
pub mod venue;

pub use ids::{ConcertId, FanId, FanLedgerId, InvalidConcertId, InvalidFanId, InvalidFanLedgerId};
