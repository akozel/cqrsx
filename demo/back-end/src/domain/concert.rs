//! `Concert` aggregate: one concert = one event stream; the source of truth for seats and for
//! every business rule R1–R8. Rules are checked synchronously, which is what makes a rejected
//! click visible as "Δ events = 0".
pub mod aggregate;
pub mod commands;
pub mod error;
pub mod events;
#[cfg(test)]
mod test_support;

pub use aggregate::{Booking, Concert, ConcertInfo, ConcertServices};
pub use commands::ConcertCommand;
pub use error::ConcertError;
pub use events::ConcertEvent;
