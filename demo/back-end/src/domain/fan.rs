//! `Fan` aggregate: one fan = one event stream, the fan's personal ticket ledger. It is a
//! downstream aggregate: commands arrive only from outbox tasks (never from the UI), so its rules
//! R9–R11 are technical — they make repeated (at-least-once) and reordered task delivery safe.
pub mod aggregate;
pub mod commands;
pub mod error;
pub mod events;
#[cfg(test)]
mod test_support;

pub use aggregate::{Fan, Ticket, TicketState};
pub use commands::FanCommand;
pub use error::FanError;
pub use events::FanEvent;
