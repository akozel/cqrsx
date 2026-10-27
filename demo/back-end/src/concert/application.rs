//! The flow of the box office (`Concert`): one file per use case, what follows each of its facts
//! ([`reactions`]: the tasks of a fan's ledger, built from the facts by [`fan_ledger`]), the concert halls and the
//! time machine.
pub mod announce_concert;
pub mod app;
pub mod book_seat;
pub mod cancel_booking;
pub mod fan_ledger;
pub mod reactions;
#[cfg(test)]
mod test_support;
pub mod time_travel;
pub mod views;
