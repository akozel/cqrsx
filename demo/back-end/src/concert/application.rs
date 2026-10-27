//! The flow of the box office (`Concert`): what follows each of its facts ([`reactions`]: the tasks of a fan's ledger,
//! built from the facts by [`fan_ledger`]), the concert halls and the time machine. Its commands need no use case of
//! their own: the HTTP handler hands them to the engine.
pub mod fan_ledger;
pub mod reactions;
#[cfg(test)]
mod test_support;
pub mod time_travel;
pub mod views;
