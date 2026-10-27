//! The flow of the fan's ticket ledgers (`Fan` aggregate, one stream per fan and concert, D-14): the tasks the
//! ledger runs, each with its contract and handler in one file (the ledger's steps [`record_ticket`] and
//! [`record_cancellation`], the e-mails [`send_e_ticket`] and [`issue_refund`]), what follows each of its facts
//! ([`reactions`]), the read model and the wallet assembled from it. Task names are `[a-z0-9_]` only (decision
//! D-05): the library's diagnostics print other names as `unknown`.
pub mod chain;
pub mod issue_refund;
pub mod reactions;
pub mod record_cancellation;
pub mod record_ticket;
pub mod send_e_ticket;
#[cfg(test)]
mod test_support;
pub mod views;
pub mod wallet;
