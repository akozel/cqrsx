//! Operations over the whole ledger and every read model: Encore! and the status of the read side, verify, and
//! what every read model shares now that the projection processors feed them from the journal (epic 03/04): the
//! processors' reports as activity log entries, and verify against the history.
pub mod activity_observer;
pub mod ledger;
pub mod rebuild;
pub mod verification;
pub mod verify;
