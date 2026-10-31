//! Operations over the whole ledger and every read model: Encore! and the status of the read side, verify, and
//! the activity log of the engine's observers: the command executors' reports (every command of every aggregate) and
//! the projection processors' (every read model, now that the processors feed them from the journal, epic 03/04).
pub mod activity_observer;
pub mod engine;
pub mod ledger;
pub mod rebuild;
pub mod verify;
