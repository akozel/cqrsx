//! Read side: two `ViewProjection`s over MongoDB (`encore_read.concert_halls`, `encore_read.fan_wallets`).
//! Views are last-writer-wins registers keyed by event `sequence`, so handlers may run in any
//! order and replay is idempotent. Updated in-process right after commit — without delivery
//! guarantees, which is exactly why Encore! (replay) exists.
pub mod concert_hall;
pub mod fan_wallet;
pub mod mongo_view_repository;
pub mod notifying;
pub mod progress;
