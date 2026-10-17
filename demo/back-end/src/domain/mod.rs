//! Domain model: the venue (reference data), the `Concert` aggregate (seats and all business
//! rules) and the downstream `Fan` aggregate (a fan's ticket ledger, fed by outbox tasks).
pub mod concert;
pub mod fan;
pub mod venue;
