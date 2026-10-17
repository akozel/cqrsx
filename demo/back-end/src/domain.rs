//! Domain model: the venue (reference data), the `Concert` aggregate (seats and all business
//! rules), the downstream `Fan` aggregate (a fan's ticket ledger, fed by outbox tasks) and the
//! validated identifiers both share.
pub mod concert;
pub mod fan;
pub mod ids;
pub mod venue;

pub use ids::{ConcertId, FanId, InvalidConcertId, InvalidFanId};
