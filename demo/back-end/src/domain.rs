//! Domain model: the `Concert` aggregate (seats and all business rules), the downstream `Fan`
//! aggregate (a fan's ticket ledger, fed by outbox tasks) and the shared kernel of value objects
//! both use (validated identifiers, the venue).
pub mod concert;
pub mod fan;
pub mod shared;

pub use shared::ids::{ConcertId, FanId, InvalidConcertId, InvalidFanId};
