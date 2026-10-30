//! The flow of a probe: what follows a ping ([`reactions`]: the tasks it asks for), the task that does nothing
//! ([`echo`]) and the views that count the pings ([`views`]).
pub mod echo;
pub mod reactions;
pub mod views;
