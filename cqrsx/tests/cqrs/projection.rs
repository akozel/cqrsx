//! The projection processor over in-memory stores: the fault model of epic 03, ticket 03.
#[path = "projection/lease.rs"]
mod lease;
#[path = "projection/live.rs"]
mod live;
#[path = "projection/model.rs"]
mod model;
#[path = "projection/rebuild.rs"]
mod rebuild;
#[path = "projection/support.rs"]
mod support;
