//! Fact: a seat has been sold. Starts the
//! [ticket delivery](crate::concert::application::reactions) workflow through the
//! `encore_record_ticket` task the workflow plans from this event and commits with it.
use crate::shared::domain::{
    FanId,
    venue::{SeatId, Zone},
};
use cqrsx::domain::event::Event;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Event)]
#[event(name = "SeatBooked", version = "1.0")]
pub struct SeatBooked {
    pub seat: SeatId,
    pub zone: Zone,
    /// The price paid — a fact, so later price changes never rewrite history.
    pub price: u32,
    pub fan_id: FanId,
    pub fan_name: String,
    pub booking_id: Uuid,
}
