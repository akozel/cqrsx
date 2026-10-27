//! Fact: a booking has been given back and the seat is free again. Starts the
//! [refund](crate::concert::application::reactions) workflow through the
//! `encore_record_cancellation` task the workflow plans from this event and commits with it.
use crate::shared::domain::{FanId, venue::SeatId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BookingCancelled {
    pub seat: SeatId,
    pub fan_id: FanId,
    pub booking_id: Uuid,
    pub cancellation_id: Uuid,
    /// The price paid at booking time, taken from history.
    pub refund: u32,
}
