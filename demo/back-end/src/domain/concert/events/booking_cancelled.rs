//! Fact: a booking has been given back and the seat is free again. Starts the
//! [refund](crate::application::workflows::refund) workflow through the
//! `encore_record_cancellation` task published atomically with this event.
use crate::domain::{FanId, shared::venue::SeatId};
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
