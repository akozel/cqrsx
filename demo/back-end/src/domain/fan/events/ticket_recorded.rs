//! Fact: a ticket is in the fan's ledger. Continues the
//! [ticket delivery](crate::application::workflows::ticket_delivery) workflow through the
//! `encore_send_e_ticket` task published atomically with this event.
use crate::domain::{
    ConcertId,
    shared::venue::{SeatId, Zone},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketRecorded {
    pub booking_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub zone: Zone,
    pub price: u32,
    pub fan_name: String,
}
