//! Fact: a ticket is in the fan's ledger. Continues the
//! [ticket delivery](crate::fan::application::reactions) workflow through the
//! `encore_send_e_ticket` task the workflow plans from this event and commits with it.
use crate::shared::domain::{
    ConcertId,
    venue::{SeatId, Zone},
};
use cqrsx::Event;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Event)]
#[event(name = "TicketRecorded", version = "1.0")]
pub struct TicketRecorded {
    pub booking_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub zone: Zone,
    pub price: u32,
    pub fan_name: String,
}
