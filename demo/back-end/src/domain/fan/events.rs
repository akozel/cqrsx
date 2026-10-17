use crate::domain::shared::venue::{SeatId, Zone};
use cqrsx::cqrs::event::DomainEvent;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FanEvent {
    TicketRecorded {
        booking_id: Uuid,
        concert_id: String,
        seat: SeatId,
        zone: Zone,
        price: u32,
        fan_name: String,
    },
    TicketCancelled {
        booking_id: Uuid,
        cancellation_id: Uuid,
        concert_id: String,
        seat: SeatId,
        refund: u32,
    },
}

impl FanEvent {
    pub fn name(&self) -> &'static str {
        match self {
            FanEvent::TicketRecorded { .. } => "TicketRecorded",
            FanEvent::TicketCancelled { .. } => "TicketCancelled",
        }
    }
}

impl DomainEvent for FanEvent {
    fn event_type(&self) -> String {
        self.name().to_owned()
    }

    fn event_version(&self) -> String {
        "1.0".to_owned()
    }
}
