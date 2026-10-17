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

/// Wire contract of the stored events (see `concert::events`): the literals must never change.
#[cfg(test)]
mod wire_format_tests {
    use super::*;
    use serde_json::{Value, json};

    fn round_trip(document: Value, event_type: &str) {
        let event: FanEvent = serde_json::from_value(document.clone()).unwrap();
        assert_eq!(serde_json::to_value(&event).unwrap(), document);
        assert_eq!(event.event_type(), event_type);
        assert_eq!(event.event_version(), "1.0");
    }

    #[test]
    fn fan_events_keep_their_stored_json() {
        round_trip(
            json!({ "type": "TicketRecorded", "booking_id": "00000000-0000-0000-0000-000000000001",
                    "concert_id": "radiohead-2026-11-21", "seat": "B4", "zone": "standard", "price": 80, "fan_name": "Fox" }),
            "TicketRecorded",
        );
        round_trip(
            json!({ "type": "TicketCancelled", "booking_id": "00000000-0000-0000-0000-000000000001",
                    "cancellation_id": "00000000-0000-0000-0000-000000000002", "concert_id": "radiohead-2026-11-21",
                    "seat": "B4", "refund": 80 }),
            "TicketCancelled",
        );
    }
}
