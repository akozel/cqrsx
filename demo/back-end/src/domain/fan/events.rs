//! Facts in one fan's ticket ledger, one module per event. Stored flat like the `Concert` events.
pub mod ticket_cancelled;
pub mod ticket_recorded;

pub use ticket_cancelled::TicketCancelled;
pub use ticket_recorded::TicketRecorded;

use cqrsx::cqrs::event::DomainEvent;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FanEvent {
    TicketRecorded(TicketRecorded),
    TicketCancelled(TicketCancelled),
}

impl DomainEvent for FanEvent {
    fn event_type(&self) -> String {
        match self {
            FanEvent::TicketRecorded(_) => "TicketRecorded",
            FanEvent::TicketCancelled(_) => "TicketCancelled",
        }
        .to_owned()
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
