//! Facts in one fan's ticket ledger, one module per event. Stored flat like the `Concert` events, with
//! the `#[event(name, version)]` of each fact.
pub mod ticket_cancelled;
pub mod ticket_recorded;

pub use ticket_cancelled::TicketCancelled;
pub use ticket_recorded::TicketRecorded;

use cqrsx::Events;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Events)]
#[serde(tag = "type")]
pub enum FanEvent {
    TicketRecorded(TicketRecorded),
    TicketCancelled(TicketCancelled),
}

/// Wire contract of the stored events (see `concert::events`): the literals must never change.
#[cfg(test)]
mod wire_format_tests {
    use super::*;
    use cqrsx::cqrs::event::DomainEvent;
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
