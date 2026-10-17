//! Facts about one concert, one module per event. Stored flat (`#[serde(tag = "type")]`) so the
//! X-ray payload reads naturally; `event_type` in the `events` table is the variant name,
//! `event_version` is `"1.0"`.
pub mod booking_cancelled;
pub mod concert_announced;
pub mod seat_booked;

pub use booking_cancelled::BookingCancelled;
pub use concert_announced::ConcertAnnounced;
pub use seat_booked::SeatBooked;

use cqrsx::cqrs::event::DomainEvent;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ConcertEvent {
    ConcertAnnounced(ConcertAnnounced),
    SeatBooked(SeatBooked),
    BookingCancelled(BookingCancelled),
}

impl DomainEvent for ConcertEvent {
    fn event_type(&self) -> String {
        match self {
            ConcertEvent::ConcertAnnounced(_) => "ConcertAnnounced",
            ConcertEvent::SeatBooked(_) => "SeatBooked",
            ConcertEvent::BookingCancelled(_) => "BookingCancelled",
        }
        .to_owned()
    }

    fn event_version(&self) -> String {
        "1.0".to_owned()
    }
}

/// Wire contract of the stored events: `events.payload` keeps exactly these JSON documents and the
/// sidecar and the front-end read them. The literals must never change; the types may.
#[cfg(test)]
mod wire_format_tests {
    use super::*;
    use serde_json::{Value, json};

    fn round_trip(document: Value, event_type: &str) {
        let event: ConcertEvent = serde_json::from_value(document.clone()).unwrap();
        assert_eq!(serde_json::to_value(&event).unwrap(), document);
        assert_eq!(event.event_type(), event_type);
        assert_eq!(event.event_version(), "1.0");
    }

    #[test]
    fn concert_events_keep_their_stored_json() {
        round_trip(
            json!({ "type": "ConcertAnnounced", "artist": "Radiohead", "city": "Tbilisi", "date": "2026-11-21", "capacity": 40 }),
            "ConcertAnnounced",
        );
        round_trip(
            json!({ "type": "SeatBooked", "seat": "B4", "zone": "standard", "price": 80, "fan_id": "fox", "fan_name": "Fox",
                    "booking_id": "00000000-0000-0000-0000-000000000001" }),
            "SeatBooked",
        );
        round_trip(
            json!({ "type": "BookingCancelled", "seat": "B4", "fan_id": "fox",
                    "booking_id": "00000000-0000-0000-0000-000000000001",
                    "cancellation_id": "00000000-0000-0000-0000-000000000002", "refund": 80 }),
            "BookingCancelled",
        );
    }
}
