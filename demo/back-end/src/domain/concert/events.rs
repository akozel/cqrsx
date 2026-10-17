use crate::domain::{
    FanId,
    shared::venue::{SeatId, Zone},
};
use chrono::NaiveDate;
use cqrsx::cqrs::event::DomainEvent;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Facts about one concert. Stored flat (`#[serde(tag = "type")]`) so the X-ray payload reads
/// naturally; `event_type` in the `events` table is the variant name, `event_version` is `"1.0"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ConcertEvent {
    ConcertAnnounced {
        artist: String,
        city: String,
        date: NaiveDate,
        capacity: u32,
    },
    SeatBooked {
        seat: SeatId,
        zone: Zone,
        /// The price paid — a fact, so later price changes never rewrite history.
        price: u32,
        fan_id: FanId,
        fan_name: String,
        booking_id: Uuid,
    },
    BookingCancelled {
        seat: SeatId,
        fan_id: FanId,
        booking_id: Uuid,
        cancellation_id: Uuid,
        refund: u32,
    },
}

impl ConcertEvent {
    pub fn name(&self) -> &'static str {
        match self {
            ConcertEvent::ConcertAnnounced { .. } => "ConcertAnnounced",
            ConcertEvent::SeatBooked { .. } => "SeatBooked",
            ConcertEvent::BookingCancelled { .. } => "BookingCancelled",
        }
    }
}

impl DomainEvent for ConcertEvent {
    fn event_type(&self) -> String {
        self.name().to_owned()
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
