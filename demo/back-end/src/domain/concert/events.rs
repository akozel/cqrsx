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
