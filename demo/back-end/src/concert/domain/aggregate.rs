//! The `Concert` state and its `impl Aggregate`: `apply` is the state machine; every command decides itself
//! (`commands/`).
use crate::{
    concert::domain::{
        ConcertCommand, ConcertError, ConcertEvent,
        events::{BookingCancelled, ConcertAnnounced, SeatBooked},
    },
    shared::domain::{
        ConcertId, FanId,
        venue::{SeatId, VENUE},
    },
};
use chrono::NaiveDate;
use cqrsx::domain::aggregate::Aggregate;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConcertInfo {
    pub artist: String,
    pub city: String,
    pub date: NaiveDate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Booking {
    pub fan_id: FanId,
    pub fan_name: String,
    pub booking_id: Uuid,
    pub price: u32,
}

/// Configuration the rules depend on; history does not (replay calls `apply`, never `decide`).
#[derive(Debug, Clone)]
pub struct ConcertRules {
    pub ticket_limit: usize,
}

impl Default for ConcertRules {
    fn default() -> Self {
        Self { ticket_limit: 4 }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Concert {
    /// `None` ⇒ the concert has not been announced (rule R1).
    info: Option<ConcertInfo>,
    /// Only occupied seats; a cancelled booking is removed.
    bookings: BTreeMap<SeatId, Booking>,
}

impl Concert {
    pub fn info(&self) -> Option<&ConcertInfo> {
        self.info.as_ref()
    }

    pub fn bookings(&self) -> &BTreeMap<SeatId, Booking> {
        &self.bookings
    }

    pub fn free_seats(&self) -> u32 {
        VENUE.capacity().saturating_sub(self.bookings.len() as u32)
    }

    /// Derived, not an event (decision D-03).
    pub fn is_sold_out(&self) -> bool {
        self.free_seats() == 0
    }

    pub fn tickets_of(&self, fan_id: &FanId) -> usize {
        self.bookings.values().filter(|b| &b.fan_id == fan_id).count()
    }

    pub fn revenue(&self) -> u32 {
        self.bookings.values().map(|b| b.price).sum()
    }
}

impl Aggregate for Concert {
    const TYPE: &'static str = "Concert";
    type Id = ConcertId;
    type Command = ConcertCommand;
    type Event = ConcertEvent;
    type Rejection = ConcertError;
    // Each command decides itself (see `commands/`): `ConcertCommand: Decide<Concert>`. Every accepted
    // command writes exactly ONE event (decision D-02): the outbox repository commits one event plus its
    // tasks atomically and rejects multi-event batches, so `decide` returns one event, not a list.
    type Rules = ConcertRules;

    /// The state machine: how every fact changes the concert. Replay calls only this.
    fn apply(&mut self, event: ConcertEvent) {
        match event {
            ConcertEvent::ConcertAnnounced(ConcertAnnounced { artist, city, date, .. }) => {
                self.info = Some(ConcertInfo { artist, city, date });
            }
            ConcertEvent::SeatBooked(SeatBooked {
                seat,
                fan_id,
                fan_name,
                booking_id,
                price,
                ..
            }) => {
                self.bookings.insert(
                    seat,
                    Booking {
                        fan_id,
                        fan_name,
                        booking_id,
                        price,
                    },
                );
            }
            ConcertEvent::BookingCancelled(BookingCancelled { seat, .. }) => {
                self.bookings.remove(&seat);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::concert::domain::test_support::{announced, booked, fan, full_house};

    #[test]
    fn after_the_last_booking_the_concert_is_sold_out() {
        let mut concert = Concert::default();
        for event in full_house() {
            concert.apply(event);
        }
        assert!(concert.is_sold_out());
        assert_eq!(concert.free_seats(), 0);
        assert_eq!(concert.revenue(), 8 * 120 + 16 * 80 + 16 * 50);
        concert.apply(ConcertEvent::BookingCancelled(BookingCancelled {
            seat: SeatId::parse("A1").unwrap(),
            fan_id: fan("crowd-0"),
            booking_id: Uuid::new_v4(),
            cancellation_id: Uuid::new_v4(),
            refund: 120,
        }));
        assert!(!concert.is_sold_out());
    }

    #[test]
    fn snapshot_round_trip_keeps_seat_keys_as_strings() {
        let mut concert = Concert::default();
        concert.apply(announced());
        concert.apply(booked("B4", "fox", "Fox", Uuid::new_v4()));
        let json = serde_json::to_value(&concert).unwrap();
        assert!(json["bookings"]["B4"].is_object());
        let restored: Concert = serde_json::from_value(json).unwrap();
        assert_eq!(restored, concert);
    }
}

/// Wire contract of the aggregate snapshot (`snapshots.payload`): the literal must never change.
#[cfg(test)]
mod wire_format_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn concert_snapshot_keeps_its_stored_json() {
        let document = json!({
            "info": { "artist": "Radiohead", "city": "Tbilisi", "date": "2026-11-21" },
            "bookings": { "B4": { "fan_id": "fox", "fan_name": "Fox", "booking_id": "00000000-0000-0000-0000-000000000001", "price": 80 } },
        });
        let concert: Concert = serde_json::from_value(document.clone()).unwrap();
        assert_eq!(concert.bookings().len(), 1);
        assert_eq!(serde_json::to_value(&concert).unwrap(), document);
    }
}
