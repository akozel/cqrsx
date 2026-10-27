//! The `Concert` state and its `impl Aggregate`: `handle` checks the rules, `apply` is the state
//! machine.
use crate::{
    concert::domain::{
        ConcertCommand, ConcertError, ConcertEvent,
        events::{BookingCancelled, ConcertAnnounced, SeatBooked},
    },
    shared::domain::{
        FanId,
        venue::{SeatId, VENUE},
    },
};
use chrono::NaiveDate;
use cqrsx::cqrs::{aggregate::Aggregate, event_sink::EventSink};
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

/// Configuration the rules depend on; history does not (replay calls `apply`, never `handle`).
#[derive(Debug, Clone)]
pub struct ConcertServices {
    pub ticket_limit: usize,
}

impl Default for ConcertServices {
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
    type Command = ConcertCommand;
    type Event = ConcertEvent;
    type Error = ConcertError;
    type Services = ConcertServices;

    /// Each command decides itself (see `commands/`). Every accepted command writes exactly ONE
    /// event (decision D-02): the outbox repository commits one event plus its tasks atomically
    /// and rejects multi-event batches, so `decide` returns one event, not a list.
    async fn handle(
        &mut self,
        command: ConcertCommand,
        services: &ConcertServices,
        sink: &EventSink<Self>,
    ) -> Result<(), ConcertError> {
        let event = match command {
            ConcertCommand::AnnounceConcert(command) => command.decide(self, services)?,
            ConcertCommand::BookSeat(command) => command.decide(self, services)?,
            ConcertCommand::CancelBooking(command) => command.decide(self, services)?,
        };
        sink.write(event, self).await;
        Ok(())
    }

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
    use crate::concert::domain::test_support::{announce, announced, book, booked, cancel, fan, full_house, services};
    use cqrsx::cqrs::testing::TestFramework;

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

    /// Decision D-02: the outbox repository commits exactly one event per command. `decide`
    /// returns one event by its signature; this safeguard keeps `handle` honest as well.
    #[test]
    fn d02_every_accepted_concert_command_yields_exactly_one_event() {
        let booking_id = Uuid::new_v4();
        let states: Vec<Vec<ConcertEvent>> = vec![
            vec![],
            vec![announced()],
            vec![announced(), booked("B4", "fox", "Fox", booking_id)],
            {
                let mut almost_full = full_house();
                almost_full.pop();
                almost_full
            },
        ];
        let commands = vec![
            announce(),
            book("B4", "fox", "Fox", Uuid::new_v4()),
            book("E8", "panda", "Panda", Uuid::new_v4()),
            cancel("B4", "fox", Uuid::new_v4()),
        ];
        let mut accepted = 0;
        for state in &states {
            for command in &commands {
                let result = TestFramework::<Concert>::with(services())
                    .given(state.clone())
                    .when(command.clone())
                    .inspect_result();
                if let Ok(events) = result {
                    accepted += 1;
                    assert_eq!(events.len(), 1, "{command:?} on {} events produced {events:?}", state.len());
                }
            }
        }
        assert!(accepted >= 4, "the matrix must contain accepted commands");
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
