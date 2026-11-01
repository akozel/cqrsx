//! `concert_halls` read model: one document per concert, built only from `Concert` events.
//!
//! The engine applies the events of a concert to its document strictly in order and only once (the
//! document's `last_sequence`); a document is never created from the middle of a history: a booking after a
//! wipe makes the projection processor rebuild the document from the whole history. So the fold needs no
//! check of its own: every event changes its seat. A seat entry keeps the `sequence` and the time of the
//! event that last changed it; a cancellation is a tombstone `{ state: "free", sequence }`, never a deleted
//! key. Derived fields (`sold`, `revenue`, `sold_out`) are recomputed from the map, never incremented.
use crate::{
    concert::domain::{
        Concert, ConcertEvent,
        events::{BookingCancelled, ConcertAnnounced, SeatBooked},
    },
    shared::domain::venue::Zone,
};
use cqrsx::{domain::event::EventEnvelope, view::View};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConcertSummary {
    pub artist: String,
    pub city: String,
    pub date: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SeatState {
    Free,
    Booked,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeatEntry {
    pub state: SeatState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fan_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fan_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<Zone>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub booking_id: Option<String>,
    /// Sequence of the event that produced this entry.
    pub sequence: i64,
    pub changed_at: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ConcertHallView {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concert: Option<ConcertSummary>,
    pub capacity: i64,
    pub seats: BTreeMap<String, SeatEntry>,
    pub sold: i64,
    pub revenue: i64,
    pub sold_out: bool,
    pub last_sequence: i64,
}

impl ConcertHallView {
    fn recompute(&mut self) {
        let booked: Vec<&SeatEntry> = self.seats.values().filter(|s| s.state == SeatState::Booked).collect();
        self.sold = booked.len() as i64;
        self.revenue = booked.iter().map(|s| s.price.unwrap_or(0)).sum();
        self.sold_out = self.capacity > 0 && self.sold >= self.capacity;
    }
}

fn recorded_at(envelope: &EventEnvelope<Concert>) -> String {
    // From the event's own metadata, not `now()`: a rebuild yields the very same document.
    envelope.metadata.get("recorded_at").cloned().unwrap_or_default()
}

impl View<Concert> for ConcertHallView {
    /// The collection of this read model in `encore_read`, and the name of its projection (the checkpoint row carries
    /// it).
    const NAME: &'static str = "concert_halls";

    fn apply(&mut self, envelope: &EventEnvelope<Concert>) {
        let sequence = envelope.sequence as i64;
        let changed_at = recorded_at(envelope);
        match &envelope.payload {
            ConcertEvent::ConcertAnnounced(ConcertAnnounced {
                artist,
                city,
                date,
                capacity,
            }) => {
                self.concert = Some(ConcertSummary {
                    artist: artist.clone(),
                    city: city.clone(),
                    date: date.to_string(),
                });
                self.capacity = i64::from(*capacity);
            }
            ConcertEvent::SeatBooked(SeatBooked {
                seat,
                zone,
                price,
                fan_id,
                fan_name,
                booking_id,
            }) => {
                self.seats.insert(
                    seat.to_string(),
                    SeatEntry {
                        state: SeatState::Booked,
                        fan_id: Some(fan_id.to_string()),
                        fan_name: Some(fan_name.clone()),
                        zone: Some(*zone),
                        price: Some(i64::from(*price)),
                        booking_id: Some(booking_id.to_string()),
                        sequence,
                        changed_at,
                    },
                );
            }
            ConcertEvent::BookingCancelled(BookingCancelled { seat, .. }) => {
                self.seats.insert(
                    seat.to_string(),
                    SeatEntry {
                        state: SeatState::Free,
                        fan_id: None,
                        fan_name: None,
                        zone: None,
                        price: None,
                        booking_id: None,
                        sequence,
                        changed_at,
                    },
                );
            }
        }
        self.last_sequence = sequence;
        self.recompute();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::domain::{
        FanId,
        venue::{SeatId, VENUE},
    };
    use chrono::NaiveDate;
    use std::collections::HashMap;
    use uuid::Uuid;

    fn envelope(sequence: usize, payload: ConcertEvent) -> EventEnvelope<Concert> {
        EventEnvelope {
            aggregate_id: "radiohead-2026-11-21".into(),
            sequence,
            payload,
            metadata: HashMap::from([("recorded_at".to_owned(), format!("2026-10-17T09:00:{sequence:02}.000000Z"))]),
        }
    }

    fn announced(sequence: usize) -> EventEnvelope<Concert> {
        envelope(
            sequence,
            ConcertEvent::ConcertAnnounced(ConcertAnnounced {
                artist: "A".into(),
                city: "C".into(),
                date: NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
                capacity: 40,
            }),
        )
    }

    fn booked(sequence: usize, seat: &str, fan: &str) -> EventEnvelope<Concert> {
        let info = VENUE.seat(seat).unwrap();
        envelope(
            sequence,
            ConcertEvent::SeatBooked(SeatBooked {
                seat: info.id,
                zone: info.zone,
                price: info.price,
                fan_id: FanId::parse(fan).unwrap(),
                fan_name: fan.to_uppercase(),
                booking_id: Uuid::new_v4(),
            }),
        )
    }

    fn cancelled(sequence: usize, seat: &str, fan: &str) -> EventEnvelope<Concert> {
        envelope(
            sequence,
            ConcertEvent::BookingCancelled(BookingCancelled {
                seat: SeatId::parse(seat).unwrap(),
                fan_id: FanId::parse(fan).unwrap(),
                booking_id: Uuid::new_v4(),
                cancellation_id: Uuid::new_v4(),
                refund: 80,
            }),
        )
    }

    fn apply(events: &[EventEnvelope<Concert>]) -> ConcertHallView {
        let mut view = ConcertHallView::default();
        for event in events {
            view.apply(event);
        }
        view
    }

    #[test]
    fn sequence_of_events_builds_the_expected_document() {
        let view = apply(&[
            announced(1),
            booked(2, "B4", "fox"),
            booked(3, "A1", "panda"),
            cancelled(4, "A1", "panda"),
        ]);
        assert_eq!(view.concert.as_ref().unwrap().date, "2026-11-21");
        assert_eq!((view.capacity, view.sold, view.revenue, view.sold_out), (40, 1, 80, false));
        assert_eq!(view.seats["B4"].state, SeatState::Booked);
        assert_eq!(view.seats["B4"].fan_id.as_deref(), Some("fox"));
        assert_eq!(view.seats["B4"].changed_at, "2026-10-17T09:00:02.000000Z");
        let a1 = &view.seats["A1"];
        assert_eq!(
            (a1.state.clone(), a1.sequence, a1.fan_id.is_none()),
            (SeatState::Free, 4, true)
        );
        assert_eq!(view.last_sequence, 4);
    }

    #[test]
    fn derived_fields_are_recomputed_and_sold_out_is_derived() {
        let mut events = vec![announced(1)];
        for (index, seat) in VENUE.seats().enumerate() {
            events.push(booked(index + 2, seat.id.as_str(), &format!("crowd-{index}")));
        }
        let view = apply(&events);
        assert_eq!(
            (view.sold, view.revenue, view.sold_out),
            (40, 8 * 120 + 16 * 80 + 16 * 50, true)
        );
        let mut view = view;
        view.apply(&cancelled(100, "A1", "crowd-0"));
        assert_eq!((view.sold, view.sold_out), (39, false));
    }

    #[test]
    fn the_document_keeps_its_shape() {
        let view = apply(&[announced(1), booked(2, "B4", "fox")]);
        let json = serde_json::to_value(&view).unwrap();
        assert_eq!(serde_json::from_value::<ConcertHallView>(json.clone()).unwrap(), view);
        assert_eq!(json["seats"]["B4"]["state"], "booked");
        assert!(json["seats"]["B4"].get("zone").is_some());
    }

    /// The fold before backlog 05/10, verbatim but for `self`: every seat entry kept the sequence of the event that
    /// last changed it, an event changed an entry only when its sequence was greater (LWW), and `last_sequence` was
    /// the greatest sequence seen.
    fn update_with_lww(view: &mut ConcertHallView, envelope: &EventEnvelope<Concert>) {
        let seat_is_older = |view: &ConcertHallView, seat: &str, sequence: i64| {
            view.seats.get(seat).is_none_or(|entry| entry.sequence < sequence)
        };
        let sequence = envelope.sequence as i64;
        let changed_at = recorded_at(envelope);
        match &envelope.payload {
            ConcertEvent::ConcertAnnounced(ConcertAnnounced {
                artist,
                city,
                date,
                capacity,
            }) => {
                view.concert = Some(ConcertSummary {
                    artist: artist.clone(),
                    city: city.clone(),
                    date: date.to_string(),
                });
                view.capacity = i64::from(*capacity);
            }
            ConcertEvent::SeatBooked(SeatBooked {
                seat,
                zone,
                price,
                fan_id,
                fan_name,
                booking_id,
            }) => {
                if seat_is_older(view, seat.as_str(), sequence) {
                    view.seats.insert(
                        seat.to_string(),
                        SeatEntry {
                            state: SeatState::Booked,
                            fan_id: Some(fan_id.to_string()),
                            fan_name: Some(fan_name.clone()),
                            zone: Some(*zone),
                            price: Some(i64::from(*price)),
                            booking_id: Some(booking_id.to_string()),
                            sequence,
                            changed_at: changed_at.clone(),
                        },
                    );
                }
            }
            ConcertEvent::BookingCancelled(BookingCancelled { seat, .. }) => {
                if seat_is_older(view, seat.as_str(), sequence) {
                    view.seats.insert(
                        seat.to_string(),
                        SeatEntry {
                            state: SeatState::Free,
                            fan_id: None,
                            fan_name: None,
                            zone: None,
                            price: None,
                            booking_id: None,
                            sequence,
                            changed_at: changed_at.clone(),
                        },
                    );
                }
            }
        }
        view.last_sequence = view.last_sequence.max(sequence);
        view.recompute();
    }

    /// The events the histories are made of, the event at `sequence` of a history.
    const ALPHABET: usize = 6;

    fn symbol(symbol: usize, sequence: usize) -> EventEnvelope<Concert> {
        match symbol {
            0 => announced(sequence),
            1 => booked(sequence, "B4", "fox"),
            2 => booked(sequence, "A1", "panda"),
            3 => booked(sequence, "A1", "fox"),
            4 => cancelled(sequence, "A1", "panda"),
            _ => cancelled(sequence, "B4", "fox"),
        }
    }

    /// The engine hands a document the events of its concert in order and once each (`last_sequence`): on every
    /// such history the fold without a check of its own is the document the fold with the LWW check made.
    #[test]
    fn an_ordered_history_folds_into_the_same_document_with_and_without_the_lww_check() {
        let mut checked = 0;
        for length in 0..=5_u32 {
            for code in 0..ALPHABET.pow(length) {
                let history: Vec<_> = (0..length)
                    .map(|place| symbol(code / ALPHABET.pow(place) % ALPHABET, place as usize + 1))
                    .collect();
                let mut with_lww = ConcertHallView::default();
                for event in &history {
                    update_with_lww(&mut with_lww, event);
                }
                assert_eq!(apply(&history), with_lww, "history {code} of length {length}");
                checked += 1;
            }
        }
        assert_eq!(checked, 9331, "every history of up to five events over six");
    }

    #[test]
    fn the_document_of_an_ordered_history_is_the_golden_one() {
        let booking = |sequence, seat, fan, id| {
            let mut event = booked(sequence, seat, fan);
            if let ConcertEvent::SeatBooked(booked) = &mut event.payload {
                booked.booking_id = Uuid::from_u128(id);
            }
            event
        };
        let view = apply(&[
            announced(1),
            booking(2, "B4", "fox", 1),
            booking(3, "A1", "panda", 2),
            cancelled(4, "A1", "panda"),
            booking(5, "A1", "fox", 3),
            booking(6, "B5", "panda", 4),
            cancelled(7, "B5", "panda"),
        ]);
        assert_eq!(
            serde_json::to_value(&view).unwrap(),
            serde_json::json!({
                "concert": { "artist": "A", "city": "C", "date": "2026-11-21" },
                "capacity": 40,
                "seats": {
                    "A1": { "state": "booked", "fan_id": "fox", "fan_name": "FOX", "zone": "vip", "price": 120,
                            "booking_id": "00000000-0000-0000-0000-000000000003", "sequence": 5,
                            "changed_at": "2026-10-17T09:00:05.000000Z" },
                    "B4": { "state": "booked", "fan_id": "fox", "fan_name": "FOX", "zone": "standard", "price": 80,
                            "booking_id": "00000000-0000-0000-0000-000000000001", "sequence": 2,
                            "changed_at": "2026-10-17T09:00:02.000000Z" },
                    "B5": { "state": "free", "sequence": 7, "changed_at": "2026-10-17T09:00:07.000000Z" },
                },
                "sold": 2,
                "revenue": 200,
                "sold_out": false,
                "last_sequence": 7,
            })
        );
    }
}
