//! `concert_halls` read model: one document per concert, built only from `Concert` events.
//!
//! The library applies the events of a concert to its document strictly in order and only once (the
//! document's `last_sequence`); a document is never created from the middle of a history: a booking after a
//! wipe makes the projection processor rebuild the document from the whole history. Inside the fold,
//! every seat entry remembers the `sequence` of the event that last changed it and an event is
//! applied only if its sequence is greater (LWW). A cancellation is a tombstone
//! `{ state: "free", sequence }`, never a deleted key. Derived fields (`sold`, `revenue`,
//! `sold_out`) are recomputed from the map, never incremented.
use crate::domain::{
    concert::{
        Concert, ConcertEvent,
        events::{BookingCancelled, ConcertAnnounced, SeatBooked},
    },
    shared::venue::Zone,
};
use cqrsx::cqrs::{event::EventEnvelope, projection::View};
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
    /// Sequence of the event that produced this entry (LWW key).
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

    fn seat_is_older(&self, seat: &str, sequence: i64) -> bool {
        self.seats.get(seat).is_none_or(|entry| entry.sequence < sequence)
    }
}

fn recorded_at(envelope: &EventEnvelope<Concert>) -> String {
    // From the event's own metadata, not `now()`: a rebuild yields the very same document.
    envelope.metadata.get("recorded_at").cloned().unwrap_or_default()
}

impl View<Concert> for ConcertHallView {
    fn update(&mut self, envelope: &EventEnvelope<Concert>) {
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
                if self.seat_is_older(seat.as_str(), sequence) {
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
                            changed_at: changed_at.clone(),
                        },
                    );
                }
            }
            ConcertEvent::BookingCancelled(BookingCancelled { seat, .. }) => {
                if self.seat_is_older(seat.as_str(), sequence) {
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
                            changed_at: changed_at.clone(),
                        },
                    );
                }
            }
        }
        self.last_sequence = self.last_sequence.max(sequence);
        self.recompute();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        FanId,
        shared::venue::{SeatId, VENUE},
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
            view.update(event);
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
    fn lww_makes_order_irrelevant_and_repeats_harmless() {
        let forward = apply(&[announced(1), booked(6, "B4", "fox"), cancelled(7, "B4", "fox")]);
        let backward = apply(&[announced(1), cancelled(7, "B4", "fox"), booked(6, "B4", "fox")]);
        assert_eq!(forward, backward);
        assert_eq!(forward.seats["B4"].state, SeatState::Free);
        let repeated = apply(&[
            announced(1),
            booked(6, "B4", "fox"),
            cancelled(7, "B4", "fox"),
            booked(6, "B4", "fox"),
        ]);
        assert_eq!(repeated, forward);
        let twice = apply(&[announced(1), booked(2, "B4", "fox"), booked(2, "B4", "fox")]);
        assert_eq!(twice.sold, 1);
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
        view.update(&cancelled(100, "A1", "crowd-0"));
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
}
