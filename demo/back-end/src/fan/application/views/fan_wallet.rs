//! `fan_wallets` read model: one document per fan ledger (a fan and a concert, D-14), built only
//! from that ledger's `Fan` events; the wallet a fan sees is assembled from all of their ledgers
//! (`fan::application::wallet`). The engine applies a ledger's events to its document in order and only once
//! (the document's `last_sequence`), so the fold needs no check of its own: every event changes its ticket,
//! keyed by `booking_id`. A ticket keeps the `sequence` (the event number inside the ledger's stream) and the
//! time of the event that last changed it. Derived counters are recomputed, never incremented.
use crate::{
    fan::domain::{
        Fan, FanEvent,
        events::{TicketCancelled, TicketRecorded},
    },
    shared::domain::FanLedgerId,
};
use cqrsx::{cqrs::projection::View, domain::event::EventEnvelope};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TicketEntryState {
    Active,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TicketEntry {
    pub concert_id: String,
    pub seat: String,
    pub price: i64,
    pub state: TicketEntryState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refund: Option<i64>,
    pub sequence: i64,
    pub changed_at: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FanWalletView {
    pub fan_id: String,
    pub fan_name: String,
    pub tickets: BTreeMap<String, TicketEntry>,
    pub active_count: i64,
    pub total_spent: i64,
    pub last_sequence: i64,
}

impl FanWalletView {
    fn recompute(&mut self) {
        let active: Vec<&TicketEntry> = self
            .tickets
            .values()
            .filter(|t| t.state == TicketEntryState::Active)
            .collect();
        self.active_count = active.len() as i64;
        self.total_spent = active.iter().map(|t| t.price).sum();
    }
}

impl View<Fan> for FanWalletView {
    /// The collection of this read model in `encore_read`, and the name of its projection (the checkpoint row carries
    /// it).
    const NAME: &'static str = "fan_wallets";

    fn update(&mut self, envelope: &EventEnvelope<Fan>) {
        let sequence = envelope.sequence as i64;
        let changed_at = envelope.metadata.get("recorded_at").cloned().unwrap_or_default();
        if self.fan_id.is_empty() {
            // The stream is a ledger (`fan:concert`); the document names the fan. An id of another
            // shape is kept as it is: a replay must never fail on it.
            self.fan_id = FanLedgerId::parse(&envelope.aggregate_id)
                .map(|ledger| ledger.fan().as_str().to_owned())
                .unwrap_or_else(|_| envelope.aggregate_id.clone());
        }
        match &envelope.payload {
            FanEvent::TicketRecorded(TicketRecorded {
                booking_id,
                concert_id,
                seat,
                price,
                fan_name,
                ..
            }) => {
                if self.fan_name.is_empty() {
                    self.fan_name = fan_name.clone();
                }
                self.tickets.insert(
                    booking_id.to_string(),
                    TicketEntry {
                        concert_id: concert_id.to_string(),
                        seat: seat.to_string(),
                        price: i64::from(*price),
                        state: TicketEntryState::Active,
                        refund: None,
                        sequence,
                        changed_at,
                    },
                );
            }
            FanEvent::TicketCancelled(TicketCancelled {
                booking_id,
                concert_id,
                seat,
                refund,
                ..
            }) => {
                let key = booking_id.to_string();
                // A cancellation follows its ticket in the ledger (R10); the refund stands in for a price the
                // document does not hold, so that the fold never fails.
                let price = self.tickets.get(&key).map(|t| t.price).unwrap_or(i64::from(*refund));
                self.tickets.insert(
                    key,
                    TicketEntry {
                        concert_id: concert_id.to_string(),
                        seat: seat.to_string(),
                        price,
                        state: TicketEntryState::Cancelled,
                        refund: Some(i64::from(*refund)),
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
        ConcertId,
        venue::{SeatId, Zone},
    };
    use std::collections::HashMap;
    use uuid::Uuid;

    fn envelope(sequence: usize, payload: FanEvent) -> EventEnvelope<Fan> {
        EventEnvelope {
            aggregate_id: "fox:radiohead-2026-11-21".into(),
            sequence,
            payload,
            metadata: HashMap::from([("recorded_at".to_owned(), format!("2026-10-17T09:00:{sequence:02}.000000Z"))]),
        }
    }

    fn recorded(sequence: usize, booking_id: Uuid, price: u32) -> EventEnvelope<Fan> {
        envelope(
            sequence,
            FanEvent::TicketRecorded(TicketRecorded {
                booking_id,
                concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
                seat: SeatId::parse("B4").unwrap(),
                zone: Zone::Standard,
                price,
                fan_name: "Fox".into(),
            }),
        )
    }

    fn cancelled(sequence: usize, booking_id: Uuid) -> EventEnvelope<Fan> {
        envelope(
            sequence,
            FanEvent::TicketCancelled(TicketCancelled {
                booking_id,
                cancellation_id: Uuid::new_v4(),
                concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
                seat: SeatId::parse("B4").unwrap(),
                refund: 80,
            }),
        )
    }

    fn apply(events: &[EventEnvelope<Fan>]) -> FanWalletView {
        let mut view = FanWalletView::default();
        for event in events {
            view.update(event);
        }
        view
    }

    #[test]
    fn wallet_tracks_active_tickets_and_spending() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let view = apply(&[recorded(1, a, 80), recorded(2, b, 120), cancelled(3, a)]);
        assert_eq!((view.fan_id.as_str(), view.fan_name.as_str()), ("fox", "Fox"));
        assert_eq!((view.active_count, view.total_spent, view.last_sequence), (1, 120, 3));
        assert_eq!(view.tickets[&a.to_string()].state, TicketEntryState::Cancelled);
        assert_eq!(view.tickets[&a.to_string()].refund, Some(80));
    }

    #[test]
    fn a_stream_id_that_is_not_a_ledger_is_kept_as_the_fan_id() {
        let mut event = recorded(1, Uuid::new_v4(), 80);
        event.aggregate_id = "legacy".into();
        assert_eq!(apply(&[event]).fan_id, "legacy");
    }

    /// The fold before backlog 05/10, verbatim but for `self`: every ticket kept the sequence of the event that last
    /// changed it, an event changed a ticket only when its sequence was greater (LWW by `booking_id`), and
    /// `last_sequence` was the greatest sequence seen.
    fn update_with_lww(view: &mut FanWalletView, envelope: &EventEnvelope<Fan>) {
        let sequence = envelope.sequence as i64;
        let changed_at = envelope.metadata.get("recorded_at").cloned().unwrap_or_default();
        if view.fan_id.is_empty() {
            view.fan_id = FanLedgerId::parse(&envelope.aggregate_id)
                .map(|ledger| ledger.fan().as_str().to_owned())
                .unwrap_or_else(|_| envelope.aggregate_id.clone());
        }
        match &envelope.payload {
            FanEvent::TicketRecorded(TicketRecorded {
                booking_id,
                concert_id,
                seat,
                price,
                fan_name,
                ..
            }) => {
                if view.fan_name.is_empty() {
                    view.fan_name = fan_name.clone();
                }
                let key = booking_id.to_string();
                if view.tickets.get(&key).is_none_or(|t| t.sequence < sequence) {
                    view.tickets.insert(
                        key,
                        TicketEntry {
                            concert_id: concert_id.to_string(),
                            seat: seat.to_string(),
                            price: i64::from(*price),
                            state: TicketEntryState::Active,
                            refund: None,
                            sequence,
                            changed_at,
                        },
                    );
                }
            }
            FanEvent::TicketCancelled(TicketCancelled {
                booking_id,
                concert_id,
                seat,
                refund,
                ..
            }) => {
                let key = booking_id.to_string();
                if view.tickets.get(&key).is_none_or(|t| t.sequence < sequence) {
                    let price = view.tickets.get(&key).map(|t| t.price).unwrap_or(i64::from(*refund));
                    view.tickets.insert(
                        key,
                        TicketEntry {
                            concert_id: concert_id.to_string(),
                            seat: seat.to_string(),
                            price,
                            state: TicketEntryState::Cancelled,
                            refund: Some(i64::from(*refund)),
                            sequence,
                            changed_at,
                        },
                    );
                }
            }
        }
        view.last_sequence = view.last_sequence.max(sequence);
        view.recompute();
    }

    /// The events the histories are made of, the event at `sequence` of a history.
    const ALPHABET: usize = 5;

    fn symbol(symbol: usize, sequence: usize) -> EventEnvelope<Fan> {
        let (b1, b2) = (Uuid::from_u128(1), Uuid::from_u128(2));
        match symbol {
            0 => recorded(sequence, b1, 80),
            1 => recorded(sequence, b2, 120),
            2 => cancelled(sequence, b1),
            3 => cancelled(sequence, b2),
            _ => recorded(sequence, b1, 50),
        }
    }

    /// The engine hands a ledger's document its events in order and once each (`last_sequence`): on every such
    /// history the fold without a check of its own is the document the fold with the LWW check made.
    #[test]
    fn an_ordered_history_folds_into_the_same_wallet_with_and_without_the_lww_check() {
        let mut checked = 0;
        for length in 0..=5_u32 {
            for code in 0..ALPHABET.pow(length) {
                let history: Vec<_> = (0..length)
                    .map(|place| symbol(code / ALPHABET.pow(place) % ALPHABET, place as usize + 1))
                    .collect();
                let mut with_lww = FanWalletView::default();
                for event in &history {
                    update_with_lww(&mut with_lww, event);
                }
                assert_eq!(apply(&history), with_lww, "history {code} of length {length}");
                checked += 1;
            }
        }
        assert_eq!(checked, 3906, "every history of up to five events over five");
    }

    #[test]
    fn the_wallet_of_an_ordered_history_is_the_golden_one() {
        let (b1, b2) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let view = apply(&[recorded(1, b1, 80), recorded(2, b2, 120), cancelled(3, b1)]);
        assert_eq!(
            serde_json::to_value(&view).unwrap(),
            serde_json::json!({
                "fan_id": "fox",
                "fan_name": "Fox",
                "tickets": {
                    "00000000-0000-0000-0000-000000000001": { "concert_id": "radiohead-2026-11-21", "seat": "B4",
                        "price": 80, "state": "cancelled", "refund": 80, "sequence": 3,
                        "changed_at": "2026-10-17T09:00:03.000000Z" },
                    "00000000-0000-0000-0000-000000000002": { "concert_id": "radiohead-2026-11-21", "seat": "B4",
                        "price": 120, "state": "active", "sequence": 2, "changed_at": "2026-10-17T09:00:02.000000Z" },
                },
                "active_count": 1,
                "total_spent": 120,
                "last_sequence": 3,
            })
        );
    }
}
