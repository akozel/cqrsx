//! `fan_wallets` read model: one document per fan ledger (a fan and a concert, D-14), built only
//! from that ledger's `Fan` events; the wallet a fan sees is assembled from all of their ledgers
//! (`application::fan::wallet`). LWW key is `booking_id`; `sequence` is the event number inside the
//! ledger's stream, so comparisons are valid within one document. Derived counters are recomputed,
//! never incremented.
use crate::domain::{
    FanLedgerId,
    fan::{
        Fan, FanEvent,
        events::{TicketCancelled, TicketRecorded},
    },
};
use cqrsx::cqrs::{event::EventEnvelope, projection::View};
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
                let key = booking_id.to_string();
                if self.tickets.get(&key).is_none_or(|t| t.sequence < sequence) {
                    self.tickets.insert(
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
                if self.tickets.get(&key).is_none_or(|t| t.sequence < sequence) {
                    // The cancellation carries everything needed: a wallet that missed the
                    // TicketRecorded event (or sees events out of order) still converges.
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
        }
        self.last_sequence = self.last_sequence.max(sequence);
        self.recompute();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        ConcertId,
        shared::venue::{SeatId, Zone},
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

    #[test]
    fn lww_by_booking_id_is_order_independent_and_idempotent() {
        let a = Uuid::new_v4();
        let forward = apply(&[recorded(1, a, 80), cancelled(2, a)]);
        let backward = apply(&[cancelled(2, a), recorded(1, a, 80)]);
        assert_eq!(forward, backward);
        assert_eq!(apply(&[recorded(1, a, 80), cancelled(2, a), recorded(1, a, 80)]), forward);
        assert_eq!(apply(&[recorded(1, a, 80), recorded(1, a, 80)]).active_count, 1);
    }
}
