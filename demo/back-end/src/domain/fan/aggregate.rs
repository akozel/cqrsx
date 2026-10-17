//! The `Fan` state (the ticket ledger) and its `impl Aggregate`.
use crate::domain::{
    fan::{FanCommand, FanError, FanEvent},
    shared::venue::{SeatId, Zone},
};
use cqrsx::cqrs::{aggregate::Aggregate, event_sink::EventSink};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TicketState {
    Active,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ticket {
    pub concert_id: String,
    pub seat: SeatId,
    pub zone: Zone,
    pub price: u32,
    pub state: TicketState,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fan {
    /// Taken from the first `RecordTicket`.
    name: Option<String>,
    /// `booking_id` → ticket (active or cancelled). Cancelled tickets stay: they are history.
    tickets: BTreeMap<Uuid, Ticket>,
}

impl Fan {
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn tickets(&self) -> &BTreeMap<Uuid, Ticket> {
        &self.tickets
    }

    pub fn active_count(&self) -> usize {
        self.tickets.values().filter(|t| t.state == TicketState::Active).count()
    }

    pub fn total_spent(&self) -> u32 {
        self.tickets
            .values()
            .filter(|t| t.state == TicketState::Active)
            .map(|t| t.price)
            .sum()
    }
}

impl Aggregate for Fan {
    const TYPE: &'static str = "Fan";
    type Command = FanCommand;
    type Event = FanEvent;
    type Error = FanError;
    type Services = ();

    // Exactly one event per accepted command (D-02), like `Concert`.
    async fn handle(&mut self, command: FanCommand, _: &(), sink: &EventSink<Self>) -> Result<(), FanError> {
        match command {
            FanCommand::RecordTicket {
                booking_id,
                concert_id,
                seat,
                zone,
                price,
                fan_name,
            } => {
                if self.tickets.contains_key(&booking_id) {
                    return Err(FanError::AlreadyRecorded { booking_id }); // R9: redelivered task
                }
                sink.write(
                    FanEvent::TicketRecorded {
                        booking_id,
                        concert_id,
                        seat,
                        zone,
                        price,
                        fan_name,
                    },
                    self,
                )
                .await;
            }
            FanCommand::RecordCancellation {
                booking_id,
                cancellation_id,
                refund,
            } => {
                // R10: the cancellation task overtook the record task → the handler retries later.
                let ticket = self.tickets.get(&booking_id).ok_or(FanError::UnknownTicket { booking_id })?;
                if ticket.state == TicketState::Cancelled {
                    return Err(FanError::AlreadyCancelled { booking_id }); // R11: redelivered task
                }
                sink.write(
                    FanEvent::TicketCancelled {
                        booking_id,
                        cancellation_id,
                        concert_id: ticket.concert_id.clone(),
                        seat: ticket.seat.clone(),
                        refund,
                    },
                    self,
                )
                .await;
            }
        }
        Ok(())
    }

    fn apply(&mut self, event: FanEvent) {
        match event {
            FanEvent::TicketRecorded {
                booking_id,
                concert_id,
                seat,
                zone,
                price,
                fan_name,
            } => {
                self.name.get_or_insert(fan_name);
                self.tickets.insert(
                    booking_id,
                    Ticket {
                        concert_id,
                        seat,
                        zone,
                        price,
                        state: TicketState::Active,
                    },
                );
            }
            FanEvent::TicketCancelled { booking_id, .. } => {
                if let Some(ticket) = self.tickets.get_mut(&booking_id) {
                    ticket.state = TicketState::Cancelled;
                }
            }
        }
    }
}

/// Wire contract of the aggregate snapshot (`snapshots.payload`): the literal must never change.
#[cfg(test)]
mod wire_format_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fan_snapshot_keeps_its_stored_json() {
        let document = json!({
            "name": "Fox",
            "tickets": { "00000000-0000-0000-0000-000000000001": {
                "concert_id": "radiohead-2026-11-21", "seat": "B4", "zone": "standard", "price": 80, "state": "cancelled" } },
        });
        let fan: Fan = serde_json::from_value(document.clone()).unwrap();
        assert_eq!(fan.tickets().len(), 1);
        assert_eq!(serde_json::to_value(&fan).unwrap(), document);
    }
}
