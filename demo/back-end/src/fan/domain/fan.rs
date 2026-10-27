//! The `Fan` state (the ticket ledger) and its `impl Aggregate`.
use crate::{
    fan::domain::{
        FanCommand, FanError, FanEvent,
        events::{TicketCancelled, TicketRecorded},
    },
    shared::domain::{
        ConcertId,
        venue::{SeatId, Zone},
    },
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
    pub concert_id: ConcertId,
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

    /// Each command decides itself (see `commands/`); exactly one event per accepted command
    /// (D-02), like `Concert`.
    async fn handle(&mut self, command: FanCommand, services: &(), sink: &EventSink<Self>) -> Result<(), FanError> {
        let event = match command {
            FanCommand::RecordTicket(command) => command.decide(self, services)?,
            FanCommand::RecordCancellation(command) => command.decide(self, services)?,
        };
        sink.write(event, self).await;
        Ok(())
    }

    /// The state machine. Cancelled tickets stay in the ledger: they are history.
    fn apply(&mut self, event: FanEvent) {
        match event {
            FanEvent::TicketRecorded(TicketRecorded {
                booking_id,
                concert_id,
                seat,
                zone,
                price,
                fan_name,
            }) => {
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
            FanEvent::TicketCancelled(TicketCancelled { booking_id, .. }) => {
                if let Some(ticket) = self.tickets.get_mut(&booking_id) {
                    ticket.state = TicketState::Cancelled;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fan::domain::test_support::{cancel, cancelled, record, recorded, seat};
    use cqrsx::cqrs::testing::TestFramework;

    #[test]
    fn derived_counters_ignore_cancelled_tickets_and_name_comes_from_the_first_ticket() {
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();
        let mut fan = Fan::default();
        fan.apply(recorded(first));
        fan.apply(FanEvent::TicketRecorded(TicketRecorded {
            booking_id: second,
            concert_id: ConcertId::parse("rammstein-2026-12-05").unwrap(),
            seat: seat("A1"),
            zone: Zone::Vip,
            price: 120,
            fan_name: "Fox (renamed)".into(),
        }));
        assert_eq!(fan.name(), Some("Fox"));
        assert_eq!((fan.active_count(), fan.total_spent()), (2, 200));
        fan.apply(cancelled(first, Uuid::new_v4()));
        assert_eq!((fan.active_count(), fan.total_spent()), (1, 120));
        assert_eq!(fan.tickets().len(), 2, "cancelled tickets remain as history");
        let json = serde_json::to_value(&fan).unwrap();
        assert!(json["tickets"][first.to_string()].is_object());
        let restored: Fan = serde_json::from_value(json).unwrap();
        assert_eq!(restored, fan);
    }

    /// Decision D-02 for the downstream aggregate.
    #[test]
    fn d02_every_accepted_fan_command_yields_exactly_one_event() {
        let id = Uuid::new_v4();
        let states = vec![vec![], vec![recorded(id)], vec![recorded(id), cancelled(id, Uuid::new_v4())]];
        let commands = vec![record(id), record(Uuid::new_v4()), cancel(id, Uuid::new_v4())];
        let mut accepted = 0;
        for state in &states {
            for command in &commands {
                if let Ok(events) = TestFramework::<Fan>::with(())
                    .given(state.clone())
                    .when(command.clone())
                    .inspect_result()
                {
                    accepted += 1;
                    assert_eq!(events.len(), 1, "{command:?} produced {events:?}");
                }
            }
        }
        assert!(accepted >= 3);
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
