//! Given/when fixtures shared by the rule tests of every `Fan` command.
use crate::{
    fan::domain::{
        FanCommand, FanEvent,
        commands::{RecordCancellation, RecordTicket},
        events::{TicketCancelled, TicketRecorded},
    },
    shared::domain::{
        ConcertId,
        venue::{SeatId, Zone},
    },
};
use uuid::Uuid;

pub fn seat(raw: &str) -> SeatId {
    SeatId::parse(raw).unwrap()
}

fn concert() -> ConcertId {
    ConcertId::parse("radiohead-2026-11-21").unwrap()
}

pub fn record(booking_id: Uuid) -> FanCommand {
    FanCommand::RecordTicket(RecordTicket {
        booking_id,
        concert_id: concert(),
        seat: seat("B4"),
        zone: Zone::Standard,
        price: 80,
        fan_name: "Fox".into(),
    })
}

pub fn recorded(booking_id: Uuid) -> FanEvent {
    FanEvent::TicketRecorded(TicketRecorded {
        booking_id,
        concert_id: concert(),
        seat: seat("B4"),
        zone: Zone::Standard,
        price: 80,
        fan_name: "Fox".into(),
    })
}

pub fn cancel(booking_id: Uuid, cancellation_id: Uuid) -> FanCommand {
    FanCommand::RecordCancellation(RecordCancellation {
        booking_id,
        cancellation_id,
        refund: 80,
    })
}

pub fn cancelled(booking_id: Uuid, cancellation_id: Uuid) -> FanEvent {
    FanEvent::TicketCancelled(TicketCancelled {
        booking_id,
        cancellation_id,
        concert_id: concert(),
        seat: seat("B4"),
        refund: 80,
    })
}
