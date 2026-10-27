//! Given/when fixtures shared by the rule tests of every `Concert` command.
use crate::{
    concert::domain::{
        ConcertCommand, ConcertEvent, ConcertServices,
        commands::{AnnounceConcert, BookSeat, CancelBooking},
        events::{ConcertAnnounced, SeatBooked},
    },
    shared::domain::{FanId, venue::VENUE},
};
use chrono::NaiveDate;
use uuid::Uuid;

pub fn services() -> ConcertServices {
    ConcertServices { ticket_limit: 4 }
}

pub fn fan(id: &str) -> FanId {
    FanId::parse(id).unwrap()
}

fn date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 11, 21).unwrap()
}

pub fn announce() -> ConcertCommand {
    ConcertCommand::AnnounceConcert(AnnounceConcert {
        artist: "Radiohead".into(),
        city: "Tbilisi".into(),
        date: date(),
    })
}

pub fn announced() -> ConcertEvent {
    ConcertEvent::ConcertAnnounced(ConcertAnnounced {
        artist: "Radiohead".into(),
        city: "Tbilisi".into(),
        date: date(),
        capacity: 40,
    })
}

pub fn book(seat: &str, fan_id: &str, fan_name: &str, booking_id: Uuid) -> ConcertCommand {
    ConcertCommand::BookSeat(BookSeat {
        seat: seat.into(),
        fan_id: fan(fan_id),
        fan_name: fan_name.into(),
        booking_id,
    })
}

pub fn booked(seat: &str, fan_id: &str, fan_name: &str, booking_id: Uuid) -> ConcertEvent {
    let info = VENUE.seat(seat).unwrap();
    ConcertEvent::SeatBooked(SeatBooked {
        seat: info.id,
        zone: info.zone,
        price: info.price,
        fan_id: fan(fan_id),
        fan_name: fan_name.into(),
        booking_id,
    })
}

pub fn cancel(seat: &str, fan_id: &str, cancellation_id: Uuid) -> ConcertCommand {
    ConcertCommand::CancelBooking(CancelBooking {
        seat: seat.into(),
        fan_id: fan(fan_id),
        cancellation_id,
    })
}

/// Every seat booked by a different fan of the crowd, so the ticket limit never interferes.
pub fn full_house() -> Vec<ConcertEvent> {
    let mut events = vec![announced()];
    for (index, seat) in VENUE.seats().enumerate() {
        events.push(booked(seat.id.as_str(), &format!("crowd-{index}"), "Crowd", Uuid::new_v4()));
    }
    events
}
