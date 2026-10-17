//! One test per rule R1–R8 with a recognizable name, happy paths for the three commands, and the
//! D-02 safeguard ("every accepted command yields exactly one event").
use super::*;
use crate::domain::{
    FanId,
    shared::venue::{SeatId, VENUE, Zone},
};
use chrono::NaiveDate;
use cqrsx::cqrs::aggregate::Aggregate;
use cqrsx::cqrs::testing::TestFramework;
use uuid::Uuid;

fn services() -> ConcertServices {
    ConcertServices { ticket_limit: 4 }
}

fn fan(id: &str) -> FanId {
    FanId::parse(id).unwrap()
}

fn date() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 11, 21).unwrap()
}

fn announced() -> ConcertEvent {
    ConcertEvent::ConcertAnnounced {
        artist: "Radiohead".into(),
        city: "Tbilisi".into(),
        date: date(),
        capacity: 40,
    }
}

fn booked(seat: &str, fan_id: &str, fan_name: &str, booking_id: Uuid) -> ConcertEvent {
    let info = VENUE.seat(seat).unwrap();
    ConcertEvent::SeatBooked {
        seat: info.id,
        zone: info.zone,
        price: info.price,
        fan_id: fan(fan_id),
        fan_name: fan_name.into(),
        booking_id,
    }
}

fn book(seat: &str, fan_id: &str, fan_name: &str, booking_id: Uuid) -> ConcertCommand {
    ConcertCommand::BookSeat {
        seat: seat.into(),
        fan_id: fan(fan_id),
        fan_name: fan_name.into(),
        booking_id,
    }
}

fn cancel(seat: &str, fan_id: &str, cancellation_id: Uuid) -> ConcertCommand {
    ConcertCommand::CancelBooking {
        seat: seat.into(),
        fan_id: fan(fan_id),
        cancellation_id,
    }
}

fn announce() -> ConcertCommand {
    ConcertCommand::AnnounceConcert {
        artist: "Radiohead".into(),
        city: "Tbilisi".into(),
        date: date(),
    }
}

/// Every seat booked by sixty different fans (crowd), so the ticket limit never interferes.
fn full_house() -> Vec<ConcertEvent> {
    let mut events = vec![announced()];
    for (index, seat) in VENUE.seats().enumerate() {
        events.push(booked(seat.id.as_str(), &format!("crowd-{index}"), "Crowd", Uuid::new_v4()));
    }
    events
}

#[test]
fn announce_concert_happy_path() {
    TestFramework::<Concert>::with(services())
        .given_no_previous_events()
        .when(announce())
        .then_expect_events(vec![announced()]);
}

#[test]
fn book_seat_happy_path_records_zone_and_price() {
    let id = Uuid::new_v4();
    TestFramework::<Concert>::with(services())
        .given(vec![announced()])
        .when(book("b4", "fox", "Fox", id))
        .then_expect_events(vec![ConcertEvent::SeatBooked {
            seat: SeatId::parse("B4").unwrap(),
            zone: Zone::Standard,
            price: 80,
            fan_id: fan("fox"),
            fan_name: "Fox".into(),
            booking_id: id,
        }]);
}

#[test]
fn cancel_booking_happy_path_refunds_the_price_paid_in_history() {
    let booking_id = Uuid::new_v4();
    let cancellation_id = Uuid::new_v4();
    // A VIP seat booked at 120: the refund comes from the SeatBooked fact.
    TestFramework::<Concert>::with(services())
        .given(vec![announced(), booked("A1", "fox", "Fox", booking_id)])
        .when(cancel("A1", "fox", cancellation_id))
        .then_expect_events(vec![ConcertEvent::BookingCancelled {
            seat: SeatId::parse("A1").unwrap(),
            fan_id: fan("fox"),
            booking_id,
            cancellation_id,
            refund: 120,
        }]);
}

#[test]
fn r1_rejects_booking_in_an_unannounced_concert() {
    TestFramework::<Concert>::with(services())
        .given_no_previous_events()
        .when(book("B4", "fox", "Fox", Uuid::new_v4()))
        .then_expect_error(ConcertError::ConcertNotFound);
    TestFramework::<Concert>::with(services())
        .given_no_previous_events()
        .when(cancel("B4", "fox", Uuid::new_v4()))
        .then_expect_error(ConcertError::ConcertNotFound);
}

#[test]
fn r2_rejects_a_second_announcement() {
    TestFramework::<Concert>::with(services())
        .given(vec![announced()])
        .when(announce())
        .then_expect_error(ConcertError::AlreadyAnnounced);
}

#[test]
fn r3_rejects_unknown_seat() {
    TestFramework::<Concert>::with(services())
        .given(vec![announced()])
        .when(book("Z9", "fox", "Fox", Uuid::new_v4()))
        .then_expect_error(ConcertError::UnknownSeat { seat: "Z9".into() });
    TestFramework::<Concert>::with(services())
        .given(vec![announced()])
        .when(cancel("Z9", "fox", Uuid::new_v4()))
        .then_expect_error(ConcertError::UnknownSeat { seat: "Z9".into() });
}

#[test]
fn r4_rejects_booking_when_sold_out() {
    let mut history = full_house();
    // Free B4 again? No: sold-out must win over "seat taken" for a taken seat, and a cancelled seat
    // makes the hall not sold out any more. So we test the taken seat on a full hall.
    history.truncate(history.len());
    TestFramework::<Concert>::with(services())
        .given(history)
        .when(book("B4", "fox", "Fox", Uuid::new_v4()))
        .then_expect_error(ConcertError::SoldOut);
}

#[test]
fn after_the_last_booking_the_concert_is_sold_out() {
    let mut concert = Concert::default();
    for event in full_house() {
        concert.apply(event);
    }
    assert!(concert.is_sold_out());
    assert_eq!(concert.free_seats(), 0);
    assert_eq!(concert.revenue(), 8 * 120 + 16 * 80 + 16 * 50);
    concert.apply(ConcertEvent::BookingCancelled {
        seat: SeatId::parse("A1").unwrap(),
        fan_id: fan("crowd-0"),
        booking_id: Uuid::new_v4(),
        cancellation_id: Uuid::new_v4(),
        refund: 120,
    });
    assert!(!concert.is_sold_out());
}

#[test]
fn r5_rejects_taken_seat() {
    TestFramework::<Concert>::with(services())
        .given(vec![announced(), booked("B4", "fox", "Fox", Uuid::new_v4())])
        .when(book("B4", "panda", "Panda", Uuid::new_v4()))
        .then_expect_error(ConcertError::SeatTaken {
            seat: SeatId::parse("B4").unwrap(),
            by: "Fox".into(),
        });
}

#[test]
fn r6_rejects_the_fifth_ticket_and_the_limit_comes_from_services() {
    let history = vec![
        announced(),
        booked("A1", "arbitrage-bot", "Arbitrage Bot", Uuid::new_v4()),
        booked("A2", "arbitrage-bot", "Arbitrage Bot", Uuid::new_v4()),
        booked("A3", "arbitrage-bot", "Arbitrage Bot", Uuid::new_v4()),
        booked("A4", "arbitrage-bot", "Arbitrage Bot", Uuid::new_v4()),
    ];
    TestFramework::<Concert>::with(services())
        .given(history.clone())
        .when(book("A5", "arbitrage-bot", "Arbitrage Bot", Uuid::new_v4()))
        .then_expect_error(ConcertError::TicketLimit { limit: 4 });
    // Another fan is unaffected.
    assert!(
        TestFramework::<Concert>::with(services())
            .given(history.clone())
            .when(book("A5", "fox", "Fox", Uuid::new_v4()))
            .inspect_result()
            .is_ok()
    );
    // A looser limit from the services accepts the fifth ticket.
    assert!(
        TestFramework::<Concert>::with(ConcertServices { ticket_limit: 5 })
            .given(history)
            .when(book("A5", "arbitrage-bot", "Arbitrage Bot", Uuid::new_v4()))
            .inspect_result()
            .is_ok()
    );
}

#[test]
fn r7_rejects_cancelling_a_free_seat() {
    TestFramework::<Concert>::with(services())
        .given(vec![announced()])
        .when(cancel("B4", "fox", Uuid::new_v4()))
        .then_expect_error(ConcertError::SeatNotBooked {
            seat: SeatId::parse("B4").unwrap(),
        });
}

#[test]
fn r8_rejects_cancelling_somebody_elses_ticket() {
    TestFramework::<Concert>::with(services())
        .given(vec![announced(), booked("B4", "fox", "Fox", Uuid::new_v4())])
        .when(cancel("B4", "panda", Uuid::new_v4()))
        .then_expect_error(ConcertError::NotYourTicket);
}

#[test]
fn cancelled_seat_can_be_booked_again() {
    let booking_id = Uuid::new_v4();
    let history = vec![
        announced(),
        booked("B4", "fox", "Fox", booking_id),
        ConcertEvent::BookingCancelled {
            seat: SeatId::parse("B4").unwrap(),
            fan_id: fan("fox"),
            booking_id,
            cancellation_id: Uuid::new_v4(),
            refund: 80,
        },
    ];
    assert!(
        TestFramework::<Concert>::with(services())
            .given(history)
            .when(book("B4", "panda", "Panda", Uuid::new_v4()))
            .inspect_result()
            .is_ok()
    );
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

#[test]
fn error_codes_and_params_match_the_api_contract() {
    assert_eq!(ConcertError::SoldOut.code(), "sold_out");
    assert_eq!(
        ConcertError::SeatTaken {
            seat: SeatId::parse("B4").unwrap(),
            by: "Fox".into()
        }
        .params(),
        serde_json::json!({ "seat": "B4", "by": "Fox" })
    );
    assert_eq!(
        ConcertError::TicketLimit { limit: 4 }.params(),
        serde_json::json!({ "limit": 4 })
    );
    assert_eq!(ConcertError::NotYourTicket.params(), serde_json::json!({}));
}

/// Decision D-02: the outbox repository commits exactly one event per command, so every accepted
/// command must produce exactly one. `TestFramework` does not check this, the runtime would
/// answer 503 — hence this safeguard over representative states.
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
