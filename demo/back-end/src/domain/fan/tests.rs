use super::*;
use crate::domain::shared::venue::{SeatId, Zone};
use cqrsx::cqrs::{aggregate::Aggregate, testing::TestFramework};
use uuid::Uuid;

fn seat(raw: &str) -> SeatId {
    SeatId::parse(raw).unwrap()
}

fn record(booking_id: Uuid) -> FanCommand {
    FanCommand::RecordTicket {
        booking_id,
        concert_id: "radiohead-2026-11-21".into(),
        seat: seat("B4"),
        zone: Zone::Standard,
        price: 80,
        fan_name: "Fox".into(),
    }
}

fn recorded(booking_id: Uuid) -> FanEvent {
    FanEvent::TicketRecorded {
        booking_id,
        concert_id: "radiohead-2026-11-21".into(),
        seat: seat("B4"),
        zone: Zone::Standard,
        price: 80,
        fan_name: "Fox".into(),
    }
}

fn cancel(booking_id: Uuid, cancellation_id: Uuid) -> FanCommand {
    FanCommand::RecordCancellation {
        booking_id,
        cancellation_id,
        refund: 80,
    }
}

fn cancelled(booking_id: Uuid, cancellation_id: Uuid) -> FanEvent {
    FanEvent::TicketCancelled {
        booking_id,
        cancellation_id,
        concert_id: "radiohead-2026-11-21".into(),
        seat: seat("B4"),
        refund: 80,
    }
}

#[test]
fn record_ticket_happy_path() {
    let id = Uuid::new_v4();
    TestFramework::<Fan>::with(())
        .given_no_previous_events()
        .when(record(id))
        .then_expect_events(vec![recorded(id)]);
}

#[test]
fn record_cancellation_happy_path_copies_concert_and_seat_from_the_ticket() {
    let id = Uuid::new_v4();
    let cancellation_id = Uuid::new_v4();
    TestFramework::<Fan>::with(())
        .given(vec![recorded(id)])
        .when(cancel(id, cancellation_id))
        .then_expect_events(vec![cancelled(id, cancellation_id)]);
}

#[test]
fn r9_rejects_a_repeated_record_ticket() {
    let id = Uuid::new_v4();
    TestFramework::<Fan>::with(())
        .given(vec![recorded(id)])
        .when(record(id))
        .then_expect_error(FanError::AlreadyRecorded { booking_id: id });
}

#[test]
fn r10_rejects_cancellation_of_unknown_ticket() {
    let id = Uuid::new_v4();
    TestFramework::<Fan>::with(())
        .given_no_previous_events()
        .when(cancel(id, Uuid::new_v4()))
        .then_expect_error(FanError::UnknownTicket { booking_id: id });
}

#[test]
fn r11_rejects_a_repeated_cancellation() {
    let id = Uuid::new_v4();
    let cancellation_id = Uuid::new_v4();
    TestFramework::<Fan>::with(())
        .given(vec![recorded(id), cancelled(id, cancellation_id)])
        .when(cancel(id, cancellation_id))
        .then_expect_error(FanError::AlreadyCancelled { booking_id: id });
}

#[test]
fn derived_counters_ignore_cancelled_tickets_and_name_comes_from_the_first_ticket() {
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let mut fan = Fan::default();
    fan.apply(recorded(first));
    fan.apply(FanEvent::TicketRecorded {
        booking_id: second,
        concert_id: "rammstein-2026-12-05".into(),
        seat: seat("A1"),
        zone: Zone::Vip,
        price: 120,
        fan_name: "Fox (renamed)".into(),
    });
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
