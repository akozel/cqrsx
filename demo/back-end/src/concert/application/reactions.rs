//! What follows each fact of the box office: the reactions of `Concert`. Two processes start here and continue in
//! the fan's ledger:
//!
//! - ticket delivery: `Concert::SeatBooked` → task `encore_record_ticket` →
//!   [`record_ticket`](crate::fan::application::record_ticket) (`Fan.RecordTicket` → `Fan::TicketRecorded`) → task
//!   `encore_send_e_ticket` → [`send_e_ticket`](crate::fan::application::send_e_ticket) (e-mail "🎫 Your e-ticket");
//! - refund: `Concert::BookingCancelled` → task `encore_record_cancellation` →
//!   [`record_cancellation`](crate::fan::application::record_cancellation) (`Fan.RecordCancellation` →
//!   `Fan::TicketCancelled`) → task `encore_issue_refund` → [`issue_refund`](crate::fan::application::issue_refund)
//!   (e-mail "💸 Refund").
//!
//! [`fan_ledger`] builds the ledger's tasks from the facts: the booking and the refund come from
//! the cancellation the box office decided, so nothing is read before the command. The attempt budget of a task is
//! the policy of its type, never the reaction's.
use crate::concert::{
    application::fan_ledger,
    domain::{Concert, ConcertEvent},
};
use cqrsx::cqrs::reactions::{Decided, Reactions, Then};

/// A booked seat starts the delivery: the ticket goes to the fan's ledger. A cancelled booking starts the refund:
/// the cancellation goes to the fan's ledger. An announcement starts nothing.
impl Reactions for Concert {
    fn react(decided: &Decided<'_, Self>, then: &mut Then) {
        match decided.event {
            ConcertEvent::ConcertAnnounced(_) => {}
            ConcertEvent::SeatBooked(booked) => then.task(fan_ledger::record_ticket(decided.id, booked)),
            ConcertEvent::BookingCancelled(cancelled) => then.task(fan_ledger::record_cancellation(decided.id, cancelled)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        concert::{
            application::test_support::{booked, cancelled_booking, concert_command, concert_id},
            domain::events::ConcertAnnounced,
        },
        fan::application::record_ticket::RecordTicketV1,
        shared::application::test_support::{formats, plan},
    };
    use cqrsx::{cqrs::reactions::ReactionsPlanner, task::format::TaskFormat};
    use serde_json::json;
    use uuid::Uuid;

    /// The planner of the box office's reactions, with the policies of the configuration.
    fn reactions() -> ReactionsPlanner<Concert> {
        ReactionsPlanner::new(crate::testing::policies())
    }

    #[test]
    fn a_booked_seat_plans_the_ticket_for_the_fans_ledger_from_the_fact() {
        let booking_id = Uuid::from_u128(1);
        let tasks = plan::<Concert, _>(&reactions(), &concert_id(), &concert_command(), &[booked(booking_id)]).unwrap();
        let [task] = &tasks[..] else {
            panic!("one task per booking: {tasks:?}");
        };
        assert_eq!(task.format(), &TaskFormat::of::<RecordTicketV1>().unwrap());
        assert_eq!(
            task.payload(),
            &json!({ "booking_id": booking_id, "concert_id": "radiohead-2026-11-21", "seat": "A1", "zone": "vip",
                     "price": 120, "fan_id": "fox", "fan_name": "Fox" })
        );
        assert_eq!(task.metadata(), &json!({}));
        assert_eq!(task.max_attempts(), 6);
    }

    #[test]
    fn a_cancelled_booking_plans_the_cancellation_with_the_price_paid() {
        let (booking_id, cancellation_id) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let tasks = plan::<Concert, _>(
            &reactions(),
            &concert_id(),
            &concert_command(),
            &[cancelled_booking(booking_id, cancellation_id)],
        )
        .unwrap();
        let [task] = &tasks[..] else {
            panic!("one task per cancellation: {tasks:?}");
        };
        assert_eq!(
            task.payload(),
            &json!({ "booking_id": booking_id, "cancellation_id": cancellation_id, "concert_id": "radiohead-2026-11-21",
                     "seat": "A1", "fan_id": "fox", "refund": 120 })
        );
        assert_eq!(task.metadata(), &json!({}));
        assert_eq!(task.max_attempts(), 6);
    }

    #[test]
    fn every_reacting_fact_plans_one_task_through_the_aggregates_planner() {
        let concert = reactions();
        let id = concert_id();
        assert_eq!(
            formats(&plan::<Concert, _>(&concert, &id, &concert_command(), &[booked(Uuid::nil())]).unwrap()),
            ["encore_record_ticket v1"]
        );
        let cancelled = cancelled_booking(Uuid::nil(), Uuid::nil());
        assert_eq!(
            formats(&plan::<Concert, _>(&concert, &id, &concert_command(), &[cancelled]).unwrap()),
            ["encore_record_cancellation v1"]
        );
        let announced = ConcertEvent::ConcertAnnounced(ConcertAnnounced {
            artist: "Radiohead".into(),
            city: "Tbilisi".into(),
            date: chrono::NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
            capacity: 40,
        });
        assert!(
            plan::<Concert, _>(&concert, &id, &concert_command(), &[announced])
                .unwrap()
                .is_empty(),
            "nothing downstream reacts to an announcement"
        );
    }

    /// The delivery plans nothing for a cancellation and the refund nothing for a booking: each fact plans only the
    /// task of its own process. The concert is the decision's typed id, so a foreign one cannot reach the reactions.
    #[test]
    fn each_fact_plans_only_its_own_task() {
        let cancelled = cancelled_booking(Uuid::from_u128(1), Uuid::from_u128(2));
        assert_eq!(
            formats(&plan::<Concert, _>(&reactions(), &concert_id(), &concert_command(), &[cancelled]).unwrap()),
            ["encore_record_cancellation v1"]
        );
        assert_eq!(
            formats(&plan::<Concert, _>(&reactions(), &concert_id(), &concert_command(), &[booked(Uuid::nil())]).unwrap()),
            ["encore_record_ticket v1"]
        );
    }
}
