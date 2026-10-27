//! What follows each fact of the box office: the planner of the `Concert` executor. Two processes start here and
//! continue in the fan's ledger:
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
//! the cancellation the box office decided, so nothing is read before the command.
use crate::concert::{
    application::fan_ledger,
    domain::{Concert, ConcertEvent},
};
use cqrsx::{
    cqrs::command::planner::{Decision, TaskPlanError, TaskPlanner},
    task::NewTask,
};

/// What follows each fact of the box office; `max_attempts` is the attempt budget of the tasks it plans.
#[derive(Debug, Clone, Copy)]
pub struct ConcertReactions {
    pub max_attempts: i32,
}

/// The planner of the `Concert` executor; `max_attempts` is the attempt budget of the tasks it plans.
pub fn concert_tasks(max_attempts: i32) -> impl TaskPlanner<Concert> {
    ConcertReactions { max_attempts }
}

/// A booked seat starts the delivery: the ticket goes to the fan's ledger. A cancelled booking starts the refund:
/// the cancellation goes to the fan's ledger. The delivery's tasks come first, the refund's after them.
impl TaskPlanner<Concert> for ConcertReactions {
    fn plan(&self, decision: &Decision<'_, Concert>) -> Result<Vec<NewTask>, TaskPlanError> {
        let mut planned = Vec::new();
        for event in decision.events {
            if let ConcertEvent::SeatBooked(booked) = event {
                planned.push(fan_ledger::record_ticket(decision.aggregate_id, booked, self.max_attempts)?);
            }
        }
        for event in decision.events {
            if let ConcertEvent::BookingCancelled(cancelled) = event {
                planned.push(fan_ledger::record_cancellation(
                    decision.aggregate_id,
                    cancelled,
                    self.max_attempts,
                )?);
            }
        }
        Ok(planned)
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
    use cqrsx::task::format::TaskFormat;
    use serde_json::json;
    use uuid::Uuid;

    const REACTIONS: ConcertReactions = ConcertReactions { max_attempts: 6 };

    #[test]
    fn a_booked_seat_plans_the_ticket_for_the_fans_ledger_from_the_fact() {
        let booking_id = Uuid::from_u128(1);
        let tasks = plan::<Concert, _>(&REACTIONS, &concert_id(), &concert_command(), &[booked(booking_id)]).unwrap();
        let [task] = &tasks[..] else {
            panic!("one task per booking: {tasks:?}");
        };
        assert_eq!(task.format(), &TaskFormat::of::<RecordTicketV1>().unwrap());
        assert_eq!(
            task.payload(),
            &json!({ "booking_id": booking_id, "concert_id": "radiohead-2026-11-21", "seat": "A1", "zone": "vip",
                     "price": 120, "fan_id": "fox", "fan_name": "Fox" })
        );
        assert_eq!(
            task.metadata(),
            &json!({ "concert_id": "radiohead-2026-11-21", "fan_id": "fox" })
        );
        assert_eq!(task.max_attempts(), 6);
    }

    #[test]
    fn a_cancelled_booking_plans_the_cancellation_with_the_price_paid() {
        let (booking_id, cancellation_id) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let tasks = plan::<Concert, _>(
            &REACTIONS,
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
        assert_eq!(
            task.metadata(),
            &json!({ "concert_id": "radiohead-2026-11-21", "fan_id": "fox" })
        );
        assert_eq!(task.max_attempts(), 6);
    }

    #[test]
    fn every_reacting_fact_plans_one_task_through_the_aggregates_planner() {
        let concert = concert_tasks(6);
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
    /// task of its own process. The concert is the decision's typed id, so a foreign one cannot reach the planner.
    #[test]
    fn each_fact_plans_only_its_own_task() {
        let cancelled = cancelled_booking(Uuid::from_u128(1), Uuid::from_u128(2));
        assert_eq!(
            formats(&plan::<Concert, _>(&REACTIONS, &concert_id(), &concert_command(), &[cancelled]).unwrap()),
            ["encore_record_cancellation v1"]
        );
        assert_eq!(
            formats(&plan::<Concert, _>(&REACTIONS, &concert_id(), &concert_command(), &[booked(Uuid::nil())]).unwrap()),
            ["encore_record_ticket v1"]
        );
    }
}
