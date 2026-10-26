//! Refund: `Concert::BookingCancelled` → task `encore_record_cancellation` →
//! [`record_cancellation`] (`Fan.RecordCancellation` → `Fan::TicketCancelled`) → task
//! `encore_issue_refund` → [`issue_refund`] (e-mail "💸 Refund").
//!
//! [`Refund`] is the process in one place: it plans each task from the decision it follows, the box
//! office's and then the fan ledger's; the steps run the tasks. The booking and the refund come from the
//! cancellation the box office decided, so nothing is read before the command.
pub mod issue_refund;
pub mod record_cancellation;
pub mod tasks;

use crate::domain::{
    ConcertId, FanLedgerId,
    concert::{Concert, ConcertEvent},
    fan::{Fan, FanEvent},
};
use cqrsx::{
    cqrs::command::planner::{Decision, TaskPlanError, TaskPlanner},
    task::NewTask,
};

/// The refund process; `max_attempts` is the attempt budget of the tasks it plans.
#[derive(Debug, Clone, Copy)]
pub struct Refund {
    pub max_attempts: i32,
}

/// A cancelled booking starts the refund: the cancellation goes to the fan's ledger.
impl TaskPlanner<Concert> for Refund {
    fn plan(&self, decision: &Decision<'_, Concert>) -> Result<Vec<NewTask>, TaskPlanError> {
        let mut planned = Vec::new();
        for event in decision.events {
            if let ConcertEvent::BookingCancelled(cancelled) = event {
                let concert_id = ConcertId::parse(decision.aggregate_id).map_err(TaskPlanError::application)?;
                planned.push(tasks::record_cancellation(&concert_id, cancelled, self.max_attempts)?);
            }
        }
        Ok(planned)
    }
}

/// A ticket cancelled in the fan's ledger is refunded.
impl TaskPlanner<Fan> for Refund {
    fn plan(&self, decision: &Decision<'_, Fan>) -> Result<Vec<NewTask>, TaskPlanError> {
        let mut planned = Vec::new();
        for event in decision.events {
            if let FanEvent::TicketCancelled(cancelled) = event {
                let ledger = FanLedgerId::parse(decision.aggregate_id).map_err(TaskPlanError::application)?;
                planned.push(tasks::issue_refund(&ledger, cancelled, self.max_attempts)?);
            }
        }
        Ok(planned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        application::workflows::test_support::{booked, cancelled_booking, concert_command, fan_command, plan, recorded},
        domain::{fan::events::TicketCancelled, shared::venue::SeatId},
    };
    use serde_json::json;
    use uuid::Uuid;

    const REFUND: Refund = Refund { max_attempts: 6 };

    #[test]
    fn a_cancelled_booking_plans_the_cancellation_with_the_price_paid() {
        let (booking_id, cancellation_id) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let tasks = plan::<Concert, _>(
            &REFUND,
            "radiohead-2026-11-21",
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
    fn a_ticket_cancelled_in_the_ledger_plans_the_refund_of_the_ledgers_fan() {
        let (booking_id, cancellation_id) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let cancelled = FanEvent::TicketCancelled(TicketCancelled {
            booking_id,
            cancellation_id,
            concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
            seat: SeatId::parse("B4").unwrap(),
            refund: 80,
        });
        let tasks = plan::<Fan, _>(&REFUND, "fox:radiohead-2026-11-21", &fan_command(), &[cancelled]).unwrap();
        assert_eq!(
            tasks.iter().map(|task| task.payload().clone()).collect::<Vec<_>>(),
            [
                json!({ "cancellation_id": cancellation_id, "booking_id": booking_id, "concert_id": "radiohead-2026-11-21",
                     "seat": "B4", "fan_id": "fox", "amount": 80 })
            ]
        );
    }

    #[test]
    fn the_other_facts_plan_nothing_and_a_foreign_aggregate_id_is_refused() {
        assert!(
            plan::<Concert, _>(&REFUND, "radiohead-2026-11-21", &concert_command(), &[booked(Uuid::nil())])
                .unwrap()
                .is_empty()
        );
        assert!(
            plan::<Fan, _>(&REFUND, "fox:radiohead-2026-11-21", &fan_command(), &[recorded(Uuid::nil())])
                .unwrap()
                .is_empty()
        );
        let cancelled = cancelled_booking(Uuid::nil(), Uuid::nil());
        assert!(matches!(
            plan::<Concert, _>(&REFUND, "Not A Concert", &concert_command(), &[cancelled]),
            Err(TaskPlanError::Application(_))
        ));
        let ticket_cancelled = FanEvent::TicketCancelled(TicketCancelled {
            booking_id: Uuid::nil(),
            cancellation_id: Uuid::nil(),
            concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
            seat: SeatId::parse("B4").unwrap(),
            refund: 80,
        });
        assert!(matches!(
            plan::<Fan, _>(&REFUND, "no-ledger", &fan_command(), &[ticket_cancelled]),
            Err(TaskPlanError::Application(_))
        ));
    }
}
