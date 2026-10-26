//! Ticket delivery: `Concert::SeatBooked` → task `encore_record_ticket` →
//! [`record_ticket`] (`Fan.RecordTicket` → `Fan::TicketRecorded`) → task `encore_send_e_ticket` →
//! [`send_e_ticket`] (e-mail "🎫 Your e-ticket").
//!
//! [`TicketDelivery`] is the process in one place: it plans each task from the decision it follows, the
//! box office's and then the fan ledger's; the steps run the tasks.
pub mod record_ticket;
pub mod send_e_ticket;
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

/// The ticket delivery process; `max_attempts` is the attempt budget of the tasks it plans.
#[derive(Debug, Clone, Copy)]
pub struct TicketDelivery {
    pub max_attempts: i32,
}

/// A booked seat starts the delivery: the ticket goes to the fan's ledger.
impl TaskPlanner<Concert> for TicketDelivery {
    fn plan(&self, decision: &Decision<'_, Concert>) -> Result<Vec<NewTask>, TaskPlanError> {
        let mut planned = Vec::new();
        for event in decision.events {
            if let ConcertEvent::SeatBooked(booked) = event {
                let concert_id = ConcertId::parse(decision.aggregate_id).map_err(TaskPlanError::application)?;
                planned.push(tasks::record_ticket(&concert_id, booked, self.max_attempts)?);
            }
        }
        Ok(planned)
    }
}

/// A ticket in the fan's ledger is mailed.
impl TaskPlanner<Fan> for TicketDelivery {
    fn plan(&self, decision: &Decision<'_, Fan>) -> Result<Vec<NewTask>, TaskPlanError> {
        let mut planned = Vec::new();
        for event in decision.events {
            if let FanEvent::TicketRecorded(recorded) = event {
                let ledger = FanLedgerId::parse(decision.aggregate_id).map_err(TaskPlanError::application)?;
                planned.push(tasks::send_e_ticket(&ledger, recorded, self.max_attempts)?);
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
        domain::fan::{FanEvent, events::TicketCancelled},
    };
    use cqrsx::task::format::TaskFormat;
    use serde_json::json;
    use uuid::Uuid;

    const DELIVERY: TicketDelivery = TicketDelivery { max_attempts: 6 };

    #[test]
    fn a_booked_seat_plans_the_ticket_for_the_fans_ledger_from_the_fact() {
        let booking_id = Uuid::from_u128(1);
        let tasks = plan::<Concert, _>(&DELIVERY, "radiohead-2026-11-21", &concert_command(), &[booked(booking_id)]).unwrap();
        let [task] = &tasks[..] else {
            panic!("one task per booking: {tasks:?}");
        };
        assert_eq!(task.format(), &TaskFormat::of::<tasks::RecordTicketV1>().unwrap());
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
    fn a_ticket_in_the_ledger_plans_its_e_ticket_for_the_ledgers_fan() {
        let booking_id = Uuid::from_u128(1);
        let tasks = plan::<Fan, _>(&DELIVERY, "fox:radiohead-2026-11-21", &fan_command(), &[recorded(booking_id)]).unwrap();
        let [task] = &tasks[..] else {
            panic!("one e-ticket per recorded ticket: {tasks:?}");
        };
        assert_eq!(
            task.payload(),
            &json!({ "booking_id": booking_id, "concert_id": "radiohead-2026-11-21", "seat": "B4", "fan_id": "fox",
                     "fan_name": "Fox", "price": 80 })
        );
        assert_eq!(
            task.metadata(),
            &json!({ "concert_id": "radiohead-2026-11-21", "fan_id": "fox" })
        );
    }

    #[test]
    fn the_other_facts_plan_nothing_and_a_foreign_aggregate_id_is_refused() {
        let cancelled = cancelled_booking(Uuid::from_u128(1), Uuid::from_u128(2));
        assert!(
            plan::<Concert, _>(&DELIVERY, "radiohead-2026-11-21", &concert_command(), &[cancelled])
                .unwrap()
                .is_empty()
        );
        let ticket_cancelled = FanEvent::TicketCancelled(TicketCancelled {
            booking_id: Uuid::from_u128(1),
            cancellation_id: Uuid::from_u128(2),
            concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
            seat: crate::domain::shared::venue::SeatId::parse("B4").unwrap(),
            refund: 80,
        });
        assert!(
            plan::<Fan, _>(&DELIVERY, "fox:radiohead-2026-11-21", &fan_command(), &[ticket_cancelled])
                .unwrap()
                .is_empty()
        );
        assert!(matches!(
            plan::<Concert, _>(&DELIVERY, "Not A Concert", &concert_command(), &[booked(Uuid::nil())]),
            Err(TaskPlanError::Application(_))
        ));
        assert!(matches!(
            plan::<Fan, _>(&DELIVERY, "no-ledger", &fan_command(), &[recorded(Uuid::nil())]),
            Err(TaskPlanError::Application(_))
        ));
    }
}
