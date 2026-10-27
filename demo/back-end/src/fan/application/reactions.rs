//! What follows each fact of a fan's ledger: the planner of the `Fan` executor. It continues the two processes the
//! box office starts: a ticket recorded in the ledger is mailed ([`send_e_ticket`]), a
//! ticket cancelled in the ledger is refunded ([`issue_refund`]).
use crate::{
    fan::{
        application::{issue_refund, send_e_ticket},
        domain::{Fan, FanEvent},
    },
    shared::domain::FanLedgerId,
};
use cqrsx::{
    cqrs::command::planner::{Decision, TaskPlanError, TaskPlanner},
    task::NewTask,
};

/// What follows each fact of a fan's ledger; `max_attempts` is the attempt budget of the tasks it plans.
#[derive(Debug, Clone, Copy)]
pub struct FanReactions {
    pub max_attempts: i32,
}

/// The planner of the `Fan` executor; `max_attempts` is the attempt budget of the tasks it plans.
pub fn fan_tasks(max_attempts: i32) -> impl TaskPlanner<Fan> {
    FanReactions { max_attempts }
}

/// A ticket in the fan's ledger is mailed; a ticket cancelled in the fan's ledger is refunded. The delivery's
/// tasks come first, the refund's after them.
impl TaskPlanner<Fan> for FanReactions {
    fn plan(&self, decision: &Decision<'_, Fan>) -> Result<Vec<NewTask>, TaskPlanError> {
        let mut planned = Vec::new();
        for event in decision.events {
            if let FanEvent::TicketRecorded(recorded) = event {
                let ledger = FanLedgerId::parse(decision.aggregate_id).map_err(TaskPlanError::application)?;
                planned.push(send_e_ticket::send_e_ticket(&ledger, recorded, self.max_attempts)?);
            }
        }
        for event in decision.events {
            if let FanEvent::TicketCancelled(cancelled) = event {
                let ledger = FanLedgerId::parse(decision.aggregate_id).map_err(TaskPlanError::application)?;
                planned.push(issue_refund::issue_refund(&ledger, cancelled, self.max_attempts)?);
            }
        }
        Ok(planned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fan::{
            application::test_support::{fan_command, recorded},
            domain::events::TicketCancelled,
        },
        shared::{
            application::test_support::{formats, plan},
            domain::{ConcertId, venue::SeatId},
        },
    };
    use serde_json::json;
    use uuid::Uuid;

    const REACTIONS: FanReactions = FanReactions { max_attempts: 6 };

    #[test]
    fn a_ticket_in_the_ledger_plans_its_e_ticket_for_the_ledgers_fan() {
        let booking_id = Uuid::from_u128(1);
        let tasks = plan::<Fan, _>(
            &REACTIONS,
            "fox:radiohead-2026-11-21",
            &fan_command(),
            &[recorded(booking_id)],
        )
        .unwrap();
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
    fn a_ticket_cancelled_in_the_ledger_plans_the_refund_of_the_ledgers_fan() {
        let (booking_id, cancellation_id) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let cancelled = FanEvent::TicketCancelled(TicketCancelled {
            booking_id,
            cancellation_id,
            concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
            seat: SeatId::parse("B4").unwrap(),
            refund: 80,
        });
        let tasks = plan::<Fan, _>(&REACTIONS, "fox:radiohead-2026-11-21", &fan_command(), &[cancelled]).unwrap();
        assert_eq!(
            tasks.iter().map(|task| task.payload().clone()).collect::<Vec<_>>(),
            [
                json!({ "cancellation_id": cancellation_id, "booking_id": booking_id, "concert_id": "radiohead-2026-11-21",
                     "seat": "B4", "fan_id": "fox", "amount": 80 })
            ]
        );
    }

    #[test]
    fn every_reacting_fact_plans_one_task_through_the_aggregates_planner() {
        let fan = fan_tasks(6);
        assert_eq!(
            formats(&plan::<Fan, _>(&fan, "fox:radiohead-2026-11-21", &fan_command(), &[recorded(Uuid::nil())]).unwrap()),
            ["encore_send_e_ticket v1"]
        );
    }

    /// The delivery plans nothing for a cancelled ticket and the refund nothing for a recorded one: each fact plans
    /// only the task of its own process.
    #[test]
    fn each_fact_plans_only_its_own_task_and_a_foreign_aggregate_id_is_refused() {
        let ticket_cancelled = FanEvent::TicketCancelled(TicketCancelled {
            booking_id: Uuid::from_u128(1),
            cancellation_id: Uuid::from_u128(2),
            concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
            seat: SeatId::parse("B4").unwrap(),
            refund: 80,
        });
        assert_eq!(
            formats(&plan::<Fan, _>(&REACTIONS, "fox:radiohead-2026-11-21", &fan_command(), &[ticket_cancelled]).unwrap()),
            ["encore_issue_refund v1"]
        );
        assert_eq!(
            formats(
                &plan::<Fan, _>(
                    &REACTIONS,
                    "fox:radiohead-2026-11-21",
                    &fan_command(),
                    &[recorded(Uuid::nil())]
                )
                .unwrap()
            ),
            ["encore_send_e_ticket v1"]
        );
        assert!(matches!(
            plan::<Fan, _>(&REACTIONS, "no-ledger", &fan_command(), &[recorded(Uuid::nil())]),
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
            plan::<Fan, _>(&REACTIONS, "no-ledger", &fan_command(), &[ticket_cancelled]),
            Err(TaskPlanError::Application(_))
        ));
    }
}
