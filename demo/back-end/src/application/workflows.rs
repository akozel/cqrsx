//! Business processes driven by outbox tasks: one folder per process, one file per step, the task
//! contracts in its `tasks` module, and at its root the process itself, the type that plans every task
//! from the decision it follows.
//!
//! - [`ticket_delivery`]: a booked seat reaches the fan's ledger, then the fan gets an e-ticket;
//! - [`refund`]: a cancelled booking reaches the fan's ledger, then the fan gets a refund.
//!
//! [`concert_tasks`] and [`fan_tasks`] are the planners of the two aggregates' executors: every process
//! that reacts to the aggregate. This module also holds the one step every ledger step performs: metadata
//! from the task, the internal `Fan` command, and the mapping of its outcome to a task result
//! (PLAN.md §2.7). R9/R11 mean "already done" (redelivery), R10 means "too early, retry".
pub mod refund;
#[cfg(test)]
mod test_support;
pub mod ticket_delivery;

use crate::{
    application::{
        command_service::{CommandMetadata, CommandOutcome},
        fan::FanApp,
        workflows::{refund::Refund, ticket_delivery::TicketDelivery},
    },
    domain::{
        ConcertId, FanId, FanLedgerId,
        concert::Concert,
        fan::{Fan, FanCommand, FanError},
    },
};
use cqrsx::{
    cqrs::command::{CommandExecutor, planner::TaskPlanner},
    task::{attempt::TaskAttempt, handler::TaskHandlerError},
};
use std::time::Duration;

pub const CHAIN_RETRY_AFTER: Duration = Duration::from_secs(1);

/// Every process that reacts to the box office: the planner of the `Concert` executor. `max_attempts`
/// is the attempt budget of the tasks they plan.
pub fn concert_tasks(max_attempts: i32) -> impl TaskPlanner<Concert> {
    (TicketDelivery { max_attempts }, Refund { max_attempts })
}

/// Every process that continues in a fan's ledger: the planner of the `Fan` executor.
pub fn fan_tasks(max_attempts: i32) -> impl TaskPlanner<Fan> {
    (TicketDelivery { max_attempts }, Refund { max_attempts })
}

pub fn map_fan_outcome(outcome: CommandOutcome<FanError>) -> Result<(), TaskHandlerError> {
    let retry = |reason: &str| {
        Err(TaskHandlerError::Retry {
            reason: reason.to_owned(),
            retry_after: Some(CHAIN_RETRY_AFTER),
        })
    };
    match outcome {
        CommandOutcome::Accepted => Ok(()),
        CommandOutcome::Rejected(FanError::AlreadyRecorded { .. } | FanError::AlreadyCancelled { .. }) => Ok(()),
        CommandOutcome::Rejected(FanError::UnknownTicket { .. }) => retry("unknown_ticket"),
        CommandOutcome::Conflict => retry("conflict"),
        CommandOutcome::Unavailable => retry("unavailable"),
    }
}

/// One link of the aggregate chain: the `Fan` command a task asks for. What follows it is planned by the
/// processes from the ledger's decision.
pub struct ChainStep<'a> {
    pub concert_id: &'a ConcertId,
    pub fan_id: &'a FanId,
    pub command: FanCommand,
}

/// Runs one chain step on behalf of `attempt`: the command continues the task's causation chain
/// (cqrsx `Causation::from_task`) and runs on the fan's ledger for the concert (D-14). A malformed
/// payload never reaches this point: the JSON adapter rejects it permanently, ids included.
pub async fn run_chain_step<E: CommandExecutor<Fan>>(
    fans: &FanApp<E>,
    attempt: &TaskAttempt,
    step: ChainStep<'_>,
) -> Result<(), TaskHandlerError> {
    let metadata = CommandMetadata::from_task(attempt);
    let ledger = FanLedgerId::new(step.fan_id, step.concert_id);
    let executed = fans.execute(&ledger, step.command, metadata).await;
    map_fan_outcome(executed.outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::workflows::test_support::{booked, cancelled_booking, concert_command, fan_command, plan, recorded};
    use crate::domain::concert::{ConcertEvent, events::ConcertAnnounced};
    use uuid::Uuid;

    #[test]
    fn outcomes_map_to_task_results_as_in_the_plan_table() {
        let id = Uuid::new_v4();
        assert!(map_fan_outcome(CommandOutcome::Accepted).is_ok());
        assert!(map_fan_outcome(CommandOutcome::Rejected(FanError::AlreadyRecorded { booking_id: id })).is_ok());
        assert!(map_fan_outcome(CommandOutcome::Rejected(FanError::AlreadyCancelled { booking_id: id })).is_ok());
        for (outcome, reason) in [
            (
                CommandOutcome::Rejected(FanError::UnknownTicket { booking_id: id }),
                "unknown_ticket",
            ),
            (CommandOutcome::Conflict, "conflict"),
            (CommandOutcome::Unavailable, "unavailable"),
        ] {
            match map_fan_outcome(outcome) {
                Err(TaskHandlerError::Retry { reason: r, retry_after }) => {
                    assert_eq!(r, reason);
                    assert_eq!(retry_after, Some(CHAIN_RETRY_AFTER));
                }
                other => panic!("expected retry for {reason}, got {other:?}"),
            }
        }
    }

    fn formats(tasks: &[cqrsx::task::NewTask]) -> Vec<String> {
        tasks.iter().map(|task| task.format().to_string()).collect()
    }

    #[test]
    fn every_reacting_fact_plans_one_task_through_the_aggregates_planner() {
        let concert = concert_tasks(6);
        let id = "radiohead-2026-11-21";
        assert_eq!(
            formats(&plan::<Concert, _>(&concert, id, &concert_command(), &[booked(Uuid::nil())]).unwrap()),
            ["encore_record_ticket v1"]
        );
        let cancelled = cancelled_booking(Uuid::nil(), Uuid::nil());
        assert_eq!(
            formats(&plan::<Concert, _>(&concert, id, &concert_command(), &[cancelled]).unwrap()),
            ["encore_record_cancellation v1"]
        );
        let announced = ConcertEvent::ConcertAnnounced(ConcertAnnounced {
            artist: "Radiohead".into(),
            city: "Tbilisi".into(),
            date: chrono::NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
            capacity: 40,
        });
        assert!(
            plan::<Concert, _>(&concert, id, &concert_command(), &[announced])
                .unwrap()
                .is_empty(),
            "nothing downstream reacts to an announcement"
        );
        let fan = fan_tasks(6);
        assert_eq!(
            formats(&plan::<Fan, _>(&fan, "fox:radiohead-2026-11-21", &fan_command(), &[recorded(Uuid::nil())]).unwrap()),
            ["encore_send_e_ticket v1"]
        );
    }
}
