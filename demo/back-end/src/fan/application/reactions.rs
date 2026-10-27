//! What follows each fact of a fan's ledger: the reactions of `Fan`. They continue the two processes the box office
//! starts: a ticket recorded in the ledger is mailed ([`SendETicketV1`]), a ticket cancelled in the ledger is
//! refunded ([`IssueRefundV1`]).
use crate::fan::{
    application::{issue_refund::IssueRefundV1, send_e_ticket::SendETicketV1},
    domain::{Fan, FanEvent},
};
use cqrsx::cqrs::reactions::{Decided, Reactions, Then};

/// A ticket in the fan's ledger is mailed; a ticket cancelled in the fan's ledger is refunded. The fan is the
/// ledger's.
impl Reactions for Fan {
    fn react(decided: &Decided<'_, Self>, then: &mut Then) {
        match decided.event {
            FanEvent::TicketRecorded(recorded) => then.task(SendETicketV1::of(decided.id, recorded)),
            FanEvent::TicketCancelled(cancelled) => then.task(IssueRefundV1::of(decided.id, cancelled)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fan::{
            application::test_support::{fan_command, ledger, recorded},
            domain::events::TicketCancelled,
        },
        shared::{
            application::test_support::{formats, plan},
            domain::{ConcertId, venue::SeatId},
        },
    };
    use cqrsx::cqrs::reactions::ReactionsPlanner;
    use serde_json::json;
    use uuid::Uuid;

    /// The planner of the ledger's reactions, with the policies of the configuration.
    fn reactions() -> ReactionsPlanner<Fan> {
        ReactionsPlanner::new(crate::testing::policies())
    }

    #[test]
    fn a_ticket_in_the_ledger_plans_its_e_ticket_for_the_ledgers_fan() {
        let booking_id = Uuid::from_u128(1);
        let tasks = plan::<Fan, _>(&reactions(), &ledger(), &fan_command(), &[recorded(booking_id)]).unwrap();
        let [task] = &tasks[..] else {
            panic!("one e-ticket per recorded ticket: {tasks:?}");
        };
        assert_eq!(
            task.payload(),
            &json!({ "booking_id": booking_id, "concert_id": "radiohead-2026-11-21", "seat": "B4", "fan_id": "fox",
                     "fan_name": "Fox", "price": 80 })
        );
        assert_eq!(task.metadata(), &json!({}));
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
        let tasks = plan::<Fan, _>(&reactions(), &ledger(), &fan_command(), &[cancelled]).unwrap();
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
        let fan = reactions();
        assert_eq!(
            formats(&plan::<Fan, _>(&fan, &ledger(), &fan_command(), &[recorded(Uuid::nil())]).unwrap()),
            ["encore_send_e_ticket v1"]
        );
    }

    /// The delivery plans nothing for a cancelled ticket and the refund nothing for a recorded one: each fact plans
    /// only the task of its own process. The ledger is the decision's typed id, so a foreign one cannot reach the
    /// reactions.
    #[test]
    fn each_fact_plans_only_its_own_task() {
        let ticket_cancelled = FanEvent::TicketCancelled(TicketCancelled {
            booking_id: Uuid::from_u128(1),
            cancellation_id: Uuid::from_u128(2),
            concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
            seat: SeatId::parse("B4").unwrap(),
            refund: 80,
        });
        assert_eq!(
            formats(&plan::<Fan, _>(&reactions(), &ledger(), &fan_command(), &[ticket_cancelled]).unwrap()),
            ["encore_issue_refund v1"]
        );
        assert_eq!(
            formats(&plan::<Fan, _>(&reactions(), &ledger(), &fan_command(), &[recorded(Uuid::nil())]).unwrap()),
            ["encore_send_e_ticket v1"]
        );
    }

    /// The ledger through a test engine, as the chain steps run it: what the activity feed says of its commands.
    mod engine {
        use crate::{
            fan::{
                application::test_support::{fan_command, ledger, ticket},
                domain::{Fan, FanCommand, commands::RecordCancellation},
            },
            shared::{
                application::{activity::ActivityHub, metadata::CommandMetadata},
                domain::{ConcertId, FanId, FanLedgerId},
            },
        };
        use cqrsx::cqrs::{
            command::{CommandExecutor, CommandGatewayError},
            testing::{command::TestCommandExecutor, mem_store::MemStore, unavailable_store::UnavailableStore},
        };
        use std::sync::Arc;
        use uuid::Uuid;

        #[tokio::test]
        async fn an_unreachable_ledger_is_unavailable_and_logged() {
            let activity = Arc::new(ActivityHub::new(8, 8));
            let fans =
                TestCommandExecutor::<Fan, _>::new(UnavailableStore, ()).with_observer(crate::testing::command_feed(&activity));
            let command = FanCommand::RecordCancellation(RecordCancellation {
                booking_id: Uuid::now_v7(),
                cancellation_id: Uuid::now_v7(),
                refund: 80,
            });
            let ledger = FanLedgerId::new(
                &FanId::parse("fox").unwrap(),
                &ConcertId::parse("radiohead-2026-11-21").unwrap(),
            );
            let executed = fans
                .execute_with_metadata(&ledger, command, CommandMetadata::ui().to_map())
                .await;
            assert_eq!(executed, Err(CommandGatewayError::Unavailable));
            let (entries, _) = activity.subscribe();
            assert_eq!(entries.len(), 1);
            assert_eq!(
                (entries[0].kind.as_str(), entries[0].fields["fan_id"].as_str()),
                ("command.failed", Some("fox"))
            );
        }

        /// What the activity feed names every command of a ledger by: its type and the ledger's fan.
        #[tokio::test]
        async fn every_ledger_command_is_logged_with_its_type_and_the_ledgers_fan() {
            let activity = Arc::new(ActivityHub::new(8, 8));
            let fans =
                TestCommandExecutor::<Fan>::new(MemStore::default(), ()).with_observer(crate::testing::command_feed(&activity));
            fans.execute_with_metadata(&ledger(), fan_command(), CommandMetadata::ui().to_map())
                .await
                .unwrap();
            let cancellation = FanCommand::RecordCancellation(RecordCancellation {
                booking_id: ticket().booking_id,
                cancellation_id: Uuid::now_v7(),
                refund: 80,
            });
            fans.execute_with_metadata(&ledger(), cancellation, CommandMetadata::ui().to_map())
                .await
                .unwrap();
            let (entries, _) = activity.subscribe();
            let logged: Vec<_> = entries
                .iter()
                .map(|entry| {
                    (
                        entry.kind.as_str(),
                        entry.fields["command_type"].as_str(),
                        entry.fields["fan_id"].as_str(),
                    )
                })
                .collect();
            assert_eq!(
                logged,
                [
                    ("command.accepted", Some("RecordTicket"), Some("fox")),
                    ("command.accepted", Some("RecordCancellation"), Some("fox")),
                ]
            );
        }
    }
}
