//! The flow of the fan's ticket ledgers (`Fan` aggregate, one stream per fan and concert, D-14): the tasks the
//! ledger runs, each with its contract and its handler, a function, in one file (the ledger's steps
//! [`record_ticket`] and [`record_cancellation`], the e-mails [`send_e_ticket`] and [`issue_refund`]), what follows
//! each of its facts ([`reactions`]), the read model and the wallet assembled from it. Task names are `[a-z0-9_]` only
//! (decision D-05): the library's diagnostics print other names as `unknown`.
pub mod issue_refund;
pub mod reactions;
pub mod record_cancellation;
pub mod record_ticket;
pub mod send_e_ticket;
#[cfg(test)]
mod test_support;
pub mod views;
pub mod wallet;

#[cfg(test)]
mod tests {
    use super::{
        record_cancellation::{RecordCancellationV1, record_cancellation},
        record_ticket::record_ticket,
        test_support::{attempt, steps, ticket},
    };
    use crate::fan::domain::{Fan, FanCommand, FanError};
    use cqrsx::{
        cqrs::{
            CommitReport,
            command::{CommandExecutor, CommandGatewayError},
        },
        task::result::TaskResult,
    };
    use std::{collections::HashMap, sync::Arc};
    use uuid::Uuid;

    type Answer = Result<CommitReport<Fan>, CommandGatewayError<FanError>>;

    /// A row of the plan table: the ledger's answer by name, the answer, the task's result.
    type Row = (&'static str, fn() -> Answer, TaskResult);

    /// A ledger that answers every command with one answer.
    struct Answering(fn() -> Answer);

    impl CommandExecutor<Fan> for Answering {
        async fn execute_with_metadata(
            &self,
            _: &crate::shared::domain::FanLedgerId,
            _: FanCommand,
            _: HashMap<String, String>,
        ) -> Answer {
            (self.0)()
        }
    }

    /// The plan table (PLAN.md §2.7), for both steps of the ledger: accepted, R9 and R11 are done; R10, a conflict
    /// and an unavailable ledger are retried, with the reasons the outbox keeps.
    #[tokio::test]
    async fn outcomes_map_to_task_results_as_in_the_plan_table() {
        let id = Uuid::new_v4();
        let rows: [Row; 6] = [
            ("accepted", || Ok(CommitReport::default()), TaskResult::Done),
            (
                "R9",
                || {
                    Err(CommandGatewayError::Domain(FanError::AlreadyRecorded {
                        booking_id: Uuid::nil(),
                    }))
                },
                TaskResult::Done,
            ),
            (
                "R11",
                || {
                    Err(CommandGatewayError::Domain(FanError::AlreadyCancelled {
                        booking_id: Uuid::nil(),
                    }))
                },
                TaskResult::Done,
            ),
            (
                "R10",
                || {
                    Err(CommandGatewayError::Domain(FanError::UnknownTicket {
                        booking_id: Uuid::nil(),
                    }))
                },
                TaskResult::retry("unknown_ticket"),
            ),
            (
                "conflict",
                || Err(CommandGatewayError::Conflict),
                TaskResult::retry("conflict"),
            ),
            (
                "unavailable",
                || Err(CommandGatewayError::Unavailable),
                TaskResult::retry("unavailable"),
            ),
        ];
        let cancellation = RecordCancellationV1 {
            booking_id: id,
            cancellation_id: Uuid::now_v7(),
            concert_id: ticket().concert_id,
            seat: ticket().seat,
            fan_id: ticket().fan_id,
            refund: 80,
        };
        for (answer, ledger, expected) in rows {
            let ledger = Arc::new(Answering(ledger));
            let ticket_step = steps(&ledger, attempt(&ticket(), Uuid::now_v7()));
            assert_eq!(
                record_ticket.call(ticket(), &ticket_step).await,
                expected,
                "record_ticket on {answer}"
            );
            let cancellation_step = steps(&ledger, attempt(&cancellation, Uuid::now_v7()));
            assert_eq!(
                record_cancellation.call(cancellation.clone(), &cancellation_step).await,
                expected,
                "record_cancellation on {answer}"
            );
        }
    }
}
