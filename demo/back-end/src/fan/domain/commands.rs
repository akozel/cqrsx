//! Commands of the `Fan` aggregate, one module per command. They are internal: only outbox task
//! handlers issue them, never the UI.
pub mod record_cancellation;
pub mod record_ticket;

pub use record_cancellation::RecordCancellation;
pub use record_ticket::RecordTicket;

use cqrsx::Commands;

/// The derive routes every variant to its command's decision and reports it to the activity feed by its name.
#[derive(Debug, Clone, PartialEq, Eq, Commands)]
pub enum FanCommand {
    RecordTicket(RecordTicket),
    RecordCancellation(RecordCancellation),
}

#[cfg(test)]
mod tests {
    use crate::fan::domain::test_support::{cancel, record};
    use cqrsx::domain::command::CommandInfo;
    use uuid::Uuid;

    /// The activity feed names a ledger's command by its type; its fan is the ledger's, its id not the command's.
    #[test]
    fn every_ledger_command_reports_its_name() {
        let id = Uuid::now_v7();
        let reported = [record(id), cancel(id, Uuid::now_v7())]
            .iter()
            .map(|command| (command.name(), command.labels(), command.command_id()))
            .collect::<Vec<_>>();
        assert_eq!(
            reported,
            [("RecordTicket", vec![], None), ("RecordCancellation", vec![], None)]
        );
    }
}
