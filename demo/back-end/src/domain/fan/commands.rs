//! Commands of the `Fan` aggregate, one module per command. They are internal: only outbox task
//! handlers issue them, never the UI.
pub mod record_cancellation;
pub mod record_ticket;

pub use record_cancellation::RecordCancellation;
pub use record_ticket::RecordTicket;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanCommand {
    RecordTicket(RecordTicket),
    RecordCancellation(RecordCancellation),
}

impl FanCommand {
    /// The command type the activity log reports.
    pub fn name(&self) -> &'static str {
        match self {
            FanCommand::RecordTicket(_) => "RecordTicket",
            FanCommand::RecordCancellation(_) => "RecordCancellation",
        }
    }
}

impl From<RecordTicket> for FanCommand {
    fn from(command: RecordTicket) -> Self {
        FanCommand::RecordTicket(command)
    }
}

impl From<RecordCancellation> for FanCommand {
    fn from(command: RecordCancellation) -> Self {
        FanCommand::RecordCancellation(command)
    }
}
