//! The command pipeline of the fan's ticket ledgers (`FanApp`) and how their rejections are reported. The
//! ledger has no UI use cases: the [chain](super::chain) steps issue its commands.
use crate::{
    fan::domain::{Fan, FanCommand, FanError},
    shared::{
        application::command_service::{CommandLabel, CommandMetadata, CommandService, Executed, Rejection},
        domain::FanLedgerId,
    },
};
use cqrsx::cqrs::command::CommandExecutor;
use serde_json::{Value, json};

/// The command pipeline of `Fan`.
pub struct FanApp<E> {
    service: CommandService<Fan, E>,
}

impl<E: CommandExecutor<Fan>> FanApp<E> {
    pub fn new(service: CommandService<Fan, E>) -> Self {
        Self { service }
    }

    /// Internal commands come from the chain steps; the reactions plan the follow-up tasks from the
    /// ledger's decision. The command runs on one ledger (`fan:concert`); the activity log still names
    /// the fan.
    pub async fn execute(&self, ledger: &FanLedgerId, command: FanCommand, metadata: CommandMetadata) -> Executed<FanError> {
        let fan = ledger.fan();
        let label = CommandLabel {
            aggregate_id: ledger.as_str(),
            command_type: command.name(),
            fan_id: Some(fan.as_str()),
        };
        self.service.run(label, command, metadata).await
    }
}

/// Technical rules R9–R11 as the activity log reports them; the chain steps map them to task
/// results (see `chain`).
impl Rejection for FanError {
    fn code(&self) -> &'static str {
        FanError::code(self)
    }

    fn params(&self) -> Value {
        match self {
            FanError::AlreadyRecorded { booking_id }
            | FanError::UnknownTicket { booking_id }
            | FanError::AlreadyCancelled { booking_id } => json!({ "booking_id": booking_id }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fan::domain::commands::RecordCancellation,
        shared::{
            application::{activity::ActivityHub, command_service::CommandOutcome},
            domain::{ConcertId, FanId},
        },
    };
    use cqrsx::cqrs::testing::{command::TestCommandExecutor, unavailable_store::UnavailableStore};
    use std::sync::Arc;
    use uuid::Uuid;

    #[test]
    fn rejections_report_the_booking() {
        let booking_id = Uuid::nil();
        let error = FanError::UnknownTicket { booking_id };
        assert_eq!(Rejection::code(&error), "unknown_ticket");
        assert_eq!(error.params(), json!({ "booking_id": booking_id }));
    }

    #[tokio::test]
    async fn an_unreachable_ledger_is_unavailable_and_logged() {
        let activity = Arc::new(ActivityHub::new(8, 8));
        let fans = FanApp::new(CommandService::new(
            TestCommandExecutor::new(UnavailableStore, ()),
            activity.clone(),
        ));
        let command = FanCommand::RecordCancellation(RecordCancellation {
            booking_id: Uuid::now_v7(),
            cancellation_id: Uuid::now_v7(),
            refund: 80,
        });
        let ledger = FanLedgerId::new(
            &FanId::parse("fox").unwrap(),
            &ConcertId::parse("radiohead-2026-11-21").unwrap(),
        );
        let executed = fans.execute(&ledger, command, CommandMetadata::ui()).await;
        assert_eq!(executed.outcome, CommandOutcome::Unavailable);
        let (entries, _) = activity.subscribe();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            (entries[0].kind.as_str(), entries[0].fields["fan_id"].as_str()),
            ("command.failed", Some("fox"))
        );
    }
}
