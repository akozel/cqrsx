//! The fan's ticket ledgers (`Fan` aggregate, one stream per fan and concert, D-14), their read
//! model and the wallet assembled from them. The ledger has no UI use cases: the
//! [workflow](crate::application::workflows) steps issue its commands.
pub mod views;
pub mod wallet;

use crate::{
    application::{
        command_service::{CommandLabel, CommandMetadata, CommandService, Executed, Rejection},
        lock::BoxOfficeLock,
    },
    domain::{
        FanLedgerId,
        fan::{Fan, FanCommand, FanError},
    },
};
use cqrsx::{cqrs::command::CommandExecutor, task::NewTask};
use serde_json::{Value, json};
use std::sync::Arc;

/// The command pipeline of `Fan`.
pub struct FanApp<E> {
    service: CommandService<Fan, E>,
}

impl<E: CommandExecutor<Fan>> FanApp<E> {
    pub fn new(service: CommandService<Fan, E>) -> Self {
        Self { service }
    }

    pub fn lock(&self) -> &Arc<BoxOfficeLock> {
        self.service.lock()
    }

    /// Internal commands come from workflow steps, which also build the follow-up tasks. The command
    /// runs on one ledger (`fan:concert`); the activity log still names the fan. During a rebuild the
    /// outcome is `Locked`, which the step turns into a short retry: the task is durable, so waiting
    /// costs nothing, and the wallet projection is not written while its collection is being rebuilt.
    pub async fn execute(
        &self,
        ledger: &FanLedgerId,
        command: FanCommand,
        metadata: CommandMetadata,
        tasks: Vec<NewTask>,
    ) -> Executed<FanError> {
        let fan = ledger.fan();
        let label = CommandLabel {
            aggregate_id: ledger.as_str(),
            command_type: command.name(),
            fan_id: Some(fan.as_str()),
        };
        self.service.run(label, command, metadata, tasks).await
    }
}

/// Technical rules R9–R11 as the activity log reports them; the workflow steps map them to task
/// results (see `workflows`).
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
        application::{activity::ActivityHub, command_service::CommandOutcome},
        domain::{ConcertId, FanId, fan::commands::RecordCancellation},
    };
    use cqrsx::cqrs::testing::{command::TestCommandExecutor, unavailable_store::UnavailableStore};
    use uuid::Uuid;

    #[test]
    fn rejections_report_the_booking() {
        let booking_id = Uuid::nil();
        let error = FanError::UnknownTicket { booking_id };
        assert_eq!(Rejection::code(&error), "unknown_ticket");
        assert_eq!(error.params(), json!({ "booking_id": booking_id }));
    }

    #[tokio::test]
    async fn a_closed_box_office_refuses_internal_commands_before_touching_storage() {
        let lock = Arc::new(BoxOfficeLock::new());
        let activity = Arc::new(ActivityHub::new(8, 8));
        let fans = FanApp::new(CommandService::new(
            TestCommandExecutor::new(UnavailableStore, ()),
            activity.clone(),
            lock.clone(),
        ));
        let _guard = lock.try_acquire().unwrap();
        let command = FanCommand::RecordCancellation(RecordCancellation {
            booking_id: Uuid::now_v7(),
            cancellation_id: Uuid::now_v7(),
            refund: 80,
        });
        let ledger = FanLedgerId::new(
            &FanId::parse("fox").unwrap(),
            &ConcertId::parse("radiohead-2026-11-21").unwrap(),
        );
        let executed = fans.execute(&ledger, command, CommandMetadata::ui(), vec![]).await;
        assert_eq!(executed.outcome, CommandOutcome::Locked);
        assert!(activity.subscribe().0.is_empty(), "a locked outcome is not logged");
    }
}
