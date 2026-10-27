//! Use case "announce a concert" (UI): `AnnounceConcert` → `ConcertAnnounced`. No task: nothing
//! downstream reacts to an announcement.
use crate::{
    concert::{
        application::app::ConcertApp,
        domain::{Concert, ConcertCommand, ConcertError, commands::AnnounceConcert},
    },
    shared::{
        application::command_service::{CommandMetadata, Executed},
        domain::ConcertId,
    },
};
use cqrsx::cqrs::command::CommandExecutor;

impl<E: CommandExecutor<Concert>> ConcertApp<E> {
    pub async fn announce_concert(
        &self,
        concert_id: &ConcertId,
        command: AnnounceConcert,
        metadata: CommandMetadata,
    ) -> Executed<ConcertError> {
        self.execute(concert_id, ConcertCommand::AnnounceConcert(command), metadata)
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        concert::{
            application::test_support::{announce, concert_id, harness},
            domain::ConcertError,
        },
        shared::application::command_service::{CommandMetadata, CommandOutcome},
    };
    use cqrsx::cqrs::testing::mem_store::MemStore;

    #[tokio::test]
    async fn an_announcement_publishes_no_task_and_is_logged() {
        let h = harness(MemStore::default());
        let executed = h.app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
        assert!(executed.outcome.is_accepted());
        assert!(executed.tasks.is_empty());
        assert!(h.executor.take_published_tasks().is_empty());
        let log = h.activity();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].kind, "command.accepted");
        assert_eq!(log[0].fields["aggregate_type"], "Concert");
        assert_eq!(log[0].fields["command_type"], "AnnounceConcert");
        assert_eq!(log[0].fields["fan_id"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn a_second_announcement_is_rejected_and_logged_with_its_code() {
        let h = harness(MemStore::default());
        h.app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
        let executed = h.app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
        assert_eq!(executed.outcome, CommandOutcome::Rejected(ConcertError::AlreadyAnnounced));
        let log = h.activity();
        assert_eq!(log[1].kind, "command.rejected");
        assert_eq!(log[1].fields["code"], "already_announced");
    }
}
