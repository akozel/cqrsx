//! The box office's command pipeline (`ConcertApp`); one file per use case next to it. The tasks that follow its
//! decisions are planned by its [reactions](super::reactions).
use crate::{
    concert::domain::{Concert, ConcertCommand, ConcertError},
    shared::{
        application::command_service::{CommandLabel, CommandMetadata, CommandService, Executed},
        domain::ConcertId,
    },
};
use cqrsx::cqrs::command::{CommandExecutor, CommandInfo};

/// The box office: the command pipeline of `Concert`. Its executor's planner plans the tasks that follow
/// every decision, so a use case passes the command only.
pub struct ConcertApp<E> {
    service: CommandService<Concert, E>,
}

impl<E: CommandExecutor<Concert>> ConcertApp<E> {
    pub fn new(service: CommandService<Concert, E>) -> Self {
        Self { service }
    }

    pub(super) async fn execute(
        &self,
        concert_id: &ConcertId,
        command: ConcertCommand,
        metadata: CommandMetadata,
    ) -> Executed<ConcertError> {
        // The labels of the command (`#[label]`): the activity feed names the fan of a booking and a cancellation.
        let labels = command.labels();
        let label = CommandLabel {
            aggregate_id: concert_id.as_str(),
            command_type: command.name(),
            fan_id: labels.iter().find(|(key, _)| *key == "fan_id").map(|(_, fan)| fan.as_str()),
        };
        self.service.run(concert_id, label, command, metadata).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        concert::application::test_support::{announce, book, cancel, concert_id, harness},
        shared::application::command_service::CommandMetadata,
    };
    use cqrsx::cqrs::testing::mem_store::MemStore;
    use serde_json::{Value, json};
    use uuid::Uuid;

    /// What the activity feed names every command of the box office by: its type and the fan it is for, accepted or
    /// rejected; an announcement is nobody's.
    #[tokio::test]
    async fn every_command_is_logged_with_its_type_and_its_fan() {
        let h = harness(MemStore::default());
        h.app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
        h.app
            .book_seat(&concert_id(), book("A1", "fox", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        h.app
            .book_seat(&concert_id(), book("A1", "panda", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        h.app
            .cancel_booking(&concert_id(), cancel("A1", "fox", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        let logged: Vec<(String, Value, Value)> = h
            .activity()
            .iter()
            .map(|entry| {
                (
                    entry.kind.clone(),
                    entry.fields["command_type"].clone(),
                    entry.fields["fan_id"].clone(),
                )
            })
            .collect();
        assert_eq!(
            logged,
            [
                ("command.accepted".to_owned(), json!("AnnounceConcert"), Value::Null),
                ("command.accepted".to_owned(), json!("BookSeat"), json!("fox")),
                ("command.rejected".to_owned(), json!("BookSeat"), json!("panda")),
                ("command.accepted".to_owned(), json!("CancelBooking"), json!("fox")),
            ]
        );
    }
}
