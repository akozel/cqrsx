//! Use case "book a seat" (UI): `BookSeat` → `SeatBooked`, from which the
//! [ticket delivery](super::reactions) plans its first task,
//! `encore_record_ticket`, committed with the event.
use crate::{
    concert::{
        application::app::ConcertApp,
        domain::{Concert, ConcertCommand, ConcertError, commands::BookSeat},
    },
    shared::{
        application::command_service::{CommandMetadata, Executed},
        domain::ConcertId,
    },
};
use cqrsx::cqrs::command::CommandExecutor;

impl<E: CommandExecutor<Concert>> ConcertApp<E> {
    /// The booking id is the command id: the one fact the whole chain refers to.
    pub async fn book_seat(
        &self,
        concert_id: &ConcertId,
        command: BookSeat,
        metadata: CommandMetadata,
    ) -> Executed<ConcertError> {
        self.execute(concert_id, ConcertCommand::BookSeat(command), metadata).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        concert::{
            application::test_support::{announce, book, concert_id, harness},
            domain::ConcertError,
        },
        shared::application::command_service::{CommandMetadata, CommandOutcome},
    };
    use cqrsx::{
        cqrs::{store::EventStore, testing::mem_store::MemStore},
        task::format::TaskFormat,
    };
    use serde_json::json;
    use uuid::Uuid;

    #[tokio::test]
    async fn a_booking_publishes_record_ticket_with_the_price_of_the_fact_and_its_causation() {
        let store = MemStore::default();
        let h = harness(store.clone());
        h.app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
        let metadata = CommandMetadata::ui();
        let booking_id = metadata.causation.command_id;
        let executed = h
            .app
            .book_seat(&concert_id(), book("a1", "fox", booking_id), metadata.clone())
            .await;
        assert!(executed.outcome.is_accepted());
        assert_eq!(executed.tasks, ["encore_record_ticket v1"]);
        let tasks = h.executor.take_published_tasks();
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            tasks[0].payload(),
            &json!({ "booking_id": booking_id, "concert_id": "radiohead-2026-11-21", "seat": "A1", "zone": "vip",
                     "price": 120, "fan_id": "fox", "fan_name": "FOX" })
        );
        let id = json!(booking_id);
        assert_eq!(
            tasks[0].metadata(),
            &json!({ "command_id": id, "correlation_id": id, "causation_id": id }),
            "wire contract of transactional_outbox.metadata"
        );
        assert_eq!(
            tasks[0].id(),
            metadata.causation.task_id(
                "Concert",
                "radiohead-2026-11-21",
                &TaskFormat::new("encore_record_ticket", 1).unwrap(),
                0
            ),
            "the task id derives from the command"
        );
        assert_eq!(tasks[0].max_attempts(), 6);
        let events = store.load_events(concert_id().as_str()).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].metadata["command_id"], booking_id.to_string());
    }

    #[tokio::test]
    async fn an_unknown_seat_is_rejected_and_publishes_no_task() {
        let h = harness(MemStore::default());
        h.app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
        let executed = h
            .app
            .book_seat(&concert_id(), book("Z9", "fox", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        assert_eq!(
            executed.outcome,
            CommandOutcome::Rejected(ConcertError::UnknownSeat { seat: "Z9".into() })
        );
        assert!(executed.tasks.is_empty());
        assert!(h.executor.take_published_tasks().is_empty());
    }

    #[tokio::test]
    async fn a_taken_seat_is_rejected_and_writes_nothing() {
        let store = MemStore::default();
        let h = harness(store.clone());
        h.app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
        h.app
            .book_seat(&concert_id(), book("B4", "fox", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        h.executor.take_published_tasks();
        let executed = h
            .app
            .book_seat(&concert_id(), book("B4", "panda", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        assert!(matches!(
            executed.outcome,
            CommandOutcome::Rejected(ConcertError::SeatTaken { .. })
        ));
        assert_eq!(store.load_events(concert_id().as_str()).await.unwrap().len(), 2);
        assert!(h.executor.take_published_tasks().is_empty());
    }
}
