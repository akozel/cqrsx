//! Use case "book a seat" (UI): `BookSeat` → `SeatBooked` + task `encore_record_ticket`, the first
//! step of the [ticket delivery](crate::application::workflows::ticket_delivery) workflow.
use crate::{
    application::{
        command_service::{CommandMetadata, Executed},
        concert::{ConcertApp, tasks},
    },
    domain::{
        ConcertId,
        concert::{Concert, ConcertError, commands::BookSeat},
        shared::venue::VENUE,
    },
};
use cqrsx::cqrs::{command::CommandExecutor, store::EventStore};

impl<E, S> ConcertApp<E, S>
where
    E: CommandExecutor<Concert>,
    S: EventStore<Concert>,
{
    /// The booking id is the command id: the one fact the whole chain refers to.
    pub async fn book_seat(
        &self,
        concert_id: &ConcertId,
        command: BookSeat,
        metadata: CommandMetadata,
    ) -> Executed<ConcertError> {
        // An unknown seat yields no task: the aggregate rejects the command (R3) before anything
        // is written, so no task can ever be published without its event.
        let tasks = match VENUE.seat(&command.seat) {
            Some(seat) => match tasks::record_ticket(concert_id, &command, seat, &metadata, self.chain_max_attempts) {
                Ok(task) => vec![task],
                Err(error) => return Executed::tasks_unavailable(error),
            },
            None => vec![],
        };
        self.execute(concert_id, command.into(), metadata, tasks).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        application::{
            command_service::{CommandMetadata, CommandOutcome},
            concert::test_support::{announce, book, concert_id, harness},
        },
        domain::concert::ConcertError,
    };
    use cqrsx::cqrs::{store::EventStore, testing::mem_store::MemStore};
    use serde_json::json;
    use uuid::Uuid;

    #[tokio::test]
    async fn a_booking_publishes_record_ticket_with_the_venue_price() {
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
        assert_eq!(
            tasks[0].metadata()["correlation_id"],
            json!(metadata.causation.correlation_id)
        );
        let events = store.load_events(concert_id().as_str()).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].metadata["command_id"], booking_id.to_string());
    }

    #[tokio::test]
    async fn an_unknown_seat_binds_no_task_and_is_rejected() {
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
        assert!(h.executor.take_bound_tasks().is_empty());
    }

    #[tokio::test]
    async fn a_taken_seat_is_rejected_and_writes_nothing() {
        let store = MemStore::default();
        let h = harness(store.clone());
        h.app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
        h.app
            .book_seat(&concert_id(), book("B4", "fox", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        let executed = h
            .app
            .book_seat(&concert_id(), book("B4", "panda", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        assert!(matches!(
            executed.outcome,
            CommandOutcome::Rejected(ConcertError::SeatTaken { .. })
        ));
        assert_eq!(store.load_events(concert_id().as_str()).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_closed_box_office_refuses_the_booking_without_writing() {
        let store = MemStore::default();
        let h = harness(store.clone());
        let _guard = h.lock.try_acquire().unwrap();
        let executed = h
            .app
            .book_seat(&concert_id(), book("B4", "fox", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        assert_eq!(executed.outcome, CommandOutcome::Locked);
        assert!(store.load_events(concert_id().as_str()).await.unwrap().is_empty());
        assert!(h.activity().is_empty(), "a locked outcome is not logged");
    }
}
