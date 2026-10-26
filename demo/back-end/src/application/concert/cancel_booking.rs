//! Use case "cancel a booking" (UI): `CancelBooking` → `BookingCancelled`, from which the
//! [refund](crate::application::workflows::refund) plans its first task, `encore_record_cancellation`,
//! committed with the event. The booking and the refund ("what was paid back then") come from the
//! cancellation the aggregate decided under its lock, so nothing is read before the command.
use crate::{
    application::{
        command_service::{CommandMetadata, Executed},
        concert::ConcertApp,
    },
    domain::{
        ConcertId,
        concert::{Concert, ConcertError, commands::CancelBooking},
    },
};
use cqrsx::cqrs::command::CommandExecutor;

impl<E: CommandExecutor<Concert>> ConcertApp<E> {
    /// The cancellation id is the command id.
    pub async fn cancel_booking(
        &self,
        concert_id: &ConcertId,
        command: CancelBooking,
        metadata: CommandMetadata,
    ) -> Executed<ConcertError> {
        self.execute(concert_id, command.into(), metadata).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        application::{
            command_service::{CommandMetadata, CommandOutcome},
            concert::test_support::{ProbeStore, announce, book, cancel, concert_id, harness, poll_once},
        },
        domain::concert::{ConcertError, ConcertEvent},
    };
    use cqrsx::cqrs::{
        store::EventStore,
        testing::{mem_store::MemStore, unavailable_store::UnavailableStore},
    };
    use serde_json::json;
    use std::{collections::HashMap, sync::Arc};
    use uuid::Uuid;

    #[tokio::test]
    async fn a_cancellation_publishes_record_cancellation_with_the_price_paid_and_reads_nothing_before() {
        let store = ProbeStore::default();
        let h = harness(store.clone());
        let booking_id = Uuid::now_v7();
        h.app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
        h.app
            .book_seat(&concert_id(), book("A1", "fox", booking_id), CommandMetadata::ui())
            .await;
        h.executor.take_published_tasks();
        let loads = store.loads();
        let metadata = CommandMetadata::ui();
        let cancellation_id = metadata.causation.command_id;
        let executed = h
            .app
            .cancel_booking(&concert_id(), cancel("a1", "fox", cancellation_id), metadata)
            .await;
        assert!(executed.outcome.is_accepted());
        assert_eq!(executed.tasks, ["encore_record_cancellation v1"]);
        assert_eq!(store.loads() - loads, 1, "one load: the command's own, under its lock");
        assert_eq!(
            h.executor.take_published_tasks()[0].payload(),
            &json!({ "booking_id": booking_id, "cancellation_id": cancellation_id, "concert_id": "radiohead-2026-11-21",
                     "seat": "A1", "fan_id": "fox", "refund": 120 })
        );
    }

    #[tokio::test]
    async fn cancelling_somebody_elses_ticket_publishes_no_task() {
        let h = harness(MemStore::default());
        h.app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
        h.app
            .book_seat(&concert_id(), book("B4", "fox", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        h.executor.take_published_tasks();
        let executed = h
            .app
            .cancel_booking(&concert_id(), cancel("B4", "panda", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        assert_eq!(executed.outcome, CommandOutcome::Rejected(ConcertError::NotYourTicket));
        assert!(executed.tasks.is_empty());
        assert!(h.executor.take_published_tasks().is_empty());
    }

    #[tokio::test]
    async fn an_unavailable_ledger_makes_the_cancellation_unavailable_and_it_is_logged() {
        let h = harness(UnavailableStore);
        let executed = h
            .app
            .cancel_booking(&concert_id(), cancel("B4", "fox", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        assert_eq!(executed.outcome, CommandOutcome::Unavailable);
        assert!(executed.tasks.is_empty());
        let log = h.activity();
        assert_eq!(
            (log.len(), log[0].kind.as_str(), log[0].fields["outcome"].as_str()),
            (1, "command.failed", Some("unavailable")),
            "the command ran and failed: nothing is read before it any more"
        );
    }

    /// The stale read the pre-read allowed: a cancellation that waits in the concert's lock line behind a
    /// cancellation and a rebooking of the same seat refunds the booking it cancels, not the one that held
    /// the seat when it was submitted.
    #[tokio::test]
    async fn a_cancellation_queued_behind_a_rebooking_refunds_the_booking_it_cancels() {
        let store = ProbeStore::default();
        let h = harness(store.clone());
        h.app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
        let first = Uuid::now_v7();
        h.app
            .book_seat(&concert_id(), book("A1", "fox", first), CommandMetadata::ui())
            .await;
        h.executor.take_published_tasks();
        // The first cancellation holds the lock while its load waits at the gate; the rebooking and the
        // second cancellation queue behind it in this order.
        let gate = store.close_gate();
        let (id, cancel_first, second, cancel_second) = (concert_id(), Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let mut first_cancellation = Box::pin(h.app.cancel_booking(
            &id,
            cancel("A1", "fox", cancel_first),
            metadata_of(cancel_first),
        ));
        assert!(poll_once(first_cancellation.as_mut()).await.is_none());
        let mut rebooking = Box::pin(h.app.book_seat(&id, book("A1", "fox", second), metadata_of(second)));
        assert!(poll_once(rebooking.as_mut()).await.is_none());
        let mut second_cancellation = Box::pin(h.app.cancel_booking(
            &id,
            cancel("A1", "fox", cancel_second),
            metadata_of(cancel_second),
        ));
        assert!(poll_once(second_cancellation.as_mut()).await.is_none());
        gate.add_permits(3);
        let (one, two, three) = tokio::join!(first_cancellation, rebooking, second_cancellation);
        assert!(one.outcome.is_accepted() && two.outcome.is_accepted() && three.outcome.is_accepted());
        let refunded: Vec<_> = h
            .executor
            .take_published_tasks()
            .into_iter()
            .filter(|task| task.format().task_type() == "encore_record_cancellation")
            .map(|task| {
                (
                    task.payload()["cancellation_id"].clone(),
                    task.payload()["booking_id"].clone(),
                )
            })
            .collect();
        assert_eq!(
            refunded,
            [(json!(cancel_first), json!(first)), (json!(cancel_second), json!(second))],
            "every cancellation refunds the booking its own decision cancelled"
        );
    }

    fn metadata_of(command_id: Uuid) -> CommandMetadata {
        let mut metadata = CommandMetadata::ui();
        let causation = &mut metadata.causation;
        (causation.command_id, causation.correlation_id, causation.causation_id) = (command_id, command_id, command_id);
        metadata
    }

    /// Many cancellations and rebookings of one seat at once: whatever the interleaving, every published
    /// cancellation task carries the booking of the `BookingCancelled` its own command committed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn racing_cancellations_and_rebookings_publish_tasks_that_match_their_events() {
        let store = MemStore::default();
        let h = Arc::new(harness(store.clone()));
        h.app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
        let mut commands = tokio::task::JoinSet::new();
        for round in 0..24 {
            let h = h.clone();
            commands.spawn(async move {
                let id = Uuid::now_v7();
                if round % 2 == 0 {
                    h.app.book_seat(&concert_id(), book("A1", "fox", id), metadata_of(id)).await
                } else {
                    h.app
                        .cancel_booking(&concert_id(), cancel("A1", "fox", id), metadata_of(id))
                        .await
                }
            });
        }
        while let Some(result) = commands.join_next().await {
            result.unwrap();
        }
        let cancelled: HashMap<String, Uuid> = store
            .load_events(concert_id().as_str())
            .await
            .unwrap()
            .into_iter()
            .filter_map(|event| match event.payload {
                ConcertEvent::BookingCancelled(cancelled) => Some((event.metadata["command_id"].clone(), cancelled.booking_id)),
                _ => None,
            })
            .collect();
        let tasks: Vec<_> = h
            .executor
            .take_published_tasks()
            .into_iter()
            .filter(|task| task.format().task_type() == "encore_record_cancellation")
            .collect();
        assert_eq!(tasks.len(), cancelled.len(), "one task per committed cancellation");
        for task in tasks {
            let command_id = task.metadata()["command_id"].as_str().unwrap().to_owned();
            assert_eq!(task.payload()["booking_id"], json!(cancelled[&command_id]));
        }
    }
}
