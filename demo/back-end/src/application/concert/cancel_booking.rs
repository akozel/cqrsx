//! Use case "cancel a booking" (UI): `CancelBooking` → `BookingCancelled` + task
//! `encore_record_cancellation`, the first step of the [refund](crate::application::workflows::refund)
//! workflow.
use crate::{
    application::{
        command_service::{CommandMetadata, Executed},
        concert::{ConcertApp, tasks},
    },
    domain::{
        ConcertId,
        concert::{Concert, ConcertError, commands::CancelBooking},
        shared::venue::VENUE,
    },
};
use cqrsx::{
    cqrs::{
        command::CommandExecutor,
        store::{AggregateContext, EventStore},
    },
    task::NewTask,
};

impl<E, S> ConcertApp<E, S>
where
    E: CommandExecutor<Concert>,
    S: EventStore<Concert>,
{
    /// The cancellation id is the command id.
    pub async fn cancel_booking(
        &self,
        concert_id: &ConcertId,
        command: CancelBooking,
        metadata: CommandMetadata,
    ) -> Executed<ConcertError> {
        let tasks = match self.refund_tasks(concert_id, &command, &metadata).await {
            Ok(tasks) => tasks,
            Err(error) => return Executed::tasks_unavailable(error),
        };
        self.execute(concert_id, command.into(), metadata, tasks).await
    }

    /// The booking identity and the refund ("what was paid back then") live in the concert's
    /// history, so read the committed state first. If the booking changes between this read and
    /// the commit, the command is retried on fresh state by the conflict policy and the aggregate
    /// still decides; the task only carries what the `Fan` aggregate needs to find the ticket.
    /// A seat that is unknown, free or somebody else's yields no task: the aggregate rejects the
    /// command (R3, R7, R8).
    async fn refund_tasks(
        &self,
        concert_id: &ConcertId,
        command: &CancelBooking,
        metadata: &CommandMetadata,
    ) -> Result<Vec<NewTask>, String> {
        let Some(seat) = VENUE.seat(&command.seat) else {
            return Ok(vec![]);
        };
        let mut context = self
            .history
            .load_aggregate(concert_id.as_str())
            .await
            .map_err(|e| e.to_string())?;
        let Some(booking) = context
            .aggregate()
            .bookings()
            .get(&seat.id)
            .filter(|booking| booking.fan_id == command.fan_id)
        else {
            return Ok(vec![]);
        };
        let task = tasks::record_cancellation(concert_id, command, seat, booking, metadata, self.task_max_attempts)
            .map_err(|e| e.to_string())?;
        Ok(vec![task])
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        application::{
            command_service::{CommandMetadata, CommandOutcome},
            concert::test_support::{announce, book, cancel, concert_id, harness},
        },
        domain::concert::ConcertError,
    };
    use cqrsx::cqrs::testing::{mem_store::MemStore, unavailable_store::UnavailableStore};
    use serde_json::json;
    use uuid::Uuid;

    #[tokio::test]
    async fn a_cancellation_publishes_record_cancellation_with_the_price_paid() {
        let h = harness(MemStore::default());
        let booking_id = Uuid::now_v7();
        h.app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
        h.app
            .book_seat(&concert_id(), book("A1", "fox", booking_id), CommandMetadata::ui())
            .await;
        h.executor.take_bound_tasks();
        let metadata = CommandMetadata::ui();
        let cancellation_id = metadata.causation.command_id;
        let executed = h
            .app
            .cancel_booking(&concert_id(), cancel("a1", "fox", cancellation_id), metadata)
            .await;
        assert!(executed.outcome.is_accepted());
        assert_eq!(executed.tasks, ["encore_record_cancellation v1"]);
        assert_eq!(
            h.executor.take_bound_tasks()[0].payload(),
            &json!({ "booking_id": booking_id, "cancellation_id": cancellation_id, "concert_id": "radiohead-2026-11-21",
                     "seat": "A1", "fan_id": "fox", "refund": 120 })
        );
    }

    #[tokio::test]
    async fn cancelling_somebody_elses_ticket_binds_no_task() {
        let h = harness(MemStore::default());
        h.app.announce_concert(&concert_id(), announce(), CommandMetadata::ui()).await;
        h.app
            .book_seat(&concert_id(), book("B4", "fox", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        h.executor.take_bound_tasks();
        let executed = h
            .app
            .cancel_booking(&concert_id(), cancel("B4", "panda", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        assert_eq!(executed.outcome, CommandOutcome::Rejected(ConcertError::NotYourTicket));
        assert!(executed.tasks.is_empty());
        assert!(h.executor.take_bound_tasks().is_empty());
    }

    #[tokio::test]
    async fn an_unavailable_ledger_makes_the_cancellation_unavailable() {
        let h = harness(UnavailableStore);
        let executed = h
            .app
            .cancel_booking(&concert_id(), cancel("B4", "fox", Uuid::now_v7()), CommandMetadata::ui())
            .await;
        assert_eq!(executed.outcome, CommandOutcome::Unavailable);
        assert!(executed.tasks.is_empty());
        assert!(h.activity().is_empty(), "the command never ran");
    }
}
