//! What follows each fact of the box office: the reactions of `Concert`. Two processes start here and continue in
//! the fan's ledger:
//!
//! - ticket delivery: `Concert::SeatBooked` → task `encore_record_ticket` →
//!   [`record_ticket`](crate::fan::application::record_ticket) (`Fan.RecordTicket` → `Fan::TicketRecorded`) → task
//!   `encore_send_e_ticket` → [`send_e_ticket`](crate::fan::application::send_e_ticket) (e-mail "🎫 Your e-ticket");
//! - refund: `Concert::BookingCancelled` → task `encore_record_cancellation` →
//!   [`record_cancellation`](crate::fan::application::record_cancellation) (`Fan.RecordCancellation` →
//!   `Fan::TicketCancelled`) → task `encore_issue_refund` → [`issue_refund`](crate::fan::application::issue_refund)
//!   (e-mail "💸 Refund").
//!
//! [`fan_ledger`] builds the ledger's tasks from the facts: the booking and the refund come from
//! the cancellation the box office decided, so nothing is read before the command. The attempt budget of a task is
//! the policy of its type, never the reaction's.
use crate::concert::{
    application::fan_ledger,
    domain::{Concert, ConcertEvent},
};
use cqrsx::cqrs::reactions::{Decided, Reactions, Then};

/// A booked seat starts the delivery: the ticket goes to the fan's ledger. A cancelled booking starts the refund:
/// the cancellation goes to the fan's ledger. An announcement starts nothing.
impl Reactions for Concert {
    fn react(decided: &Decided<'_, Self>, then: &mut Then) {
        match decided.event {
            ConcertEvent::ConcertAnnounced(_) => {}
            ConcertEvent::SeatBooked(booked) => then.task(fan_ledger::record_ticket(decided.id, booked)),
            ConcertEvent::BookingCancelled(cancelled) => then.task(fan_ledger::record_cancellation(decided.id, cancelled)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        concert::{
            application::test_support::{booked, cancelled_booking, concert_command, concert_id},
            domain::events::ConcertAnnounced,
        },
        fan::application::record_ticket::RecordTicketV1,
        shared::application::test_support::{formats, plan},
    };
    use cqrsx::{cqrs::reactions::ReactionsPlanner, task::format::TaskFormat};
    use serde_json::json;
    use uuid::Uuid;

    /// The planner of the box office's reactions, with the policies of the configuration.
    fn reactions() -> ReactionsPlanner<Concert> {
        ReactionsPlanner::new(crate::testing::policies())
    }

    #[test]
    fn a_booked_seat_plans_the_ticket_for_the_fans_ledger_from_the_fact() {
        let booking_id = Uuid::from_u128(1);
        let tasks = plan::<Concert, _>(&reactions(), &concert_id(), &concert_command(), &[booked(booking_id)]).unwrap();
        let [task] = &tasks[..] else {
            panic!("one task per booking: {tasks:?}");
        };
        assert_eq!(task.format(), &TaskFormat::of::<RecordTicketV1>().unwrap());
        assert_eq!(
            task.payload(),
            &json!({ "booking_id": booking_id, "concert_id": "radiohead-2026-11-21", "seat": "A1", "zone": "vip",
                     "price": 120, "fan_id": "fox", "fan_name": "Fox" })
        );
        assert_eq!(task.metadata(), &json!({}));
        assert_eq!(task.max_attempts(), 6);
    }

    #[test]
    fn a_cancelled_booking_plans_the_cancellation_with_the_price_paid() {
        let (booking_id, cancellation_id) = (Uuid::from_u128(1), Uuid::from_u128(2));
        let tasks = plan::<Concert, _>(
            &reactions(),
            &concert_id(),
            &concert_command(),
            &[cancelled_booking(booking_id, cancellation_id)],
        )
        .unwrap();
        let [task] = &tasks[..] else {
            panic!("one task per cancellation: {tasks:?}");
        };
        assert_eq!(
            task.payload(),
            &json!({ "booking_id": booking_id, "cancellation_id": cancellation_id, "concert_id": "radiohead-2026-11-21",
                     "seat": "A1", "fan_id": "fox", "refund": 120 })
        );
        assert_eq!(task.metadata(), &json!({}));
        assert_eq!(task.max_attempts(), 6);
    }

    #[test]
    fn every_reacting_fact_plans_one_task_through_the_aggregates_planner() {
        let concert = reactions();
        let id = concert_id();
        assert_eq!(
            formats(&plan::<Concert, _>(&concert, &id, &concert_command(), &[booked(Uuid::nil())]).unwrap()),
            ["encore_record_ticket v1"]
        );
        let cancelled = cancelled_booking(Uuid::nil(), Uuid::nil());
        assert_eq!(
            formats(&plan::<Concert, _>(&concert, &id, &concert_command(), &[cancelled]).unwrap()),
            ["encore_record_cancellation v1"]
        );
        let announced = ConcertEvent::ConcertAnnounced(ConcertAnnounced {
            artist: "Radiohead".into(),
            city: "Tbilisi".into(),
            date: chrono::NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
            capacity: 40,
        });
        assert!(
            plan::<Concert, _>(&concert, &id, &concert_command(), &[announced])
                .unwrap()
                .is_empty(),
            "nothing downstream reacts to an announcement"
        );
    }

    /// The delivery plans nothing for a cancellation and the refund nothing for a booking: each fact plans only the
    /// task of its own process. The concert is the decision's typed id, so a foreign one cannot reach the reactions.
    #[test]
    fn each_fact_plans_only_its_own_task() {
        let cancelled = cancelled_booking(Uuid::from_u128(1), Uuid::from_u128(2));
        assert_eq!(
            formats(&plan::<Concert, _>(&reactions(), &concert_id(), &concert_command(), &[cancelled]).unwrap()),
            ["encore_record_cancellation v1"]
        );
        assert_eq!(
            formats(&plan::<Concert, _>(&reactions(), &concert_id(), &concert_command(), &[booked(Uuid::nil())]).unwrap()),
            ["encore_record_ticket v1"]
        );
    }

    /// The box office through a test engine, as the HTTP handler runs it: the executor with the reactions' planner
    /// and the activity feed's observer. What each command publishes with its event, what the feed says of it, and the
    /// races of cancellations and rebookings on one concert.
    mod engine {
        use crate::concert::{
            application::test_support::{ProbeStore, announce, book, cancel, committed, concert_id, harness, poll_once},
            domain::{ConcertError, ConcertEvent},
        };
        use cqrsx::{
            cqrs::{
                command::{CommandGatewayError, metadata::CommandMetadata},
                store::EventStore,
                testing::{mem_store::MemStore, unavailable_store::UnavailableStore},
            },
            task::format::TaskFormat,
        };
        use serde_json::{Value, json};
        use std::{collections::HashMap, sync::Arc};
        use uuid::Uuid;

        /// What the activity feed names every command of the box office by: its type and the fan it is for, accepted
        /// or rejected; an announcement is nobody's.
        #[tokio::test]
        async fn every_command_is_logged_with_its_type_and_its_fan() {
            let h = harness(MemStore::default());
            h.execute(&concert_id(), announce(), CommandMetadata::root("ui"))
                .await
                .unwrap();
            h.execute(&concert_id(), book("A1", "fox", Uuid::now_v7()), CommandMetadata::root("ui"))
                .await
                .unwrap();
            // Refused: the feed below says so.
            let _ = h
                .execute(
                    &concert_id(),
                    book("A1", "panda", Uuid::now_v7()),
                    CommandMetadata::root("ui"),
                )
                .await;
            h.execute(
                &concert_id(),
                cancel("A1", "fox", Uuid::now_v7()),
                CommandMetadata::root("ui"),
            )
            .await
            .unwrap();
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

        #[tokio::test]
        async fn an_announcement_publishes_no_task_and_is_logged() {
            let h = harness(MemStore::default());
            let executed = h.execute(&concert_id(), announce(), CommandMetadata::root("ui")).await;
            assert!(executed.is_ok());
            assert!(committed(&executed).is_empty());
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
            h.execute(&concert_id(), announce(), CommandMetadata::root("ui"))
                .await
                .unwrap();
            let executed = h.execute(&concert_id(), announce(), CommandMetadata::root("ui")).await;
            assert_eq!(executed, Err(CommandGatewayError::Domain(ConcertError::AlreadyAnnounced)));
            let log = h.activity();
            assert_eq!(log[1].kind, "command.rejected");
            assert_eq!(log[1].fields["code"], "already_announced");
        }

        #[tokio::test]
        async fn a_booking_publishes_record_ticket_with_the_price_of_the_fact_and_its_causation() {
            let store = MemStore::default();
            let h = harness(store.clone());
            h.execute(&concert_id(), announce(), CommandMetadata::root("ui"))
                .await
                .unwrap();
            let metadata = CommandMetadata::root("ui");
            let booking_id = metadata.causation.command_id;
            let executed = h
                .execute(&concert_id(), book("a1", "fox", booking_id), metadata.clone())
                .await;
            assert!(executed.is_ok());
            assert_eq!(committed(&executed), ["encore_record_ticket v1"]);
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
            h.execute(&concert_id(), announce(), CommandMetadata::root("ui"))
                .await
                .unwrap();
            let executed = h
                .execute(&concert_id(), book("Z9", "fox", Uuid::now_v7()), CommandMetadata::root("ui"))
                .await;
            assert_eq!(
                executed,
                Err(CommandGatewayError::Domain(ConcertError::UnknownSeat { seat: "Z9".into() }))
            );
            assert!(committed(&executed).is_empty());
            assert!(h.executor.take_published_tasks().is_empty());
        }

        #[tokio::test]
        async fn a_taken_seat_is_rejected_and_writes_nothing() {
            let store = MemStore::default();
            let h = harness(store.clone());
            h.execute(&concert_id(), announce(), CommandMetadata::root("ui"))
                .await
                .unwrap();
            h.execute(&concert_id(), book("B4", "fox", Uuid::now_v7()), CommandMetadata::root("ui"))
                .await
                .unwrap();
            h.executor.take_published_tasks();
            let executed = h
                .execute(
                    &concert_id(),
                    book("B4", "panda", Uuid::now_v7()),
                    CommandMetadata::root("ui"),
                )
                .await;
            assert!(matches!(
                executed,
                Err(CommandGatewayError::Domain(ConcertError::SeatTaken { .. }))
            ));
            assert_eq!(store.load_events(concert_id().as_str()).await.unwrap().len(), 2);
            assert!(h.executor.take_published_tasks().is_empty());
        }

        #[tokio::test]
        async fn a_cancellation_publishes_record_cancellation_with_the_price_paid_and_reads_nothing_before() {
            let store = ProbeStore::default();
            let h = harness(store.clone());
            let booking_id = Uuid::now_v7();
            h.execute(&concert_id(), announce(), CommandMetadata::root("ui"))
                .await
                .unwrap();
            h.execute(&concert_id(), book("A1", "fox", booking_id), CommandMetadata::root("ui"))
                .await
                .unwrap();
            h.executor.take_published_tasks();
            let loads = store.loads();
            let metadata = CommandMetadata::root("ui");
            let cancellation_id = metadata.causation.command_id;
            let executed = h.execute(&concert_id(), cancel("a1", "fox", cancellation_id), metadata).await;
            assert!(executed.is_ok());
            assert_eq!(committed(&executed), ["encore_record_cancellation v1"]);
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
            h.execute(&concert_id(), announce(), CommandMetadata::root("ui"))
                .await
                .unwrap();
            h.execute(&concert_id(), book("B4", "fox", Uuid::now_v7()), CommandMetadata::root("ui"))
                .await
                .unwrap();
            h.executor.take_published_tasks();
            let executed = h
                .execute(
                    &concert_id(),
                    cancel("B4", "panda", Uuid::now_v7()),
                    CommandMetadata::root("ui"),
                )
                .await;
            assert_eq!(executed, Err(CommandGatewayError::Domain(ConcertError::NotYourTicket)));
            assert!(committed(&executed).is_empty());
            assert!(h.executor.take_published_tasks().is_empty());
        }

        #[tokio::test]
        async fn an_unavailable_ledger_makes_the_cancellation_unavailable_and_it_is_logged() {
            let h = harness(UnavailableStore);
            let executed = h
                .execute(
                    &concert_id(),
                    cancel("B4", "fox", Uuid::now_v7()),
                    CommandMetadata::root("ui"),
                )
                .await;
            assert_eq!(executed, Err(CommandGatewayError::Unavailable));
            assert!(committed(&executed).is_empty());
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
            h.execute(&concert_id(), announce(), CommandMetadata::root("ui"))
                .await
                .unwrap();
            let first = Uuid::now_v7();
            h.execute(&concert_id(), book("A1", "fox", first), CommandMetadata::root("ui"))
                .await
                .unwrap();
            h.executor.take_published_tasks();
            // The first cancellation holds the lock while its load waits at the gate; the rebooking and the
            // second cancellation queue behind it in this order.
            let gate = store.close_gate();
            let (id, cancel_first, second, cancel_second) = (concert_id(), Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
            let mut first_cancellation = Box::pin(h.execute(&id, cancel("A1", "fox", cancel_first), metadata_of(cancel_first)));
            assert!(poll_once(first_cancellation.as_mut()).await.is_none());
            let mut rebooking = Box::pin(h.execute(&id, book("A1", "fox", second), metadata_of(second)));
            assert!(poll_once(rebooking.as_mut()).await.is_none());
            let mut second_cancellation =
                Box::pin(h.execute(&id, cancel("A1", "fox", cancel_second), metadata_of(cancel_second)));
            assert!(poll_once(second_cancellation.as_mut()).await.is_none());
            gate.add_permits(3);
            let (one, two, three) = tokio::join!(first_cancellation, rebooking, second_cancellation);
            assert!(one.is_ok() && two.is_ok() && three.is_ok());
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
            let mut metadata = CommandMetadata::root("ui");
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
            h.execute(&concert_id(), announce(), CommandMetadata::root("ui"))
                .await
                .unwrap();
            let mut commands = tokio::task::JoinSet::new();
            for round in 0..24 {
                let h = h.clone();
                commands.spawn(async move {
                    let id = Uuid::now_v7();
                    if round % 2 == 0 {
                        h.execute(&concert_id(), book("A1", "fox", id), metadata_of(id)).await
                    } else {
                        h.execute(&concert_id(), cancel("A1", "fox", id), metadata_of(id)).await
                    }
                });
            }
            while let Some(result) = commands.join_next().await {
                // Accepted or refused: the events below say which.
                let _ = result.unwrap();
            }
            let cancelled: HashMap<String, Uuid> = store
                .load_events(concert_id().as_str())
                .await
                .unwrap()
                .into_iter()
                .filter_map(|event| match event.payload {
                    ConcertEvent::BookingCancelled(cancelled) => {
                        Some((event.metadata["command_id"].clone(), cancelled.booking_id))
                    }
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
}
