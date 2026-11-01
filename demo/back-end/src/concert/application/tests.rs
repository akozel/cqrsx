//! The box office end to end on the engine in memory (`testing::test_app`): its commands, the tasks they start in the
//! fans' ledgers, the letters those end in and the concert halls; and the races of cancellations and rebookings on one
//! concert, as the HTTP API issues them.
use crate::{
    concert::{
        application::{
            test_support::{announce, book, cancel, concert_id, poll_once},
            views::concert_hall::{ConcertHallView, SeatState},
        },
        domain::{Concert, ConcertEvent},
    },
    fan::application::record_ticket::RecordTicketV1,
    testing::test_app,
};
use cqrsx::{
    journal::{EventStore, store::JournalEventStore},
    memory::task_queue::TaskStatus,
    task::format::TaskFormat,
    worker::queue::{TaskQueue, settlement::SettlementRequest},
};
use cqrsx_engine::Cqrs;
use serde_json::json;
use std::{collections::HashMap, sync::Arc, time::Duration};
use uuid::Uuid;

/// Every task of the outbox: its type, its status and its claims.
fn deliveries(cqrs: &Cqrs) -> Vec<(String, TaskStatus, i32)> {
    let tasks = cqrs.outbox().unwrap().tasks();
    tasks
        .into_iter()
        .map(|task| (task.entry.task.format().task_type().to_owned(), task.status, task.attempts))
        .collect()
}

fn done(task_type: &str, attempts: i32) -> (String, TaskStatus, i32) {
    (task_type.to_owned(), TaskStatus::Completed, attempts)
}

#[tokio::test]
async fn a_cancellation_refunds_the_fan() {
    let (cqrs, mail) = test_app().await;
    let ui = cqrs.source("ui");
    ui.execute::<Concert>(&concert_id(), announce()).await.unwrap();
    ui.execute::<Concert>(&concert_id(), book("A1", "fox", Uuid::now_v7()))
        .await
        .unwrap();
    ui.execute::<Concert>(&concert_id(), cancel("A1", "fox", Uuid::now_v7()))
        .await
        .unwrap();
    assert_eq!(cqrs.run_tasks().await, 4, "two steps of the ledger, two e-mails");
    assert_eq!(mail.kinds(), ["e_ticket", "refund"]);
    assert_eq!(
        deliveries(&cqrs),
        [
            done("encore_record_ticket", 1),
            done("encore_record_cancellation", 1),
            done("encore_send_e_ticket", 1),
            done("encore_issue_refund", 1)
        ]
    );
}

/// G-16: the process stops after the effects of the ledger's step and of its e-mail and before their acknowledgements;
/// both are delivered again once their leases run out. The step is done by R9 (the ticket is in the ledger already),
/// so no second ticket and no second e-ticket task; the e-mail goes out again under its key, which the provider keeps
/// once.
#[tokio::test(start_paused = true)]
async fn a_delivery_repeated_after_a_lost_acknowledgement_records_one_ticket_and_one_letter() {
    let (cqrs, mail) = test_app().await;
    let ui = cqrs.source("ui");
    let booking_id = Uuid::now_v7();
    ui.execute::<Concert>(&concert_id(), announce()).await.unwrap();
    ui.execute::<Concert>(&concert_id(), book("A1", "fox", booking_id))
        .await
        .unwrap();
    cqrs.outbox().unwrap().lose_settlements(true);
    assert_eq!(
        cqrs.run_tasks().await,
        2,
        "the step and the e-mail ran; their acknowledgements are lost"
    );
    cqrs.outbox().unwrap().lose_settlements(false);
    tokio::time::advance(cqrs.settings().task_lease()).await;
    assert_eq!(
        cqrs.run_tasks().await,
        0,
        "returned by the recovery, they wait the redelivery backoff"
    );
    tokio::time::advance(Duration::from_secs(6)).await;
    assert_eq!(cqrs.run_tasks().await, 2, "both delivered again");
    assert_eq!(
        deliveries(&cqrs),
        [done("encore_record_ticket", 2), done("encore_send_e_ticket", 2)],
        "one e-ticket task: the ledger recorded one ticket"
    );
    assert_eq!(mail.sent().len(), 2, "at least once: the mail service was called twice");
    let letters: Vec<String> = mail.letters().into_iter().map(|letter| letter.id).collect();
    assert_eq!(letters, [format!("e_ticket:{booking_id}")], "one letter per key");
}

/// G-22: a worker of another node holds the step that records the ticket, so the cancellation reaches the ledger first
/// and is retried (R10) after the second of `STEP`; once the ticket is in, the cancellation is too, and the fan gets
/// both letters.
#[tokio::test(start_paused = true)]
async fn a_cancellation_that_overtook_its_ticket_is_retried_and_refunds_after_the_ticket() {
    let (cqrs, mail) = test_app().await;
    let ui = cqrs.source("ui");
    ui.execute::<Concert>(&concert_id(), announce()).await.unwrap();
    ui.execute::<Concert>(&concert_id(), book("A1", "fox", Uuid::now_v7()))
        .await
        .unwrap();
    ui.execute::<Concert>(&concert_id(), cancel("A1", "fox", Uuid::now_v7()))
        .await
        .unwrap();
    let record_ticket = TaskFormat::of::<RecordTicketV1>();
    let mut held = cqrs
        .outbox()
        .unwrap()
        .claim(&record_ticket, "node-b", 1, Duration::from_secs(60))
        .await
        .unwrap();
    assert_eq!(cqrs.run_tasks().await, 1, "the cancellation came first");
    let tasks = cqrs.outbox().unwrap().tasks();
    assert_eq!(
        (tasks[1].status, tasks[1].last_error.as_deref()),
        (TaskStatus::Pending, Some("unknown_ticket"))
    );
    let retry = SettlementRequest::Retry {
        delay: Duration::from_secs(1),
        reason: "unavailable".into(),
    };
    cqrs.outbox().unwrap().settle(&held.remove(0), retry).await.unwrap();
    assert_eq!(cqrs.run_tasks().await, 0, "both wait a second");
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        cqrs.run_tasks().await,
        4,
        "the ticket, the cancellation, the e-ticket, the refund"
    );
    assert_eq!(mail.kinds(), ["e_ticket", "refund"]);
    assert_eq!(
        deliveries(&cqrs),
        [
            done("encore_record_ticket", 2),
            done("encore_record_cancellation", 2),
            done("encore_send_e_ticket", 1),
            done("encore_issue_refund", 1)
        ]
    );
}

#[tokio::test]
async fn the_concert_halls_catch_up_with_the_journal() {
    let (cqrs, _) = test_app().await;
    let ui = cqrs.source("ui");
    ui.execute::<Concert>(&concert_id(), announce()).await.unwrap();
    ui.execute::<Concert>(&concert_id(), book("A1", "fox", Uuid::now_v7()))
        .await
        .unwrap();
    ui.execute::<Concert>(&concert_id(), book("B2", "panda", Uuid::now_v7()))
        .await
        .unwrap();
    ui.execute::<Concert>(&concert_id(), cancel("A1", "fox", Uuid::now_v7()))
        .await
        .unwrap();
    cqrs.catch_up().await;
    let halls = cqrs.view::<ConcertHallView>();
    let hall = halls.get(&concert_id()).await.unwrap().unwrap();
    assert_eq!(
        (hall.last_sequence, &hall.view.seats["A1"].state, &hall.view.seats["B2"].state),
        (4, &SeatState::Free, &SeatState::Booked)
    );
    assert_eq!((hall.view.sold, hall.view.revenue), (1, 80));
    let listed: Vec<String> = halls.list().await.unwrap().into_iter().map(|hall| hall.id).collect();
    assert_eq!(listed, [concert_id().to_string()]);
    assert_eq!(cqrs.views().verify("concert_halls").await.unwrap().differences, []);
}

/// The stale read the pre-read allowed: a cancellation that waits in the concert's lock line behind a
/// cancellation and a rebooking of the same seat refunds the booking it cancels, not the one that held
/// the seat when it was submitted.
#[tokio::test]
async fn a_cancellation_queued_behind_a_rebooking_refunds_the_booking_it_cancels() {
    let (cqrs, _) = test_app().await;
    let ui = cqrs.source("ui");
    ui.execute::<Concert>(&concert_id(), announce()).await.unwrap();
    let first = Uuid::now_v7();
    ui.execute::<Concert>(&concert_id(), book("A1", "fox", first)).await.unwrap();
    // The first cancellation holds the lock while its load waits at the gate; the rebooking and the
    // second cancellation queue behind it in this order.
    let gate = cqrs.journal().unwrap().hold_loads();
    let (id, cancel_first, second, cancel_second) = (concert_id(), Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    let mut first_cancellation = Box::pin(ui.execute::<Concert>(&id, cancel("A1", "fox", cancel_first)));
    assert!(poll_once(first_cancellation.as_mut()).await.is_none());
    let mut rebooking = Box::pin(ui.execute::<Concert>(&id, book("A1", "fox", second)));
    assert!(poll_once(rebooking.as_mut()).await.is_none());
    let mut second_cancellation = Box::pin(ui.execute::<Concert>(&id, cancel("A1", "fox", cancel_second)));
    assert!(poll_once(second_cancellation.as_mut()).await.is_none());
    gate.add_permits(3);
    let (one, two, three) = tokio::join!(first_cancellation, rebooking, second_cancellation);
    assert!(one.is_ok() && two.is_ok() && three.is_ok());
    let refunded: Vec<_> = cqrs
        .outbox()
        .unwrap()
        .tasks()
        .into_iter()
        .map(|queued| queued.entry.task)
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

/// Many cancellations and rebookings of one seat at once: whatever the interleaving, every published
/// cancellation task carries the booking of the `BookingCancelled` its own command committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_cancellations_and_rebookings_publish_tasks_that_match_their_events() {
    let (cqrs, _) = test_app().await;
    let cqrs = Arc::new(cqrs);
    cqrs.source("ui").execute::<Concert>(&concert_id(), announce()).await.unwrap();
    let mut commands = tokio::task::JoinSet::new();
    for round in 0..24 {
        let cqrs = cqrs.clone();
        commands.spawn(async move {
            let (ui, id) = (cqrs.source("ui"), Uuid::now_v7());
            if round % 2 == 0 {
                ui.execute::<Concert>(&concert_id(), book("A1", "fox", id)).await
            } else {
                ui.execute::<Concert>(&concert_id(), cancel("A1", "fox", id)).await
            }
        });
    }
    while let Some(result) = commands.join_next().await {
        // Accepted or refused: the events below say which.
        let _ = result.unwrap();
    }
    let store = JournalEventStore::<_, Concert>::new(cqrs.journal().unwrap().clone());
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
    let tasks: Vec<_> = cqrs
        .outbox()
        .unwrap()
        .tasks()
        .into_iter()
        .map(|queued| queued.entry.task)
        .filter(|task| task.format().task_type() == "encore_record_cancellation")
        .collect();
    assert_eq!(tasks.len(), cancelled.len(), "one task per committed cancellation");
    for task in tasks {
        let command_id = task.metadata()["command_id"].as_str().unwrap().to_owned();
        assert_eq!(task.payload()["booking_id"], json!(cancelled[&command_id]));
    }
}
