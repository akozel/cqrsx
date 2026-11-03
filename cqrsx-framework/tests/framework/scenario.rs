//! The path of `tests/in_memory/planning.rs` through modules on the framework in memory: a box office whose bookings go into
//! the fans' wallets by a step of a chain, whose recorded tickets are mailed through a dependency, with a view of the
//! halls; the commands, the tasks, the view and the chain's causation, written with the derive macros.
use crate::modules::*;
#[cfg(feature = "operation-utils")]
use cqrsx::command::causation::{CAUSATION_ID, COMMAND_ID};
use cqrsx::{
    command::{CommandError, causation::CORRELATION_ID},
    journal::repository::EventRepository,
    memory::task_queue::TaskStatus,
};
use cqrsx_framework::{Cqrsx, memory::InMemory};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

#[tokio::test]
async fn the_box_office_and_the_wallets_run_through_their_modules_in_memory() {
    let outbox = Arc::new(Outbox::default());
    let mail: Arc<Mailer> = outbox.clone();
    let cqrsx = Cqrsx::from_lookup("SHOP", |name| (name == "SHOP_ATTEMPTS").then(|| "5".to_owned()))
        .event_store(InMemory)
        .projections(InMemory)
        .module(box_office)
        .module(wallets)
        .provide(mail)
        .build()
        .await
        .unwrap();
    let ui = cqrsx.source("ui");
    let booking_id = Uuid::now_v7();
    let book = |fan: &str, booking_id| BookSeat {
        seat: "A1".into(),
        fan: fan.into(),
        booking_id,
    };
    let booked = ui
        .execute_with_id::<Show>(booking_id, &"s-1".into(), book("fox", booking_id))
        .await
        .unwrap();
    assert_eq!(
        booked.tasks[0].max_attempts().get(),
        5,
        "the policy's budget from SHOP_ATTEMPTS"
    );
    assert_eq!(
        ui.execute::<Show>(&"s-1".into(), book("panda", Uuid::now_v7()))
            .await
            .unwrap_err(),
        CommandError::Rejected(ShowError::SeatTaken { seat: "A1".into() })
    );

    assert_eq!(cqrsx.run_tasks().await, 2, "the step, then the e-mail its command published");
    assert_eq!(*outbox.0.lock().unwrap(), ["fox:A1"]);
    let tasks = cqrsx.outbox().unwrap().tasks();
    let delivered: Vec<(&str, TaskStatus)> = tasks
        .iter()
        .map(|task| (task.entry.task.format().task_type(), task.status))
        .collect();
    assert_eq!(
        delivered,
        [
            ("record_ticket", TaskStatus::Completed),
            ("mail_ticket", TaskStatus::Completed)
        ]
    );
    let chain = booking_id.to_string();
    assert_eq!(
        tasks[1].entry.task.metadata()[CORRELATION_ID],
        chain,
        "one correlation id along the chain"
    );
    assert_eq!(
        tasks[1].entry.task.metadata()["to"],
        "fox",
        "the label is kept in the metadata"
    );
    let recorded = cqrsx.journal().unwrap().events::<Wallet>("fox").await.unwrap();
    assert_eq!(recorded[0].metadata[CORRELATION_ID], chain);

    cqrsx.catch_up().await;
    let hall = cqrsx.view::<Hall>().get(&"s-1").await.unwrap().unwrap();
    assert_eq!(hall.view.seats, BTreeMap::from([("A1".to_owned(), "fox".to_owned())]));
    #[cfg(feature = "operation-utils")]
    assert_eq!(cqrsx.views().verify("halls").await.unwrap().differences, []);
}

#[cfg(feature = "operation-utils")]
#[tokio::test]
async fn a_uuid_field_of_a_command_is_data_and_never_its_id() {
    let outbox = Arc::new(Outbox::default());
    let mail: Arc<Mailer> = outbox.clone();
    let cqrsx = Cqrsx::from_lookup("SHOP", |_| None)
        .event_store(InMemory)
        .projections(InMemory)
        .module(box_office)
        .module(wallets)
        .provide(mail)
        .build()
        .await
        .unwrap();
    let ui = cqrsx.source("ui");
    let (field, given) = (Uuid::now_v7(), Uuid::now_v7());
    let book = |seat: &str| BookSeat {
        seat: seat.into(),
        fan: "fox".into(),
        booking_id: field,
    };
    ui.execute::<Show>(&"s-1".into(), book("A1")).await.unwrap();
    ui.execute_with_id::<Show>(given, &"s-1".into(), book("A2")).await.unwrap();
    let events = cqrsx.history::<Show>(&"s-1".into()).await.unwrap();
    for event in &events {
        let ShowEvent::SeatBooked(booked) = &event.payload;
        assert_eq!(booked.booking_id, field, "the field reaches the event as data");
    }
    let generated = Uuid::parse_str(&events[0].metadata[COMMAND_ID]).unwrap();
    assert_ne!(generated, field, "the id of an `execute` is generated");
    assert_eq!(generated.get_version_num(), 7);
    let id = given.to_string();
    assert_eq!(
        [
            &events[1].metadata[COMMAND_ID],
            &events[1].metadata[CORRELATION_ID],
            &events[1].metadata[CAUSATION_ID]
        ],
        [&id, &id, &id],
        "the id of an `execute_with_id` is its argument"
    );
}
