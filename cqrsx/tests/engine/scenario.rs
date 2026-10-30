//! The path of `tests/cqrs/planning.rs` through modules on the engine in memory: a box office whose bookings go into
//! the fans' wallets by a step of a chain, whose recorded tickets are mailed through a dependency, with a view of the
//! halls; the commands, the tasks, the view and the chain's causation, written with the derive macros.
use crate::modules::*;
use cqrsx::{
    cqrs::{
        command::{CommandGatewayError, causation::CORRELATION_ID},
        store::repository::PersistedEventRepository,
    },
    engine::Cqrs,
    task::testing::mem_task_queue::TaskStatus,
};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

#[tokio::test]
async fn the_box_office_and_the_wallets_run_through_their_modules_in_memory() {
    let outbox = Arc::new(Outbox::default());
    let mail: Arc<Mailer> = outbox.clone();
    let cqrs = Cqrs::from_lookup("SHOP", |name| (name == "SHOP_ATTEMPTS").then(|| "5".to_owned()))
        .module(box_office)
        .module(wallets)
        .provide(mail)
        .build()
        .await
        .unwrap();
    let ui = cqrs.from("ui");
    let booking_id = Uuid::now_v7();
    let book = |fan: &str, booking_id| BookSeat {
        seat: "A1".into(),
        fan: fan.into(),
        booking_id,
    };
    let booked = ui.execute::<Show>(&"s-1".into(), book("fox", booking_id)).await.unwrap();
    assert_eq!(booked.tasks[0].max_attempts(), 5, "the policy's budget from SHOP_ATTEMPTS");
    assert_eq!(
        ui.execute::<Show>(&"s-1".into(), book("panda", Uuid::now_v7()))
            .await
            .unwrap_err(),
        CommandGatewayError::Domain(ShowError::SeatTaken { seat: "A1".into() })
    );

    assert_eq!(cqrs.run_tasks().await, 2, "the step, then the e-mail its command published");
    assert_eq!(*outbox.0.lock().unwrap(), ["fox:A1"]);
    let tasks = cqrs.outbox().tasks();
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
    let recorded = cqrs.journal().events::<Wallet>("fox").await.unwrap();
    assert_eq!(recorded[0].metadata[CORRELATION_ID], chain);

    cqrs.catch_up().await;
    let hall = cqrs.view::<Hall>().get(&"s-1").await.unwrap().unwrap();
    assert_eq!(hall.view.seats, BTreeMap::from([("A1".to_owned(), "fox".to_owned())]));
    assert_eq!(cqrs.views().verify("halls").await.unwrap().differences, []);
}
