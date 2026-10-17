//! A fan ledger over in-memory storage and task attempts, for the workflow step tests.
use crate::{
    application::{
        activity::ActivityHub,
        command_service::CommandService,
        concert::tasks::RecordTicketV1,
        fan::FanApp,
        lock::BoxOfficeLock,
        test_support::{BoundTasks, TestExecutor, executor},
    },
    domain::{
        ConcertId, FanId,
        fan::Fan,
        shared::venue::{SeatId, Zone},
    },
};
use cqrsx::{
    cqrs::testing::mem_store::MemStore,
    task::{
        attempt::TaskAttempt,
        format::{TaskFormat, TaskPayload},
    },
};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

pub type TestFans = FanApp<TestExecutor<Fan, MemStore<Fan>>>;

pub fn fans(store: MemStore<Fan>) -> (Arc<TestFans>, BoundTasks) {
    let bound = BoundTasks::default();
    let service = CommandService::new(
        executor(store, (), bound.clone()),
        vec![],
        Arc::new(ActivityHub::new(16, 16)),
        Arc::new(BoxOfficeLock::new()),
    );
    (Arc::new(FanApp::new(service)), bound)
}

/// One claim of a task of type `P`, carrying the correlation id of the command that published it.
pub fn attempt<P: TaskPayload>(correlation_id: Uuid) -> TaskAttempt {
    TaskAttempt::new(
        Uuid::new_v4(),
        TaskFormat::of::<P>().unwrap(),
        json!({}),
        json!({ "correlation_id": correlation_id }),
        1,
    )
    .unwrap()
}

/// Fox's B4 ticket, always the same booking.
pub fn ticket() -> RecordTicketV1 {
    RecordTicketV1 {
        booking_id: Uuid::from_u128(1),
        concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
        seat: SeatId::parse("B4").unwrap(),
        zone: Zone::Standard,
        price: 80,
        fan_id: FanId::parse("fox").unwrap(),
        fan_name: "Fox".into(),
    }
}
