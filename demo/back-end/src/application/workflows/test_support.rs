//! A fan ledger over in-memory storage and task attempts, for the workflow step tests.
use crate::{
    application::{activity::ActivityHub, command_service::CommandService, concert::tasks::RecordTicketV1, fan::FanApp},
    domain::{
        ConcertId, FanId, FanLedgerId,
        fan::Fan,
        shared::venue::{SeatId, Zone},
    },
};
use cqrsx::{
    cqrs::testing::{command::TestCommandExecutor, mem_store::MemStore},
    task::{attempt::TaskAttempt, format::TaskPayload},
};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

pub type TestFans = FanApp<TestCommandExecutor<Fan>>;

/// The ledger and a clone of the executor it runs its commands through (for the task records).
pub fn fans(store: MemStore<Fan>) -> (Arc<TestFans>, TestCommandExecutor<Fan>) {
    let executor = TestCommandExecutor::new(store, ());
    let service = CommandService::new(executor.clone(), Arc::new(ActivityHub::new(16, 16)));
    (Arc::new(FanApp::new(service)), executor)
}

/// The first claim of `payload`, carrying the correlation id of the command that published it.
pub fn attempt<P: TaskPayload>(payload: &P, correlation_id: Uuid) -> TaskAttempt {
    TaskAttempt::first(payload, json!({ "correlation_id": correlation_id })).unwrap()
}

/// Fox's ledger for the Radiohead concert: the stream `ticket()` lands in.
pub fn ledger() -> FanLedgerId {
    let ticket = ticket();
    FanLedgerId::new(&ticket.fan_id, &ticket.concert_id)
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
