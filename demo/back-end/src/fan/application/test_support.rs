//! A fan ledger over in-memory storage, task attempts and the facts and commands of decisions built by hand, for
//! the tests of the ledger's tasks and reactions.
use crate::{
    fan::{
        application::{app::FanApp, reactions::fan_tasks, record_ticket::RecordTicketV1},
        domain::{Fan, FanCommand, FanEvent, commands::RecordTicket, events::TicketRecorded},
    },
    shared::{
        application::{activity::ActivityHub, command_service::CommandService},
        domain::{
            ConcertId, FanId, FanLedgerId,
            venue::{SeatId, Zone},
        },
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

/// The ledger, whose executor plans as in production, and a clone of the executor (for the task records).
pub fn fans(store: MemStore<Fan>) -> (Arc<TestFans>, TestCommandExecutor<Fan>) {
    let executor = TestCommandExecutor::new(store, ()).with_task_planner(fan_tasks(6));
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

/// Fox's B4 ticket in the ledger.
pub fn recorded(booking_id: Uuid) -> FanEvent {
    FanEvent::TicketRecorded(TicketRecorded {
        booking_id,
        concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
        seat: SeatId::parse("B4").unwrap(),
        zone: Zone::Standard,
        price: 80,
        fan_name: "Fox".into(),
    })
}

/// Some ledger command: the planners never read it.
pub fn fan_command() -> FanCommand {
    let ticket = ticket();
    FanCommand::RecordTicket(RecordTicket {
        booking_id: ticket.booking_id,
        concert_id: ticket.concert_id,
        seat: ticket.seat,
        zone: ticket.zone,
        price: ticket.price,
        fan_name: ticket.fan_name,
    })
}
