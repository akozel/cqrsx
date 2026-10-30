//! A fan ledger over in-memory storage, task attempts and their contexts, and the facts and commands of decisions
//! built by hand, for the tests of the ledger's tasks and reactions.
use crate::shared::application::policies::CHAIN_RETRY_AFTER;
use crate::{
    fan::{
        application::record_ticket::RecordTicketV1,
        domain::{Fan, FanCommand, FanEvent, commands::RecordTicket, events::TicketRecorded},
    },
    shared::domain::{
        ConcertId, FanId, FanLedgerId,
        venue::{SeatId, Zone},
    },
};
use cqrsx::{
    cqrs::{
        command::CommandExecutor,
        reactions::ReactionsPlanner,
        testing::{command_executor::TestCommandExecutor, mem_store::MemStore},
    },
    task::{
        attempt::TaskAttempt,
        context::{Executors, TaskContext},
        format::TaskPayload,
    },
};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

/// The ledgers' executor, which plans with its reactions as in production, for the handlers, and a clone of it (for
/// the task records).
pub fn fans(store: MemStore<Fan>) -> (Arc<TestCommandExecutor<Fan>>, TestCommandExecutor<Fan>) {
    let executor =
        TestCommandExecutor::new(store, ()).with_task_planner(ReactionsPlanner::<Fan>::new(crate::testing::policies()));
    (Arc::new(executor.clone()), executor)
}

/// The first claim of `payload`, carrying the correlation id of the command that published it.
pub fn attempt<P: TaskPayload>(payload: &P, correlation_id: Uuid) -> TaskAttempt {
    TaskAttempt::try_first(payload, json!({ "correlation_id": correlation_id })).unwrap()
}

/// The context of a step of the ledger: `attempt`, the step's pause, `fans` as the ledgers' executor.
pub fn steps<E: CommandExecutor<Fan> + 'static>(fans: &Arc<E>, attempt: TaskAttempt) -> TaskContext {
    TaskContext::new(attempt, CHAIN_RETRY_AFTER).with_executors(Executors::default().with::<Fan, E>(fans.clone()).unwrap())
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

/// Some ledger command: the reactions never read it.
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
