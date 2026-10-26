//! A fan ledger over in-memory storage, task attempts and decisions built by hand, for the workflow tests.
use crate::{
    application::{
        activity::ActivityHub,
        command_service::CommandService,
        fan::FanApp,
        workflows::{fan_tasks, ticket_delivery::tasks::RecordTicketV1},
    },
    domain::{
        ConcertId, FanId, FanLedgerId,
        concert::{
            ConcertCommand, ConcertEvent,
            commands::BookSeat,
            events::{BookingCancelled, SeatBooked},
        },
        fan::{Fan, FanCommand, FanEvent, commands::RecordTicket, events::TicketRecorded},
        shared::venue::{SeatId, Zone},
    },
};
use cqrsx::{
    cqrs::{
        aggregate::Aggregate,
        command::planner::{Decision, TaskPlanError, TaskPlanner},
        testing::{command::TestCommandExecutor, mem_store::MemStore},
    },
    task::{NewTask, attempt::TaskAttempt, format::TaskPayload},
};
use serde_json::json;
use std::{collections::HashMap, sync::Arc};
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

/// What `planner` plans for `events` decided on `aggregate_id`, the decision built by hand (the planners
/// read neither the state nor the command).
pub fn plan<A: Aggregate, P: TaskPlanner<A>>(
    planner: &P,
    aggregate_id: &str,
    command: &A::Command,
    events: &[A::Event],
) -> Result<Vec<NewTask>, TaskPlanError> {
    let state = A::default();
    planner.plan(&Decision {
        aggregate_id,
        before: &state,
        after: &state,
        command,
        events,
        metadata: &HashMap::new(),
        causation: None,
    })
}

/// Fox's VIP seat A1 at 120, booked as `booking_id`.
pub fn booked(booking_id: Uuid) -> ConcertEvent {
    ConcertEvent::SeatBooked(SeatBooked {
        seat: SeatId::parse("A1").unwrap(),
        zone: Zone::Vip,
        price: 120,
        fan_id: FanId::parse("fox").unwrap(),
        fan_name: "Fox".into(),
        booking_id,
    })
}

/// The cancellation of Fox's A1, refunding the 120 paid.
pub fn cancelled_booking(booking_id: Uuid, cancellation_id: Uuid) -> ConcertEvent {
    ConcertEvent::BookingCancelled(BookingCancelled {
        seat: SeatId::parse("A1").unwrap(),
        fan_id: FanId::parse("fox").unwrap(),
        booking_id,
        cancellation_id,
        refund: 120,
    })
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

/// Some box-office command: the planners never read it.
pub fn concert_command() -> ConcertCommand {
    ConcertCommand::BookSeat(BookSeat {
        seat: "A1".into(),
        fan_id: FanId::parse("fox").unwrap(),
        fan_name: "Fox".into(),
        booking_id: Uuid::nil(),
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
