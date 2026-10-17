//! Task contracts the box office publishes: the first step of each workflow, built from the
//! command before it runs and committed atomically with its event. Names are `[a-z0-9_]` only
//! (decision D-05): the library's diagnostics print other names as `unknown`.
use crate::{
    application::{command_service::CommandMetadata, tasks::new_task},
    domain::{
        ConcertId, FanId,
        concert::{
            Booking,
            commands::{BookSeat, CancelBooking},
        },
        shared::venue::{SeatId, SeatInfo, Zone},
    },
};
use cqrsx::task::{NewTask, TaskValidationError, format::TaskPayload};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `Concert::SeatBooked` → `Fan.RecordTicket`: step one of
/// [ticket delivery](crate::application::workflows::ticket_delivery).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordTicketV1 {
    pub booking_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub zone: Zone,
    pub price: u32,
    pub fan_id: FanId,
    pub fan_name: String,
}

impl TaskPayload for RecordTicketV1 {
    const TASK_TYPE: &'static str = "encore_record_ticket";
    const TASK_VERSION: i32 = 1;
}

/// `Concert::BookingCancelled` → `Fan.RecordCancellation`: step one of
/// [refund](crate::application::workflows::refund).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordCancellationV1 {
    pub booking_id: Uuid,
    pub cancellation_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub fan_id: FanId,
    pub refund: u32,
}

impl TaskPayload for RecordCancellationV1 {
    const TASK_TYPE: &'static str = "encore_record_cancellation";
    const TASK_VERSION: i32 = 1;
}

/// The booking as the fan ledger will record it; zone and price come from the venue.
pub fn record_ticket(
    concert_id: &ConcertId,
    command: &BookSeat,
    seat: SeatInfo,
    metadata: &CommandMetadata,
    max_attempts: i32,
) -> Result<NewTask, TaskValidationError> {
    let payload = RecordTicketV1 {
        booking_id: command.booking_id,
        concert_id: concert_id.clone(),
        seat: seat.id,
        zone: seat.zone,
        price: seat.price,
        fan_id: command.fan_id.clone(),
        fan_name: command.fan_name.clone(),
    };
    new_task(payload, metadata, concert_id, &command.fan_id, max_attempts)
}

/// The cancellation of `booking`; the refund is what was paid back then.
pub fn record_cancellation(
    concert_id: &ConcertId,
    command: &CancelBooking,
    seat: SeatInfo,
    booking: &Booking,
    metadata: &CommandMetadata,
    max_attempts: i32,
) -> Result<NewTask, TaskValidationError> {
    let payload = RecordCancellationV1 {
        booking_id: booking.booking_id,
        cancellation_id: command.cancellation_id,
        concert_id: concert_id.clone(),
        seat: seat.id,
        fan_id: command.fan_id.clone(),
        refund: booking.price,
    };
    new_task(payload, metadata, concert_id, &command.fan_id, max_attempts)
}
