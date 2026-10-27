//! What the box office asks of a fan's ledger, built from its own facts: the contracts are the ledger's
//! ([`RecordTicketV1`], [`RecordCancellationV1`]); everything comes from the fact, nothing is read.
use crate::{
    concert::domain::events::{BookingCancelled, SeatBooked},
    fan::application::{record_cancellation::RecordCancellationV1, record_ticket::RecordTicketV1},
    shared::{application::tasks::new_task, domain::ConcertId},
};
use cqrsx::task::{NewTask, TaskValidationError};

/// The booking as the fan's ledger will record it: everything comes from the fact, the price paid included.
pub fn record_ticket(concert_id: &ConcertId, booked: &SeatBooked, max_attempts: i32) -> Result<NewTask, TaskValidationError> {
    let payload = RecordTicketV1 {
        booking_id: booked.booking_id,
        concert_id: concert_id.clone(),
        seat: booked.seat.clone(),
        zone: booked.zone,
        price: booked.price,
        fan_id: booked.fan_id.clone(),
        fan_name: booked.fan_name.clone(),
    };
    new_task(payload, concert_id, &booked.fan_id, max_attempts)
}

/// The cancellation as the fan's ledger will record it: the booking and the refund ("what was paid back
/// then") come from the fact the box office decided, never from a read before the command.
pub fn record_cancellation(
    concert_id: &ConcertId,
    cancelled: &BookingCancelled,
    max_attempts: i32,
) -> Result<NewTask, TaskValidationError> {
    let payload = RecordCancellationV1 {
        booking_id: cancelled.booking_id,
        cancellation_id: cancelled.cancellation_id,
        concert_id: concert_id.clone(),
        seat: cancelled.seat.clone(),
        fan_id: cancelled.fan_id.clone(),
        refund: cancelled.refund,
    };
    new_task(payload, concert_id, &cancelled.fan_id, max_attempts)
}
