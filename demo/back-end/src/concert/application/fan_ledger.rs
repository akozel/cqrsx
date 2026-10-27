//! What the box office asks of a fan's ledger, built from its own facts: the contracts are the ledger's
//! ([`RecordTicketV1`], [`RecordCancellationV1`]); everything comes from the fact, nothing is read.
use crate::{
    concert::domain::events::{BookingCancelled, SeatBooked},
    fan::application::{record_cancellation::RecordCancellationV1, record_ticket::RecordTicketV1},
    shared::domain::ConcertId,
};

/// The booking as the fan's ledger will record it: everything comes from the fact, the price paid included.
pub fn record_ticket(concert_id: &ConcertId, booked: &SeatBooked) -> RecordTicketV1 {
    RecordTicketV1 {
        booking_id: booked.booking_id,
        concert_id: concert_id.clone(),
        seat: booked.seat.clone(),
        zone: booked.zone,
        price: booked.price,
        fan_id: booked.fan_id.clone(),
        fan_name: booked.fan_name.clone(),
    }
}

/// The cancellation as the fan's ledger will record it: the booking and the refund ("what was paid back
/// then") come from the fact the box office decided, never from a read before the command.
pub fn record_cancellation(concert_id: &ConcertId, cancelled: &BookingCancelled) -> RecordCancellationV1 {
    RecordCancellationV1 {
        booking_id: cancelled.booking_id,
        cancellation_id: cancelled.cancellation_id,
        concert_id: concert_id.clone(),
        seat: cancelled.seat.clone(),
        fan_id: cancelled.fan_id.clone(),
        refund: cancelled.refund,
    }
}
