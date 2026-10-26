//! Task contracts of the refund and how [`Refund`](super::Refund) builds them from the facts it follows.
use crate::{
    application::tasks::new_task,
    domain::{
        ConcertId, FanId, FanLedgerId, concert::events::BookingCancelled, fan::events::TicketCancelled, shared::venue::SeatId,
    },
};
use cqrsx::task::{NewTask, TaskValidationError, format::TaskPayload};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `Concert::BookingCancelled` → `Fan.RecordCancellation`: step one of the refund
/// ([`record_cancellation`](super::record_cancellation)).
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

/// `Fan::TicketCancelled` → e-mail "💸 Refund" (step [`issue_refund`](super::issue_refund)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IssueRefundV1 {
    pub cancellation_id: Uuid,
    pub booking_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub fan_id: FanId,
    pub amount: u32,
}

impl TaskPayload for IssueRefundV1 {
    const TASK_TYPE: &'static str = "encore_issue_refund";
    const TASK_VERSION: i32 = 1;
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

/// The refund of a ticket the ledger cancelled; the fan is the ledger's.
pub fn issue_refund(
    ledger: &FanLedgerId,
    cancelled: &TicketCancelled,
    max_attempts: i32,
) -> Result<NewTask, TaskValidationError> {
    let fan_id = ledger.fan();
    let payload = IssueRefundV1 {
        cancellation_id: cancelled.cancellation_id,
        booking_id: cancelled.booking_id,
        concert_id: cancelled.concert_id.clone(),
        seat: cancelled.seat.clone(),
        fan_id: fan_id.clone(),
        amount: cancelled.refund,
    };
    new_task(payload, &cancelled.concert_id, &fan_id, max_attempts)
}
