//! Task contracts the refund steps publish.
use crate::domain::{ConcertId, FanId, shared::venue::SeatId};
use cqrsx::task::format::TaskPayload;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
