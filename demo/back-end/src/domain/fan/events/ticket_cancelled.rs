//! Fact: a ticket in the fan's ledger is cancelled. Continues the
//! [refund](crate::application::workflows::refund) workflow through the `encore_issue_refund`
//! task the workflow plans from this event and commits with it.
use crate::domain::{ConcertId, shared::venue::SeatId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TicketCancelled {
    pub booking_id: Uuid,
    pub cancellation_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub refund: u32,
}
