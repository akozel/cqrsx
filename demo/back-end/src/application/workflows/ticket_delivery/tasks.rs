//! Task contracts the ticket-delivery steps publish.
use crate::domain::{ConcertId, FanId, shared::venue::SeatId};
use cqrsx::task::format::TaskPayload;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `Fan::TicketRecorded` → e-mail "🎫 Your e-ticket" (step [`send_e_ticket`](super::send_e_ticket)).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendETicketV1 {
    pub booking_id: Uuid,
    pub concert_id: ConcertId,
    pub seat: SeatId,
    pub fan_id: FanId,
    pub fan_name: String,
    pub price: u32,
}

impl TaskPayload for SendETicketV1 {
    const TASK_TYPE: &'static str = "encore_send_e_ticket";
    const TASK_VERSION: i32 = 1;
}
