use crate::domain::venue::{SeatId, Zone};
use uuid::Uuid;

/// Internal commands, built by outbox task handlers from the task payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FanCommand {
    RecordTicket {
        booking_id: Uuid,
        concert_id: String,
        seat: SeatId,
        zone: Zone,
        price: u32,
        fan_name: String,
    },
    RecordCancellation {
        booking_id: Uuid,
        cancellation_id: Uuid,
        refund: u32,
    },
}

impl FanCommand {
    pub fn name(&self) -> &'static str {
        match self {
            FanCommand::RecordTicket { .. } => "RecordTicket",
            FanCommand::RecordCancellation { .. } => "RecordCancellation",
        }
    }
}
