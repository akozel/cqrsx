use thiserror::Error;
use uuid::Uuid;

/// Technical rules R9–R11. Task handlers map `AlreadyRecorded` / `AlreadyCancelled` to "already
/// done" (task completed) and `UnknownTicket` to "too early, retry" (see `outbox::chain`).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FanError {
    #[error("ticket {booking_id} is already recorded")]
    AlreadyRecorded { booking_id: Uuid },
    #[error("ticket {booking_id} is unknown (not recorded yet)")]
    UnknownTicket { booking_id: Uuid },
    #[error("ticket {booking_id} is already cancelled")]
    AlreadyCancelled { booking_id: Uuid },
}

impl FanError {
    pub fn code(&self) -> &'static str {
        match self {
            FanError::AlreadyRecorded { .. } => "already_recorded",
            FanError::UnknownTicket { .. } => "unknown_ticket",
            FanError::AlreadyCancelled { .. } => "already_cancelled",
        }
    }

    pub fn params(&self) -> serde_json::Value {
        match self {
            FanError::AlreadyRecorded { booking_id }
            | FanError::UnknownTicket { booking_id }
            | FanError::AlreadyCancelled { booking_id } => serde_json::json!({ "booking_id": booking_id }),
        }
    }
}
