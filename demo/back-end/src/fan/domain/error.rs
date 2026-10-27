use thiserror::Error;
use uuid::Uuid;

/// Technical rules R9–R11. Workflow steps map `AlreadyRecorded` / `AlreadyCancelled` to "already
/// done" (task completed) and `UnknownTicket` to "too early, retry" (see `fan::application::chain`).
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
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The codes are the contract of the activity log and the task-outcome mapping.
    #[test]
    fn rejection_codes_are_stable() {
        let booking_id = Uuid::nil();
        let codes: Vec<&str> = [
            FanError::AlreadyRecorded { booking_id },
            FanError::UnknownTicket { booking_id },
            FanError::AlreadyCancelled { booking_id },
        ]
        .iter()
        .map(FanError::code)
        .collect();
        assert_eq!(codes, ["already_recorded", "unknown_ticket", "already_cancelled"]);
    }
}
