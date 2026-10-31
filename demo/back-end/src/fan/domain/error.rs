use cqrsx::Rejection;
use thiserror::Error;
use uuid::Uuid;

/// Technical rules R9–R11. Workflow steps map `AlreadyRecorded` / `AlreadyCancelled` to "already
/// done" (task completed) and `UnknownTicket` to "too early, retry" (see `fan::application::record_ticket`).
/// The activity log reports them by their code, the variant in snake case, and the booking
/// (`#[derive(Rejection)]`).
#[derive(Debug, Clone, PartialEq, Eq, Error, Rejection)]
pub enum FanError {
    #[error("ticket {booking_id} is already recorded")]
    AlreadyRecorded { booking_id: Uuid },
    #[error("ticket {booking_id} is unknown (not recorded yet)")]
    UnknownTicket { booking_id: Uuid },
    #[error("ticket {booking_id} is already cancelled")]
    AlreadyCancelled { booking_id: Uuid },
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::domain::rejection::Rejection;
    use serde_json::{Value, json};

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

    #[test]
    fn rejections_report_the_booking() {
        let booking_id = Uuid::nil();
        let error = FanError::UnknownTicket { booking_id };
        assert_eq!(Rejection::code(&error), "unknown_ticket");
        assert_eq!(error.params(), json!({ "booking_id": booking_id }));
    }

    /// Every rule R9–R11 as the activity feed reports it, variant by variant: the code and the booking.
    #[test]
    fn every_ledger_rejection_reports_its_code_and_its_booking() {
        let booking_id = Uuid::from_u128(1);
        let reported: Vec<(&str, Value)> = [
            FanError::AlreadyRecorded { booking_id },
            FanError::UnknownTicket { booking_id },
            FanError::AlreadyCancelled { booking_id },
        ]
        .iter()
        .map(|error| (Rejection::code(error), Rejection::params(error)))
        .collect();
        let booking = json!({ "booking_id": "00000000-0000-0000-0000-000000000001" });
        assert_eq!(
            reported,
            [
                ("already_recorded", booking.clone()),
                ("unknown_ticket", booking.clone()),
                ("already_cancelled", booking),
            ]
        );
    }
}
