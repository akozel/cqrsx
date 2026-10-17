use crate::domain::FanId;
use chrono::NaiveDate;
use uuid::Uuid;

/// Internal command shape; the HTTP DTO (`http::commands`) is a separate type that fills the
/// generated identifiers (`booking_id` / `cancellation_id` = `command_id`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConcertCommand {
    AnnounceConcert {
        artist: String,
        city: String,
        date: NaiveDate,
    },
    BookSeat {
        /// Raw seat text from the UI; the aggregate validates it (rule R3).
        seat: String,
        fan_id: FanId,
        fan_name: String,
        booking_id: Uuid,
    },
    CancelBooking {
        seat: String,
        fan_id: FanId,
        cancellation_id: Uuid,
    },
}

impl ConcertCommand {
    pub fn name(&self) -> &'static str {
        match self {
            ConcertCommand::AnnounceConcert { .. } => "AnnounceConcert",
            ConcertCommand::BookSeat { .. } => "BookSeat",
            ConcertCommand::CancelBooking { .. } => "CancelBooking",
        }
    }

    pub fn fan_id(&self) -> Option<&FanId> {
        match self {
            ConcertCommand::AnnounceConcert { .. } => None,
            ConcertCommand::BookSeat { fan_id, .. } | ConcertCommand::CancelBooking { fan_id, .. } => Some(fan_id),
        }
    }
}
