//! Commands of the `Concert` aggregate, one module per command: the intent and the rules that
//! decide it. The enum below is what the aggregate accepts.
pub mod announce_concert;
pub mod book_seat;
pub mod cancel_booking;

pub use announce_concert::AnnounceConcert;
pub use book_seat::BookSeat;
pub use cancel_booking::CancelBooking;

use crate::domain::FanId;

/// Internal command shape; the HTTP DTO is a separate type that fills the generated identifiers
/// (`booking_id` / `cancellation_id` = `command_id`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConcertCommand {
    AnnounceConcert(AnnounceConcert),
    BookSeat(BookSeat),
    CancelBooking(CancelBooking),
}

impl ConcertCommand {
    /// The command type the activity log reports.
    pub fn name(&self) -> &'static str {
        match self {
            ConcertCommand::AnnounceConcert(_) => "AnnounceConcert",
            ConcertCommand::BookSeat(_) => "BookSeat",
            ConcertCommand::CancelBooking(_) => "CancelBooking",
        }
    }

    pub fn fan_id(&self) -> Option<&FanId> {
        match self {
            ConcertCommand::AnnounceConcert(_) => None,
            ConcertCommand::BookSeat(command) => Some(&command.fan_id),
            ConcertCommand::CancelBooking(command) => Some(&command.fan_id),
        }
    }
}

impl From<AnnounceConcert> for ConcertCommand {
    fn from(command: AnnounceConcert) -> Self {
        ConcertCommand::AnnounceConcert(command)
    }
}

impl From<BookSeat> for ConcertCommand {
    fn from(command: BookSeat) -> Self {
        ConcertCommand::BookSeat(command)
    }
}

impl From<CancelBooking> for ConcertCommand {
    fn from(command: CancelBooking) -> Self {
        ConcertCommand::CancelBooking(command)
    }
}
