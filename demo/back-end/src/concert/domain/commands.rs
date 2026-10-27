//! Commands of the `Concert` aggregate, one module per command: the intent and the rules that
//! decide it (`impl Decide<Concert>`). The enum below is what the aggregate accepts; the derive routes every
//! variant to its command's decision and reports it to the activity feed (name, `#[label]`, `#[command_id]`).
pub mod announce_concert;
pub mod book_seat;
pub mod cancel_booking;

pub use announce_concert::AnnounceConcert;
pub use book_seat::BookSeat;
pub use cancel_booking::CancelBooking;

use cqrsx::Commands;

/// Internal command shape; the HTTP DTO is a separate type that fills the generated identifiers
/// (`booking_id` / `cancellation_id` = `command_id`).
#[derive(Debug, Clone, PartialEq, Eq, Commands)]
pub enum ConcertCommand {
    AnnounceConcert(AnnounceConcert),
    BookSeat(BookSeat),
    CancelBooking(CancelBooking),
}

#[cfg(test)]
mod tests {
    use crate::concert::domain::test_support::{announce, book, cancel};
    use cqrsx::cqrs::command::CommandInfo;
    use uuid::Uuid;

    /// What the activity feed names a command by, and the id a booking and a cancellation share with their command.
    #[test]
    fn every_command_reports_its_name_its_labels_and_its_id() {
        let (booking_id, cancellation_id) = (Uuid::now_v7(), Uuid::now_v7());
        let reported = [
            announce(),
            book("B4", "fox", "Fox", booking_id),
            cancel("B4", "fox", cancellation_id),
        ]
        .iter()
        .map(|command| (command.name(), command.labels(), command.command_id()))
        .collect::<Vec<_>>();
        assert_eq!(
            reported,
            [
                ("AnnounceConcert", vec![], None),
                ("BookSeat", vec![("fan_id", "fox".to_owned())], Some(booking_id)),
                ("CancelBooking", vec![("fan_id", "fox".to_owned())], Some(cancellation_id)),
            ]
        );
    }
}
