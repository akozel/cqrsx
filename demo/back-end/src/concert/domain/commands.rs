//! Commands of the `Concert` aggregate, one module per command: the intent and the rules that
//! decide it (`impl Decide<Concert>`). The enum below is what the aggregate accepts; the derive routes every
//! variant to its command's decision and reports it to the activity feed (name, `#[label]`).
pub mod announce_concert;
pub mod book_seat;
pub mod cancel_booking;

pub use announce_concert::AnnounceConcert;
pub use book_seat::BookSeat;
pub use cancel_booking::CancelBooking;

use cqrsx::domain::command::{Command, Decide};

/// Internal command shape; the HTTP DTO is a separate type that fills the generated identifiers
/// (`booking_id` / `cancellation_id` = `command_id`).
#[derive(Debug, Clone, PartialEq, Eq, Command, Decide)]
pub enum ConcertCommand {
    AnnounceConcert(AnnounceConcert),
    BookSeat(BookSeat),
    CancelBooking(CancelBooking),
}

#[cfg(test)]
mod tests {
    use crate::concert::domain::test_support::{announce, book, cancel};
    use cqrsx::domain::command::Command;
    use uuid::Uuid;

    /// What the activity feed names a command by and filters it by.
    #[test]
    fn every_command_reports_its_name_and_its_labels() {
        let reported = [
            announce(),
            book("B4", "fox", "Fox", Uuid::now_v7()),
            cancel("B4", "fox", Uuid::now_v7()),
        ]
        .iter()
        .map(|command| {
            let labels: Vec<(&str, String)> = command.labels().into_iter().map(|label| (label.name, label.value)).collect();
            (command.name(), labels)
        })
        .collect::<Vec<_>>();
        assert_eq!(
            reported,
            [
                ("AnnounceConcert", vec![]),
                ("BookSeat", vec![("fan_id", "fox".to_owned())]),
                ("CancelBooking", vec![("fan_id", "fox".to_owned())]),
            ]
        );
    }
}
