use crate::shared::domain::venue::SeatId;
use cqrsx::domain::rejection::Rejection;
use thiserror::Error;

/// Business-rule rejections R1–R8 as the API reports them (`{ code, params }`); texts live in the
/// front-end i18n. The code (`#[derive(Rejection)]`) is the variant in snake case, the stable key the
/// i18n files translate; the params are its fields, which the texts interpolate.
/// `PartialEq` + `Clone` are required by `TestFramework::then_expect_error` and by
/// `CommandError`.
#[derive(Debug, Clone, PartialEq, Eq, Error, Rejection)]
pub enum ConcertError {
    #[error("this concert has not been announced")]
    ConcertNotFound,
    #[error("this concert has already been announced")]
    AlreadyAnnounced,
    #[error("seat {seat} does not exist: rows A–E, seats 1–8")]
    UnknownSeat { seat: String },
    #[error("sold out! no tickets left")]
    SoldOut,
    #[error("seat {seat} is already taken by {by}")]
    SeatTaken { seat: SeatId, by: String },
    #[error("anti-scalper rule: max {limit} tickets per fan")]
    TicketLimit { limit: usize },
    #[error("nobody has booked seat {seat}")]
    SeatNotBooked { seat: SeatId },
    #[error("not your ticket: only the owner can cancel it")]
    NotYourTicket,
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::domain::rejection::Rejection;
    use serde_json::{Value, json};

    /// The codes are the API contract: the front-end i18n files translate exactly these keys.
    #[test]
    fn rejection_codes_are_stable() {
        let seat = SeatId::parse("B4").unwrap();
        let codes: Vec<&str> = [
            ConcertError::ConcertNotFound,
            ConcertError::AlreadyAnnounced,
            ConcertError::UnknownSeat { seat: "Z9".into() },
            ConcertError::SoldOut,
            ConcertError::SeatTaken {
                seat: seat.clone(),
                by: "Fox".into(),
            },
            ConcertError::TicketLimit { limit: 4 },
            ConcertError::SeatNotBooked { seat },
            ConcertError::NotYourTicket,
        ]
        .iter()
        .map(ConcertError::code)
        .collect();
        assert_eq!(
            codes,
            [
                "concert_not_found",
                "already_announced",
                "unknown_seat",
                "sold_out",
                "seat_taken",
                "ticket_limit",
                "seat_not_booked",
                "not_your_ticket"
            ]
        );
    }

    #[test]
    fn rejections_carry_what_the_ui_texts_interpolate() {
        let seat = SeatId::parse("B4").unwrap();
        let taken = ConcertError::SeatTaken { seat, by: "Fox".into() };
        assert_eq!(Rejection::code(&taken), "seat_taken");
        assert_eq!(taken.parameters(), json!({ "seat": "B4", "by": "Fox" }));
        assert_eq!(ConcertError::TicketLimit { limit: 4 }.parameters(), json!({ "limit": 4 }));
        assert_eq!(
            ConcertError::UnknownSeat { seat: "Z9".into() }.parameters(),
            json!({ "seat": "Z9" })
        );
        assert_eq!(ConcertError::NotYourTicket.parameters(), json!({}));
    }

    /// Every rule R1–R8 as the API and the activity feed report it, variant by variant: the code and the fields the UI
    /// texts interpolate.
    #[test]
    fn every_rejection_reports_its_code_and_the_fields_of_its_variant() {
        let seat = SeatId::parse("B4").unwrap();
        let reported: Vec<(&str, Value)> = [
            ConcertError::ConcertNotFound,
            ConcertError::AlreadyAnnounced,
            ConcertError::UnknownSeat { seat: "Z9".into() },
            ConcertError::SoldOut,
            ConcertError::SeatTaken {
                seat: seat.clone(),
                by: "Fox".into(),
            },
            ConcertError::TicketLimit { limit: 4 },
            ConcertError::SeatNotBooked { seat },
            ConcertError::NotYourTicket,
        ]
        .iter()
        .map(|error| (Rejection::code(error), Rejection::parameters(error)))
        .collect();
        assert_eq!(
            reported,
            [
                ("concert_not_found", json!({})),
                ("already_announced", json!({})),
                ("unknown_seat", json!({ "seat": "Z9" })),
                ("sold_out", json!({})),
                ("seat_taken", json!({ "seat": "B4", "by": "Fox" })),
                ("ticket_limit", json!({ "limit": 4 })),
                ("seat_not_booked", json!({ "seat": "B4" })),
                ("not_your_ticket", json!({})),
            ]
        );
    }
}
