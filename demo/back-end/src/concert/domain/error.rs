use crate::shared::domain::venue::SeatId;
use thiserror::Error;

/// Business-rule rejections R1–R8. `code()` is the stable key the API and the front-end i18n
/// files use; the parameters the texts interpolate are an application concern (`Rejection`).
/// `PartialEq` + `Clone` are required by `TestFramework::then_expect_error` and by
/// `CommandGatewayError`.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
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

impl ConcertError {
    pub fn code(&self) -> &'static str {
        match self {
            ConcertError::ConcertNotFound => "concert_not_found",
            ConcertError::AlreadyAnnounced => "already_announced",
            ConcertError::UnknownSeat { .. } => "unknown_seat",
            ConcertError::SoldOut => "sold_out",
            ConcertError::SeatTaken { .. } => "seat_taken",
            ConcertError::TicketLimit { .. } => "ticket_limit",
            ConcertError::SeatNotBooked { .. } => "seat_not_booked",
            ConcertError::NotYourTicket => "not_your_ticket",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
