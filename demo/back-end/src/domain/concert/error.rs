use crate::domain::shared::venue::SeatId;
use serde_json::{Value, json};
use thiserror::Error;

/// Business-rule rejections R1–R8. `code()` and `params()` feed the API (`{ code, params }`);
/// user-facing texts live in the front-end i18n files. `PartialEq` + `Clone` are required by
/// `TestFramework::then_expect_error` and by `CommandGatewayError`.
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

    pub fn params(&self) -> Value {
        match self {
            ConcertError::UnknownSeat { seat } => json!({ "seat": seat }),
            ConcertError::SeatTaken { seat, by } => json!({ "seat": seat, "by": by }),
            ConcertError::TicketLimit { limit } => json!({ "limit": limit }),
            ConcertError::SeatNotBooked { seat } => json!({ "seat": seat }),
            _ => json!({}),
        }
    }
}
