//! The body of `POST /api/concerts/{concert_id}/commands`: what the UI may ask, and the command it becomes.
use crate::{
    concert::domain::{
        ConcertCommand,
        commands::{AnnounceConcert, BookSeat, CancelBooking},
    },
    shared::{domain::FanId, presentation::http::error::ApiError},
};
use axum::{Json, extract::rejection::JsonRejection};
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

/// Wire shape of a UI command, `{ "type": "BookSeat", .. }`. Internal `Fan` commands are deliberately not
/// representable here.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ConcertCommandRequest {
    AnnounceConcert { artist: String, city: String, date: NaiveDate },
    BookSeat { seat: String, fan_id: String, fan_name: String },
    CancelBooking { seat: String, fan_id: String },
}

impl ConcertCommandRequest {
    /// The request in a body: `400` for a body that is not JSON and for JSON that is not a command of the UI.
    pub fn from_body(body: Result<Json<Value>, JsonRejection>) -> Result<Self, ApiError> {
        let Json(raw) = body.map_err(|e| ApiError::invalid(format!("invalid JSON body: {e}")))?;
        serde_json::from_value(raw).map_err(|e| ApiError::invalid(format!("invalid command: {e}")))
    }

    /// The command of the request, its text trimmed and checked (`400` otherwise); a booking and a cancellation are
    /// set to the id the command is executed with (`command_id`).
    pub fn into_command(self, command_id: Uuid) -> Result<ConcertCommand, ApiError> {
        let fan = |raw: &str| FanId::parse(raw).map_err(|e| ApiError::invalid(e.to_string()));
        Ok(match self {
            Self::AnnounceConcert { artist, city, date } => {
                if artist.trim().is_empty() || city.trim().is_empty() {
                    return Err(ApiError::invalid("artist and city must not be empty"));
                }
                ConcertCommand::AnnounceConcert(AnnounceConcert {
                    artist: artist.trim().to_owned(),
                    city: city.trim().to_owned(),
                    date,
                })
            }
            Self::BookSeat { seat, fan_id, fan_name } => ConcertCommand::BookSeat(BookSeat {
                seat,
                fan_id: fan(&fan_id)?,
                fan_name: if fan_name.trim().is_empty() {
                    fan_id
                } else {
                    fan_name.trim().to_owned()
                },
                // The booking is its command: the booking id is the command id, the one fact the whole chain refers to.
                booking_id: command_id,
            }),
            Self::CancelBooking { seat, fan_id } => ConcertCommand::CancelBooking(CancelBooking {
                seat,
                fan_id: fan(&fan_id)?,
                // The cancellation is its command: the cancellation id is the command id.
                cancellation_id: command_id,
            }),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_request_rejects_internal_commands_and_unknown_fields() {
        assert!(serde_json::from_value::<ConcertCommandRequest>(json!({ "type": "RecordTicket" })).is_err());
        assert!(
            serde_json::from_value::<ConcertCommandRequest>(
                json!({ "type": "BookSeat", "seat": "B4", "fan_id": "fox", "fan_name": "Fox", "booking_id": "x" })
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<ConcertCommandRequest>(
                json!({ "type": "BookSeat", "seat": "B4", "fan_id": "fox", "fan_name": "Fox" })
            )
            .is_ok()
        );
    }

    #[test]
    fn a_body_that_is_not_a_command_is_a_bad_request_with_its_reason() {
        let invalid = |body| ConcertCommandRequest::from_body(Ok(Json(body))).unwrap_err();
        let error = invalid(json!({ "type": "RecordTicket" }));
        assert_eq!(
            (error.status, error.code.as_str()),
            (axum::http::StatusCode::BAD_REQUEST, "invalid_request")
        );
        assert!(error.message.starts_with("invalid command"), "{}", error.message);
        assert!(
            ConcertCommandRequest::from_body(Ok(Json(json!({ "type": "CancelBooking", "seat": "B4", "fan_id": "fox" })))).is_ok()
        );
    }

    #[test]
    fn booking_and_cancellation_ids_are_the_command_id() {
        let command_id = Uuid::now_v7();
        let book = ConcertCommandRequest::BookSeat {
            seat: "B4".into(),
            fan_id: "fox".into(),
            fan_name: "  ".into(),
        }
        .into_command(command_id)
        .unwrap();
        assert!(
            matches!(book, ConcertCommand::BookSeat(BookSeat { booking_id, ref fan_name, .. }) if booking_id == command_id && fan_name == "fox")
        );
        let cancel = ConcertCommandRequest::CancelBooking {
            seat: "B4".into(),
            fan_id: "fox".into(),
        }
        .into_command(command_id)
        .unwrap();
        assert!(
            matches!(cancel, ConcertCommand::CancelBooking(CancelBooking { cancellation_id, .. }) if cancellation_id == command_id)
        );
    }

    #[test]
    fn the_input_is_checked_and_trimmed() {
        let command_id = Uuid::now_v7();
        let bad_fan = ConcertCommandRequest::CancelBooking {
            seat: "B4".into(),
            fan_id: "Fox!".into(),
        };
        assert!(
            bad_fan
                .into_command(command_id)
                .unwrap_err()
                .message
                .contains("invalid fan id")
        );
        let announce = |artist: &str, city: &str| ConcertCommandRequest::AnnounceConcert {
            artist: artist.into(),
            city: city.into(),
            date: NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
        };
        let blank = announce(" ", "Minsk").into_command(command_id).unwrap_err();
        assert_eq!(blank.message, "artist and city must not be empty");
        assert!(announce("Radiohead", "  ").into_command(command_id).is_err());
        let ConcertCommand::AnnounceConcert(announced) = announce(" Radiohead ", " Minsk ").into_command(command_id).unwrap()
        else {
            panic!("an announcement stays an announcement");
        };
        assert_eq!((announced.artist.as_str(), announced.city.as_str()), ("Radiohead", "Minsk"));
        let named = ConcertCommandRequest::BookSeat {
            seat: "B4".into(),
            fan_id: "fox".into(),
            fan_name: " Fox ".into(),
        };
        assert!(matches!(
            named.into_command(command_id).unwrap(),
            ConcertCommand::BookSeat(BookSeat { ref fan_name, .. }) if fan_name == "Fox"
        ));
    }
}
