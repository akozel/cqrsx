//! `POST /api/concerts/{concert_id}/commands` — the only write endpoint. The UI never validates
//! business rules: every click becomes a command and the aggregate is the judge.
use crate::{
    application::command_service::{CommandMetadata, CommandOutcome},
    bootstrap::AppState,
    domain::{FanId, concert::ConcertCommand},
    presentation::http::{error::ApiError, parse_concert_id},
};
use axum::{
    Json,
    extract::{Path, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::NaiveDate;
use serde::Deserialize;
use serde_json::{Value, json};

/// Wire shape of a UI command. Internal `Fan` commands are deliberately not representable here.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum CommandDto {
    AnnounceConcert { artist: String, city: String, date: NaiveDate },
    BookSeat { seat: String, fan_id: String, fan_name: String },
    CancelBooking { seat: String, fan_id: String },
}

fn to_command(dto: CommandDto, metadata: &CommandMetadata) -> Result<ConcertCommand, ApiError> {
    let fan = |raw: &str| FanId::parse(raw).map_err(|e| ApiError::invalid(e.to_string()));
    Ok(match dto {
        CommandDto::AnnounceConcert { artist, city, date } => {
            if artist.trim().is_empty() || city.trim().is_empty() {
                return Err(ApiError::invalid("artist and city must not be empty"));
            }
            ConcertCommand::AnnounceConcert {
                artist: artist.trim().to_owned(),
                city: city.trim().to_owned(),
                date,
            }
        }
        CommandDto::BookSeat { seat, fan_id, fan_name } => ConcertCommand::BookSeat {
            seat,
            fan_id: fan(&fan_id)?,
            fan_name: if fan_name.trim().is_empty() {
                fan_id.clone()
            } else {
                fan_name.trim().to_owned()
            },
            // The booking id is the command id: the one fact the whole chain refers to.
            booking_id: metadata.command_id,
        },
        CommandDto::CancelBooking { seat, fan_id } => ConcertCommand::CancelBooking {
            seat,
            fan_id: fan(&fan_id)?,
            cancellation_id: metadata.command_id,
        },
    })
}

pub async fn execute(
    State(state): State<AppState>,
    Path(concert_id): Path<String>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    // Lock check first (§6.4 step 1): even an invalid command is told the box office is closed.
    if state.rebuild.is_locked() {
        return Err(ApiError::locked().with_extra(json!({ "concert_id": concert_id })));
    }
    let Json(raw) = body.map_err(|e| ApiError::invalid(format!("invalid JSON body: {e}")))?;
    let dto: CommandDto = serde_json::from_value(raw).map_err(|e| ApiError::invalid(format!("invalid command: {e}")))?;
    let metadata = CommandMetadata::ui();
    let command = to_command(dto, &metadata)?;
    let command_id = metadata.command_id;
    let executed = state.concert_commands.execute(&concert_id, command, metadata).await;
    let base = json!({ "command_id": command_id, "concert_id": concert_id });
    Ok(match executed.outcome {
        CommandOutcome::Accepted { warnings } => (
            StatusCode::OK,
            Json(
                json!({ "outcome": "accepted", "command_id": command_id, "concert_id": concert_id,
                         "tasks": executed.tasks, "warnings": warnings }),
            ),
        )
            .into_response(),
        CommandOutcome::Rejected(error) => {
            ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "rejected", error.code(), error.to_string())
                .with_params(error.params())
                .with_extra(base)
                .into_response()
        }
        CommandOutcome::Conflict => ApiError::new(
            StatusCode::CONFLICT,
            "conflict",
            "conflict",
            "the concert changed concurrently; retries exhausted",
        )
        .with_extra(base)
        .into_response(),
        CommandOutcome::Unavailable => ApiError::unavailable("the event store is unavailable")
            .with_extra(base)
            .into_response(),
        CommandOutcome::Locked => ApiError::locked().with_extra(base).into_response(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dto_rejects_internal_commands_and_unknown_fields() {
        assert!(serde_json::from_value::<CommandDto>(json!({ "type": "RecordTicket" })).is_err());
        assert!(
            serde_json::from_value::<CommandDto>(
                json!({ "type": "BookSeat", "seat": "B4", "fan_id": "fox", "fan_name": "Fox", "booking_id": "x" })
            )
            .is_err()
        );
        assert!(
            serde_json::from_value::<CommandDto>(json!({ "type": "BookSeat", "seat": "B4", "fan_id": "fox", "fan_name": "Fox" }))
                .is_ok()
        );
    }

    #[test]
    fn booking_and_cancellation_ids_are_the_command_id() {
        let metadata = CommandMetadata::ui();
        let book = to_command(
            CommandDto::BookSeat {
                seat: "B4".into(),
                fan_id: "fox".into(),
                fan_name: "  ".into(),
            },
            &metadata,
        )
        .unwrap();
        assert!(
            matches!(book, ConcertCommand::BookSeat { booking_id, ref fan_name, .. } if booking_id == metadata.command_id && fan_name == "fox")
        );
        let cancel = to_command(
            CommandDto::CancelBooking {
                seat: "B4".into(),
                fan_id: "fox".into(),
            },
            &metadata,
        )
        .unwrap();
        assert!(
            matches!(cancel, ConcertCommand::CancelBooking { cancellation_id, .. } if cancellation_id == metadata.command_id)
        );
        assert!(
            to_command(
                CommandDto::CancelBooking {
                    seat: "B4".into(),
                    fan_id: "Fox!".into()
                },
                &metadata
            )
            .is_err()
        );
    }
}
