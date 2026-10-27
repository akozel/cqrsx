//! `POST /api/concerts/{concert_id}/commands` — the only write endpoint. The UI never validates
//! business rules: every click becomes a command and the aggregate is the judge.
use crate::{
    bootstrap::AppState,
    concert::domain::{
        ConcertCommand, ConcertError,
        commands::{AnnounceConcert, BookSeat, CancelBooking},
    },
    shared::{
        domain::{ConcertId, FanId},
        presentation::{error::ApiError, parse_concert_id},
    },
};
use axum::{
    Json,
    extract::{Path, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use chrono::NaiveDate;
use cqrsx::cqrs::{
    command::{CommandExecutor, CommandGatewayError, metadata::CommandMetadata},
    rejection::Rejection,
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

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
            ConcertCommand::AnnounceConcert(AnnounceConcert {
                artist: artist.trim().to_owned(),
                city: city.trim().to_owned(),
                date,
            })
        }
        CommandDto::BookSeat { seat, fan_id, fan_name } => ConcertCommand::BookSeat(BookSeat {
            seat,
            fan_id: fan(&fan_id)?,
            fan_name: if fan_name.trim().is_empty() {
                fan_id.clone()
            } else {
                fan_name.trim().to_owned()
            },
            // The booking id is the command id: the one fact the whole chain refers to.
            booking_id: metadata.causation.command_id,
        }),
        CommandDto::CancelBooking { seat, fan_id } => ConcertCommand::CancelBooking(CancelBooking {
            seat,
            fan_id: fan(&fan_id)?,
            cancellation_id: metadata.causation.command_id,
        }),
    })
}

pub async fn handle(
    State(state): State<AppState>,
    Path(concert_id): Path<String>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    let Json(raw) = body.map_err(|e| ApiError::invalid(format!("invalid JSON body: {e}")))?;
    let dto: CommandDto = serde_json::from_value(raw).map_err(|e| ApiError::invalid(format!("invalid command: {e}")))?;
    let metadata = CommandMetadata::root("ui");
    let command = to_command(dto, &metadata)?;
    let command_id = metadata.causation.command_id;
    let result = state
        .concerts
        .execute_with_metadata(&concert_id, command, metadata.to_map())
        .await
        .map(|report| report.tasks.iter().map(|task| task.format().to_string()).collect());
    Ok(answer(result, command_id, &concert_id))
}

/// The answer to a command: `200` with the formats of the tasks its commit published, `422` with the code and the
/// params of its rejection, `409` after a conflict, `503` when it did not run; every answer names the command and the
/// concert.
fn answer(result: Result<Vec<String>, CommandGatewayError<ConcertError>>, command_id: Uuid, concert_id: &ConcertId) -> Response {
    let base = json!({ "command_id": command_id, "concert_id": concert_id });
    match result {
        // Committed: the read models follow from the journal (D-17); the feed and the status tell when.
        Ok(tasks) => (
            StatusCode::OK,
            Json(json!({ "outcome": "accepted", "command_id": command_id, "concert_id": concert_id, "tasks": tasks })),
        )
            .into_response(),
        Err(CommandGatewayError::Domain(error)) => {
            ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "rejected", error.code(), error.to_string())
                .with_params(error.params())
                .with_extra(base)
                .into_response()
        }
        Err(CommandGatewayError::Conflict) => ApiError::new(
            StatusCode::CONFLICT,
            "conflict",
            "conflict",
            "the concert changed concurrently; retries exhausted",
        )
        .with_extra(base)
        .into_response(),
        Err(CommandGatewayError::Unavailable) => {
            ApiError::unavailable("the command did not run: the event store is unavailable or the concert is busy; retry")
                .with_extra(base)
                .into_response()
        }
    }
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
        let metadata = CommandMetadata::root("ui");
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
            matches!(book, ConcertCommand::BookSeat(BookSeat { booking_id, ref fan_name, .. }) if booking_id == metadata.causation.command_id && fan_name == "fox")
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
            matches!(cancel, ConcertCommand::CancelBooking(CancelBooking { cancellation_id, .. }) if cancellation_id == metadata.causation.command_id)
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

    /// The body of a response, as JSON.
    async fn body(response: Response) -> (StatusCode, Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn every_outcome_answers_with_its_status_and_shape() {
        let (command_id, concert_id) = (Uuid::now_v7(), ConcertId::parse("c-1").unwrap());
        let respond = |result| answer(result, command_id, &concert_id);
        let seat = crate::shared::domain::venue::SeatId::parse("B4").unwrap();
        let taken = ConcertError::SeatTaken { seat, by: "Fox".into() };
        assert_eq!(
            body(respond(Ok(vec!["encore_record_ticket v1".to_owned()]))).await,
            (
                StatusCode::OK,
                json!({ "outcome": "accepted", "command_id": command_id, "concert_id": "c-1",
                        "tasks": ["encore_record_ticket v1"] })
            )
        );
        assert_eq!(
            body(respond(Err(CommandGatewayError::Domain(taken)))).await,
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({ "outcome": "rejected",
                        "error": { "code": "seat_taken", "params": { "seat": "B4", "by": "Fox" },
                                   "message": "seat B4 is already taken by Fox" },
                        "command_id": command_id, "concert_id": "c-1" })
            )
        );
        assert_eq!(
            body(respond(Err(CommandGatewayError::Conflict))).await,
            (
                StatusCode::CONFLICT,
                json!({ "outcome": "conflict",
                        "error": { "code": "conflict", "params": {},
                                   "message": "the concert changed concurrently; retries exhausted" },
                        "command_id": command_id, "concert_id": "c-1" })
            )
        );
        assert_eq!(
            body(respond(Err(CommandGatewayError::Unavailable))).await,
            (
                StatusCode::SERVICE_UNAVAILABLE,
                json!({ "outcome": "unavailable",
                        "error": { "code": "unavailable", "params": {},
                                   "message": "the command did not run: the event store is unavailable or the concert is busy; retry" },
                        "command_id": command_id, "concert_id": "c-1" })
            )
        );
    }
}
