//! The answer to `POST /api/concerts/{concert_id}/commands`: the outcome of the command and the status it picks.
use crate::{
    concert::domain::ConcertError,
    shared::{domain::ConcertId, presentation::http::error::ApiError},
};
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use cqrsx::{command::CommandError, domain::rejection::Rejection};
use serde::Serialize;
use uuid::Uuid;

/// What became of a command. `200` with the formats of the tasks its commit published, `422` with the code and the
/// params of its rejection, `409` after a conflict, `503` when it did not run; every answer names the command and the
/// concert.
#[derive(Debug)]
pub struct ConcertCommandResponse {
    command_id: Uuid,
    concert_id: ConcertId,
    outcome: Result<Vec<String>, CommandError<ConcertError>>,
}

/// The body of `200`: `{ outcome: "accepted", command_id, concert_id, tasks }`.
#[derive(Debug, Serialize)]
struct Accepted {
    outcome: &'static str,
    command_id: Uuid,
    concert_id: ConcertId,
    tasks: Vec<String>,
}

/// What every answer says of the command, next to the error of a failed one.
#[derive(Debug, Serialize)]
struct Ids {
    command_id: Uuid,
    concert_id: ConcertId,
}

impl ConcertCommandResponse {
    /// The answer to command `command_id` on `concert_id`, whose outcome is `tasks` (the formats of the tasks its
    /// commit published) or the error of the command.
    pub fn new(command_id: Uuid, concert_id: ConcertId, outcome: Result<Vec<String>, CommandError<ConcertError>>) -> Self {
        Self {
            command_id,
            concert_id,
            outcome,
        }
    }
}

impl IntoResponse for ConcertCommandResponse {
    fn into_response(self) -> Response {
        let Self {
            command_id,
            concert_id,
            outcome,
        } = self;
        let ids = Ids { command_id, concert_id };
        match outcome {
            // Committed: the read models follow from the journal (D-17); the feed and the status tell when.
            Ok(tasks) => (
                StatusCode::OK,
                Json(Accepted {
                    outcome: "accepted",
                    command_id: ids.command_id,
                    concert_id: ids.concert_id,
                    tasks,
                }),
            )
                .into_response(),
            Err(CommandError::Rejected(error)) => {
                ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "rejected", error.code(), error.to_string())
                    .with_params(error.parameters())
                    .with_extra(&ids)
                    .into_response()
            }
            Err(CommandError::Conflict) => ApiError::new(
                StatusCode::CONFLICT,
                "conflict",
                "conflict",
                "the concert changed concurrently; retries exhausted",
            )
            .with_extra(&ids)
            .into_response(),
            Err(CommandError::Unavailable) => {
                ApiError::unavailable("the command did not run: the event store is unavailable or the concert is busy; retry")
                    .with_extra(&ids)
                    .into_response()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    /// The body of a response, as JSON.
    async fn body(response: Response) -> (StatusCode, Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn every_outcome_answers_with_its_status_and_shape() {
        let (command_id, concert_id) = (Uuid::now_v7(), ConcertId::parse("c-1").unwrap());
        let respond = |outcome| ConcertCommandResponse::new(command_id, concert_id.clone(), outcome).into_response();
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
            body(respond(Err(CommandError::Rejected(taken)))).await,
            (
                StatusCode::UNPROCESSABLE_ENTITY,
                json!({ "outcome": "rejected",
                        "error": { "code": "seat_taken", "params": { "seat": "B4", "by": "Fox" },
                                   "message": "seat B4 is already taken by Fox" },
                        "command_id": command_id, "concert_id": "c-1" })
            )
        );
        assert_eq!(
            body(respond(Err(CommandError::Conflict))).await,
            (
                StatusCode::CONFLICT,
                json!({ "outcome": "conflict",
                        "error": { "code": "conflict", "params": {},
                                   "message": "the concert changed concurrently; retries exhausted" },
                        "command_id": command_id, "concert_id": "c-1" })
            )
        );
        assert_eq!(
            body(respond(Err(CommandError::Unavailable))).await,
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
