//! `POST /api/concerts/{concert_id}/commands` — the only write endpoint. The UI never validates
//! business rules: every click becomes a command and the aggregate is the judge.
mod request;
mod response;

use crate::{
    concert::domain::Concert,
    shared::presentation::http::{error::ApiError, parse_concert_id},
};
use axum::{
    Json,
    extract::{Path, State, rejection::JsonRejection},
};
use cqrsx_engine::Cqrs;
use request::ConcertCommandRequest;
use response::ConcertCommandResponse;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

/// The command goes to the engine from the source `ui`: its id is chosen here, so that every answer names it, and is
/// the id of a booking and a cancellation.
pub async fn handle(
    State(cqrs): State<Arc<Cqrs>>,
    Path(concert_id): Path<String>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<ConcertCommandResponse, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    let request = ConcertCommandRequest::from_body(body)?;
    let command_id = Uuid::now_v7();
    let command = request.into_command(command_id)?;
    let outcome = cqrs
        .source("ui")
        .execute_with_id::<Concert>(command_id, &concert_id, command)
        .await
        .map(|report| report.tasks.iter().map(|task| task.format().to_string()).collect());
    Ok(ConcertCommandResponse::new(command_id, concert_id, outcome))
}
