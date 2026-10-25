//! `POST /api/projections/rebuild` — ⏪ Encore!: the owners of the checkpoints rebuild every read model from
//! the journal while the box office stays open (`202`). `pace_ms` is the pause between the events of one key
//! (D-18): the UI sends the slow-motion delay or 0.
use crate::{application::operations::rebuild::RebuildRefused, bootstrap::AppState, presentation::http::error::ApiError};
use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RebuildRequest {
    #[serde(default)]
    pace_ms: u64,
}

/// The answer to a refused rebuild request, shared with the rebuild of one hall.
pub fn refused(refusal: RebuildRefused) -> ApiError {
    match refusal {
        RebuildRefused::AlreadyRunning => ApiError::new(
            StatusCode::CONFLICT,
            "conflict",
            "rebuild_in_progress",
            "a rebuild is already running",
        ),
        RebuildRefused::PaceTooLong { .. } => ApiError::invalid(refusal.to_string()),
        RebuildRefused::Unavailable(message) => ApiError::unavailable(message),
    }
}

pub async fn handle(
    State(state): State<AppState>,
    body: Result<Json<RebuildRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    // No body means full speed; anything else must parse.
    let request = match body {
        Ok(Json(request)) => request,
        Err(JsonRejection::MissingJsonContentType(_)) => RebuildRequest::default(),
        Err(error) => return Err(ApiError::invalid(format!("invalid rebuild request: {error}"))),
    };
    let started = state
        .rebuild
        .rebuild(Duration::from_millis(request.pace_ms))
        .await
        .map_err(refused)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "outcome": "started", "rebuild_id": started.rebuild_id, "pace_ms": request.pace_ms })),
    )
        .into_response())
}
