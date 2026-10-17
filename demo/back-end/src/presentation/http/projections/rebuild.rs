//! `POST /api/projections/rebuild` — ⏪ Encore!: start a rebuild in the background (`202`).
use crate::{
    application::operations::rebuild::{AlreadyRunning, Speed},
    bootstrap::AppState,
    presentation::http::error::ApiError,
};
use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Deserialize)]
pub struct RebuildRequest {
    #[serde(default = "default_speed")]
    speed: Speed,
}

fn default_speed() -> Speed {
    Speed::Slow
}

pub async fn handle(
    State(state): State<AppState>,
    body: Result<Json<RebuildRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    // An empty body means "slow"; anything else must parse.
    let speed = match body {
        Ok(Json(request)) => request.speed,
        Err(JsonRejection::MissingJsonContentType(_)) => Speed::Slow,
        Err(error) => return Err(ApiError::invalid(format!("invalid rebuild request: {error}"))),
    };
    match state.rebuild.start(speed) {
        Ok(rebuild_id) => Ok((
            StatusCode::ACCEPTED,
            Json(json!({ "outcome": "started", "rebuild_id": rebuild_id, "speed": speed })),
        )
            .into_response()),
        Err(AlreadyRunning) => Err(ApiError::new(
            StatusCode::CONFLICT,
            "conflict",
            "rebuild_in_progress",
            "a rebuild is already running",
        )),
    }
}
