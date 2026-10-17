//! Admin endpoints: 🧨 wipe, ⏪ Encore! (rebuild), status; ⏳ timeline and time machine.
use crate::{
    app::rebuild::{AlreadyRunning, Speed, WipeError},
    http::{AppState, error::ApiError, parse_concert_id},
};
use axum::{
    Json,
    extract::{Path, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};

pub async fn wipe(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    match state.rebuild.wipe().await {
        Ok(collections) => Ok(Json(json!({ "outcome": "wiped", "collections": collections }))),
        Err(WipeError::Locked) => Err(ApiError::locked()),
        Err(WipeError::Storage(message)) => Err(ApiError::unavailable(message)),
    }
}

#[derive(Debug, Deserialize)]
pub struct RebuildRequest {
    #[serde(default = "default_speed")]
    speed: Speed,
}

fn default_speed() -> Speed {
    Speed::Slow
}

pub async fn rebuild(
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

pub async fn status(State(state): State<AppState>) -> Json<Value> {
    Json(state.rebuild.status_json())
}

pub async fn timeline(State(state): State<AppState>, Path(concert_id): Path<String>) -> Result<Json<Value>, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    let rows = state.time_travel.timeline(&concert_id).await?;
    Ok(Json(Value::Array(rows)))
}

pub async fn state_at(
    State(state): State<AppState>,
    Path((concert_id, sequence)): Path<(String, usize)>,
) -> Result<Json<Value>, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    match state.time_travel.state_at(&concert_id, sequence).await? {
        Some(state) => Ok(Json(state)),
        None => Err(ApiError::not_found("sequence_out_of_range", "this concert has no such event")),
    }
}

/// `GET /api/projections/verify` [P1]: rebuild into temporary collections and compare. Refused during a rebuild.
pub async fn verify(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    if state.rebuild.is_locked() {
        return Err(ApiError::locked());
    }
    Ok(Json(state.verify.run().await?))
}
