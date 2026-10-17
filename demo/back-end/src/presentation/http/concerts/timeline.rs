//! `GET /api/concerts/{concert_id}/timeline` — ⏳ one row per event, for the time-machine slider.
use crate::{
    bootstrap::AppState,
    presentation::http::{error::ApiError, parse_concert_id},
};
use axum::{
    Json,
    extract::{Path, State},
};
use serde_json::Value;

pub async fn handle(State(state): State<AppState>, Path(concert_id): Path<String>) -> Result<Json<Value>, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    let rows = state.time_travel.timeline(&concert_id).await?;
    Ok(Json(Value::Array(rows)))
}
