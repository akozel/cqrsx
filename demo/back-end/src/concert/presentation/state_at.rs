//! `GET /api/concerts/{concert_id}/state-at/{sequence}` — ⏳ the concert after event `sequence`,
//! computed from its events alone.
use crate::{
    bootstrap::AppState,
    shared::presentation::{error::ApiError, parse_concert_id},
};
use axum::{
    Json,
    extract::{Path, State},
};
use serde_json::Value;

pub async fn handle(
    State(state): State<AppState>,
    Path((concert_id, sequence)): Path<(String, usize)>,
) -> Result<Json<Value>, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    match state.time_travel.state_at(&concert_id, sequence).await? {
        Some(state) => Ok(Json(state)),
        None => Err(ApiError::not_found("sequence_out_of_range", "this concert has no such event")),
    }
}
