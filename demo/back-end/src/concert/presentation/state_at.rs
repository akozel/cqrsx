//! `GET /api/concerts/{concert_id}/state-at/{sequence}` — ⏳ the concert after event `sequence`,
//! computed from its events alone.
use crate::{
    concert::application::time_travel,
    shared::presentation::{error::ApiError, parse_concert_id},
};
use axum::{
    Json,
    extract::{Path, State},
};
use cqrsx::engine::Cqrs;
use serde_json::Value;
use std::sync::Arc;

pub async fn handle(
    State(cqrs): State<Arc<Cqrs>>,
    Path((concert_id, sequence)): Path<(String, usize)>,
) -> Result<Json<Value>, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    match time_travel::state_at(&cqrs, &concert_id, sequence).await? {
        Some(state) => Ok(Json(state)),
        None => Err(ApiError::not_found("sequence_out_of_range", "this concert has no such event")),
    }
}
