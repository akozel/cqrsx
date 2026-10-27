//! `GET /api/concerts/{concert_id}` — the seat-map document `{ concert_id, version, view }`.
use crate::{
    bootstrap::AppState,
    shared::presentation::{error::ApiError, parse_concert_id, stored_document},
};
use axum::{
    Json,
    extract::{Path, State},
};
use cqrsx::cqrs::projection::repository::{StoredView, ViewRepository};
use serde_json::Value;

pub async fn handle(State(state): State<AppState>, Path(concert_id): Path<String>) -> Result<Json<Value>, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    match state
        .read
        .halls
        .load_with_context(concert_id.as_str())
        .await
        .map_err(ApiError::read_model_unavailable)?
    {
        Some(found) => Ok(Json(stored_document(StoredView::from(found), "concert_id"))),
        None => Err(ApiError::not_found(
            "projection_missing",
            "no read model for this concert (wiped or never announced)",
        )),
    }
}
