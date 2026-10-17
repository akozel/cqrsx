//! `GET /api/fans/{fan_id}/wallet` — the wallet document `{ fan_id, version, view }`.
use crate::{
    bootstrap::AppState,
    presentation::http::{error::ApiError, parse_fan_id, stored_document},
};
use axum::{
    Json,
    extract::{Path, State},
};
use serde_json::Value;

pub async fn handle(State(state): State<AppState>, Path(fan_id): Path<String>) -> Result<Json<Value>, ApiError> {
    let fan_id = parse_fan_id(&fan_id)?;
    match state
        .read
        .wallets
        .load(fan_id.as_str())
        .await
        .map_err(ApiError::read_model_unavailable)?
    {
        Some(stored) => Ok(Json(stored_document(stored, "fan_id"))),
        None => Err(ApiError::not_found("projection_missing", "no wallet for this fan yet")),
    }
}
