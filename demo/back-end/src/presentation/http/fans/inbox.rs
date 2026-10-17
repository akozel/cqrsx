//! `GET /api/fans/{fan_id}/inbox` — e-mails from the simulated external mail service, oldest first.
use crate::{
    bootstrap::AppState,
    presentation::http::{error::ApiError, parse_fan_id},
};
use axum::{
    Json,
    extract::{Path, State},
};
use serde_json::{Value, json};

pub async fn handle(State(state): State<AppState>, Path(fan_id): Path<String>) -> Result<Json<Value>, ApiError> {
    let fan_id = parse_fan_id(&fan_id)?;
    let mails = state
        .read
        .inbox
        .of_fan(&fan_id)
        .await
        .map_err(ApiError::read_model_unavailable)?;
    Ok(Json(json!(mails)))
}
