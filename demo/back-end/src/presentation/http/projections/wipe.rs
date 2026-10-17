//! `POST /api/projections/wipe` — 🧨 drop every read model; refused during a rebuild.
use crate::{application::operations::rebuild::WipeError, bootstrap::AppState, presentation::http::error::ApiError};
use axum::{Json, extract::State};
use serde_json::{Value, json};

pub async fn handle(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    match state.rebuild.wipe().await {
        Ok(collections) => Ok(Json(json!({ "outcome": "wiped", "collections": collections }))),
        Err(WipeError::Locked) => Err(ApiError::locked()),
        Err(WipeError::Storage(message)) => Err(ApiError::unavailable(message)),
    }
}
