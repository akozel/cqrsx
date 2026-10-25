//! `GET /api/projections/status` — every projection: its checkpoint, lag, owner and state, plus the Encore!
//! this node started and the last one it saw finish.
use crate::{bootstrap::AppState, presentation::http::error::ApiError};
use axum::{Json, extract::State};
use serde_json::Value;

pub async fn handle(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(state.rebuild.status().await?))
}
