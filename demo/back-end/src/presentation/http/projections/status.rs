//! `GET /api/projections/status` — idle or running (with progress), plus the last result.
use crate::bootstrap::AppState;
use axum::{Json, extract::State};
use serde_json::Value;

pub async fn handle(State(state): State<AppState>) -> Json<Value> {
    Json(state.rebuild.status_json())
}
