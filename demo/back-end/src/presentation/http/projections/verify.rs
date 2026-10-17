//! `GET /api/projections/verify` \[P1\] — rebuild into scratch copies and compare with the live read
//! models. Refused during a rebuild.
use crate::{bootstrap::AppState, presentation::http::error::ApiError};
use axum::{Json, extract::State};
use serde_json::Value;

pub async fn handle(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    if state.rebuild.is_locked() {
        return Err(ApiError::locked());
    }
    Ok(Json(state.verify.run().await?))
}
