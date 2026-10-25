//! `GET /api/projections/verify` \[P1\] — every document against the history of its aggregate (D-20):
//! documents behind the journal are counted apart, only a wrong one is a difference. Allowed at any time,
//! during Encore! too.
use crate::{bootstrap::AppState, presentation::http::error::ApiError};
use axum::{Json, extract::State};
use serde_json::Value;

pub async fn handle(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    Ok(Json(state.verify.run().await?))
}
