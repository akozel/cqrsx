//! HTTP API (axum): router and handlers. The shared state is [`AppState`], built by `bootstrap`.
pub mod activity;
pub mod commands;
pub mod error;
pub mod personas;
pub mod projections;
pub mod queries;
#[cfg(test)]
mod tests;

use crate::{
    bootstrap::AppState,
    domain::{ConcertId, FanId},
    presentation::http::error::ApiError,
};
use axum::{
    Router,
    routing::{get, post},
};
use tower_http::trace::TraceLayer;

/// Path segment → validated id, or `400 invalid_request`.
pub fn parse_concert_id(raw: &str) -> Result<ConcertId, ApiError> {
    ConcertId::parse(raw).map_err(|e| ApiError::invalid(e.to_string()))
}

pub fn parse_fan_id(raw: &str) -> Result<FanId, ApiError> {
    FanId::parse(raw).map_err(|e| ApiError::invalid(e.to_string()))
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/health", get(queries::health))
        .route("/api/config", get(queries::config))
        .route("/api/concerts", get(queries::concerts))
        .route("/api/concerts/{concert_id}", get(queries::concert))
        .route("/api/concerts/{concert_id}/commands", post(commands::execute))
        .route("/api/fans/{fan_id}/wallet", get(queries::wallet))
        .route("/api/fans/{fan_id}/inbox", get(queries::inbox))
        .route("/api/concerts/{concert_id}/timeline", get(projections::timeline))
        .route("/api/concerts/{concert_id}/state-at/{sequence}", get(projections::state_at))
        .route("/api/projections/wipe", post(projections::wipe))
        .route("/api/projections/rebuild", post(projections::rebuild))
        .route("/api/projections/status", get(projections::status))
        .route("/api/projections/verify", get(projections::verify))
        .route("/api/activity", get(activity::stream))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
