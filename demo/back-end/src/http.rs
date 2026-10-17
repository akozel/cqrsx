//! HTTP API (axum): router, shared state and handlers.
pub mod activity;
pub mod commands;
pub mod error;
pub mod projections;
pub mod queries;
#[cfg(test)]
mod tests;

use crate::{
    app::{
        activity::ActivityHub,
        command_service::{ConcertCommands, FanCommands},
        read_side::ReadSide,
        rebuild::RebuildService,
        time_travel::TimeTravel,
        verify::VerifyService,
    },
    config::Config,
    domain::{ConcertId, FanId},
    http::error::ApiError,
};
use axum::{
    Router,
    routing::{get, post},
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub concert_commands: Arc<ConcertCommands>,
    pub fan_commands: Arc<FanCommands>,
    pub activity: Arc<ActivityHub>,
    pub rebuild: Arc<RebuildService>,
    pub read: Arc<ReadSide>,
    pub time_travel: Arc<TimeTravel>,
    pub verify: Arc<VerifyService>,
    /// SSE streams end when this token is cancelled; otherwise graceful shutdown would wait for
    /// every open browser tab.
    pub shutdown: CancellationToken,
}

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
