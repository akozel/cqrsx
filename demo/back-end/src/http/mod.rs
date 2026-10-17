//! HTTP API (axum): router, shared state and handlers.
pub mod activity;
pub mod commands;
pub mod error;
pub mod queries;

use crate::{
    app::{
        activity::ActivityHub,
        command_service::{ConcertCommands, FanCommands},
        read_side::ReadSide,
        rebuild::RebuildService,
    },
    config::Config,
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
    /// SSE streams end when this token is cancelled; otherwise graceful shutdown would wait for
    /// every open browser tab.
    pub shutdown: CancellationToken,
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
        .route("/api/activity", get(activity::stream))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
