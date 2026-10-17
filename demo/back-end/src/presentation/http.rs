//! HTTP API (axum): the server's routes, one module per resource and one file per action. The
//! shared state is [`AppState`], built by `bootstrap`.
pub mod activity;
pub mod concerts;
pub mod config;
pub mod error;
pub mod fans;
pub mod health;
pub mod projections;
#[cfg(test)]
mod tests;

use crate::{
    application::ports::Stored,
    bootstrap::AppState,
    domain::{ConcertId, FanId},
    presentation::http::error::ApiError,
};
use axum::{
    Router,
    routing::{get, post},
};
use serde::Serialize;
use serde_json::{Value, json};
use tower_http::trace::TraceLayer;

/// Path segment → validated id, or `400 invalid_request`.
pub fn parse_concert_id(raw: &str) -> Result<ConcertId, ApiError> {
    ConcertId::parse(raw).map_err(|e| ApiError::invalid(e.to_string()))
}

pub fn parse_fan_id(raw: &str) -> Result<FanId, ApiError> {
    FanId::parse(raw).map_err(|e| ApiError::invalid(e.to_string()))
}

/// A stored read model as the API returns it: `{ <id_field>, version, view }`.
pub fn stored_document<V: Serialize>(stored: Stored<V>, id_field: &str) -> Value {
    json!({ id_field: stored.id, "version": stored.version, "view": stored.view })
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/health", get(health::handle))
        .route("/api/config", get(config::handle))
        .route("/api/concerts", get(concerts::list::handle))
        .route("/api/concerts/{concert_id}", get(concerts::get::handle))
        .route("/api/concerts/{concert_id}/commands", post(concerts::execute_command::handle))
        .route("/api/concerts/{concert_id}/timeline", get(concerts::timeline::handle))
        .route(
            "/api/concerts/{concert_id}/state-at/{sequence}",
            get(concerts::state_at::handle),
        )
        .route("/api/fans/{fan_id}/wallet", get(fans::wallet::handle))
        .route("/api/fans/{fan_id}/inbox", get(fans::inbox::handle))
        .route("/api/projections/wipe", post(projections::wipe::handle))
        .route("/api/projections/rebuild", post(projections::rebuild::handle))
        .route("/api/projections/status", get(projections::status::handle))
        .route("/api/projections/verify", get(projections::verify::handle))
        .route("/api/activity", get(activity::stream::handle))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
