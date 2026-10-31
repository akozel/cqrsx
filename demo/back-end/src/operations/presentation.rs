//! The HTTP API of the operations: health, settings, the activity stream and the read side.
pub mod activity;
pub mod config;
pub mod health;
pub mod projections;

use axum::{
    Router,
    routing::{get, post},
};
use cqrsx_engine::Cqrs;
use std::sync::Arc;

/// The service routes, one line per endpoint.
pub fn routes() -> Router<Arc<Cqrs>> {
    Router::new()
        .route("/api/health", get(health::handle))
        .route("/api/config", get(config::handle))
        .route("/api/projections/rebuild", post(projections::rebuild::handle))
        .route("/api/projections/status", get(projections::status::handle))
        .route("/api/projections/verify", get(projections::verify::handle))
        .route("/api/activity", get(activity::stream::handle))
}
