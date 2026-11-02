//! The HTTP API of the operations. The read side lies in the directory of its kind of engine entry; health, settings
//! and the activity stream use none of the four entries and lie directly in `http/`.
use axum::{
    Router,
    routing::{get, post},
};
use cqrsx_engine::Cqrs;
use std::sync::Arc;

pub mod activity;
pub mod config;
pub mod health;
/// Serve the read side: `cqrs.views()`, `cqrs.view::<V>().rebuild(..)`.
pub mod maintenance {
    pub mod rebuild;
    pub mod status;
    pub mod verify;
}

/// The URL table of the operations: a section per kind, one line per endpoint.
pub fn routes() -> Router<Arc<Cqrs>> {
    Router::new()
        // maintenance: the read side
        .route("/api/projections/rebuild", post(maintenance::rebuild::handle))
        .route("/api/projections/status", get(maintenance::status::handle))
        .route("/api/projections/verify", get(maintenance::verify::handle))
        // no engine entry: the process, the UI's settings, the activity feed
        .route("/api/health", get(health::handle))
        .route("/api/config", get(config::handle))
        .route("/api/activity", get(activity::handle))
}
