//! `/api/concerts`: the box office — its one write endpoint, the read model, the rebuild of one hall and the time
//! machine.
pub mod execute_command;
pub mod get;
pub mod list;
pub mod rebuild;
pub mod state_at;
pub mod timeline;

use axum::{
    Router,
    routing::{get, post},
};
use cqrsx_engine::Cqrs;
use std::sync::Arc;

/// The routes of the box office, one line per endpoint.
pub fn routes() -> Router<Arc<Cqrs>> {
    Router::new()
        .route("/api/concerts", get(list::handle))
        .route("/api/concerts/{concert_id}", get(get::handle))
        .route("/api/concerts/{concert_id}/commands", post(execute_command::handle))
        .route("/api/concerts/{concert_id}/rebuild", post(rebuild::handle))
        .route("/api/concerts/{concert_id}/timeline", get(timeline::handle))
        .route("/api/concerts/{concert_id}/state-at/{sequence}", get(state_at::handle))
}
