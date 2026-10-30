//! `/api/probe`: the load stand's one endpoint.
pub mod ping;

use axum::{Router, routing::post};
use cqrsx::engine::Cqrs;
use std::sync::Arc;

/// The route of the probe.
pub fn routes() -> Router<Arc<Cqrs>> {
    Router::new().route("/api/probe/{probe_id}", post(ping::handle))
}
