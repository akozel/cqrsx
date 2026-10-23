//! The HTTP endpoints: one line per endpoint. The handlers live in `presentation::http`, one file per
//! action; everything they share is the [`AppState`].
use crate::{
    bootstrap::AppState,
    presentation::http::{activity, concerts, config, fans, health, projections},
};
use axum::{
    Router,
    routing::{get, post},
};
use tower_http::trace::TraceLayer;

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
        .route("/api/projections/rebuild", post(projections::rebuild::handle))
        .route("/api/projections/status", get(projections::status::handle))
        .route("/api/projections/verify", get(projections::verify::handle))
        .route("/api/activity", get(activity::stream::handle))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
