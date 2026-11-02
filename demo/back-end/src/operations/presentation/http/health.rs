//! `GET /api/health` — liveness for the container health check; touches no storage.
use axum::Json;
use serde::Serialize;

/// `{ status: "ok" }`.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    status: &'static str,
}

pub async fn handle() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}
