//! `GET /api/health` — liveness for the container health check; touches no storage.
use axum::Json;
use serde_json::{Value, json};

pub async fn handle() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}
