//! `GET /api/projections/status` — every projection: its checkpoint, lag, owner and state, plus the Encore!
//! this node started and the last one it saw finish.
use crate::{operations::application::rebuild::RebuildService, shared::presentation::error::ApiError};
use axum::{Json, extract::State};
use cqrsx_engine::Cqrs;
use serde_json::Value;
use std::sync::Arc;

pub async fn handle(State(cqrs): State<Arc<Cqrs>>) -> Result<Json<Value>, ApiError> {
    Ok(Json(RebuildService::of(&cqrs)?.status().await?))
}
