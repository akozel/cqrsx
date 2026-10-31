//! `GET /api/projections/verify` \[P1\] — every document against the history of its aggregate (D-20):
//! documents behind the journal are counted apart, only a wrong one is a difference. Allowed at any time,
//! during Encore! too.
use crate::{operations::application::verify::VerifyService, shared::presentation::error::ApiError};
use axum::{Json, extract::State};
use cqrsx_engine::Cqrs;
use serde_json::Value;
use std::sync::Arc;

pub async fn handle(State(cqrs): State<Arc<Cqrs>>) -> Result<Json<Value>, ApiError> {
    Ok(Json(VerifyService::of(&cqrs).run().await?))
}
