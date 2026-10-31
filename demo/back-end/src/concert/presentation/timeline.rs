//! `GET /api/concerts/{concert_id}/timeline` — ⏳ one row per event, for the time-machine slider.
use crate::{
    concert::application::time_travel,
    shared::presentation::{error::ApiError, parse_concert_id},
};
use axum::{
    Json,
    extract::{Path, State},
};
use cqrsx_engine::Cqrs;
use serde_json::Value;
use std::sync::Arc;

pub async fn handle(State(cqrs): State<Arc<Cqrs>>, Path(concert_id): Path<String>) -> Result<Json<Value>, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    let rows = time_travel::timeline(&cqrs, &concert_id).await?;
    Ok(Json(Value::Array(rows)))
}
