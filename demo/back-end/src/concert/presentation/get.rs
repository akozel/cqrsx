//! `GET /api/concerts/{concert_id}` — the seat-map document `{ concert_id, version, view }`.
use crate::{
    concert::application::views::concert_hall::ConcertHallView,
    shared::presentation::{error::ApiError, parse_concert_id, stored_document},
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
    match cqrs
        .view::<ConcertHallView>()
        .get(&concert_id)
        .await
        .map_err(ApiError::read_model_unavailable)?
    {
        Some(found) => Ok(Json(stored_document(found, "concert_id"))),
        None => Err(ApiError::not_found(
            "projection_missing",
            "no read model for this concert (wiped or never announced)",
        )),
    }
}
