//! `GET /api/concerts` — summary of every concert in `concert_halls` (`[]` when the read models
//! are empty, e.g. after a wipe).
use crate::{bootstrap::AppState, shared::presentation::error::ApiError};
use axum::{Json, extract::State};
use cqrsx::cqrs::projection::repository::{StoredView, ViewRepository};
use serde_json::{Value, json};

pub async fn handle(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let halls = state.read.halls.list().await.map_err(ApiError::read_model_unavailable)?;
    let list = halls
        .into_iter()
        .map(|StoredView { id, version, view, .. }| {
            let concert = view.concert.as_ref();
            json!({
                "concert_id": id,
                "artist": concert.map(|c| &c.artist),
                "city": concert.map(|c| &c.city),
                "date": concert.map(|c| &c.date),
                "sold": view.sold,
                "capacity": view.capacity,
                "revenue": view.revenue,
                "sold_out": view.sold_out,
                "last_sequence": view.last_sequence,
                "version": version,
            })
        })
        .collect();
    Ok(Json(Value::Array(list)))
}
