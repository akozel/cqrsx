//! `GET /api/concerts` — summary of every concert in `concert_halls` (`[]` when the read models
//! are empty, e.g. after a wipe).
use crate::{application::ports::Stored, bootstrap::AppState, presentation::http::error::ApiError};
use axum::{Json, extract::State};
use serde_json::{Value, json};

pub async fn handle(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let halls = state.read.halls.list().await.map_err(ApiError::read_model_unavailable)?;
    let list = halls
        .into_iter()
        .map(|Stored { id, version, view }| {
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
                "incomplete": view.incomplete,
                "last_sequence": view.last_sequence,
                "version": version,
            })
        })
        .collect();
    Ok(Json(Value::Array(list)))
}
