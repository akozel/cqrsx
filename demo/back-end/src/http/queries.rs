//! Read endpoints: health, config and the read-model queries.
use crate::{
    app::personas::PERSONAS,
    config::Config,
    domain::venue::{ROWS, SEATS_PER_ROW, Zone},
    http::{AppState, error::ApiError, parse_concert_id, parse_fan_id},
    projections::concert_hall::ConcertHallView,
};
use axum::{
    Json,
    extract::{Path, State},
};
use mongodb::bson::{Bson, Document};
use serde::Deserialize;
use serde_json::{Value, json};

pub async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

pub fn config_json(config: &Config) -> Value {
    json!({
        "venue": {
            "rows": ROWS.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
            "seats_per_row": SEATS_PER_ROW,
            "zones": Zone::ALL.iter().map(|zone| json!({
                "id": zone.id(),
                "rows": zone.rows().iter().map(|r| r.to_string()).collect::<Vec<_>>(),
                "price": zone.price(),
            })).collect::<Vec<_>>(),
        },
        "personas": PERSONAS,
        "ticket_limit": config.ticket_limit,
        "task_failure_rate": config.task_failure_rate,
        "task_max_attempts": config.task_max_attempts,
        "snapshot_every": config.snapshot_every,
    })
}

pub async fn config(State(state): State<AppState>) -> Json<Value> {
    Json(config_json(&state.config))
}

/// Mongo document → JSON (relaxed extended JSON) with `_id` renamed to the view id field.
fn view_document(document: Document, id_field: &str) -> Value {
    let mut json = Bson::Document(document).into_relaxed_extjson();
    if let Value::Object(map) = &mut json
        && let Some(id) = map.remove("_id")
    {
        map.insert(id_field.to_owned(), id);
    }
    json
}

fn storage(error: impl std::fmt::Display) -> ApiError {
    ApiError::unavailable(format!("read model storage unavailable: {error}"))
}

/// A `concert_halls` document as stored: `{ _id, version, view }`.
#[derive(Deserialize)]
struct HallDocument {
    #[serde(rename = "_id")]
    concert_id: String,
    version: i64,
    view: ConcertHallView,
}

/// `GET /api/concerts` — summary of every concert in `concert_halls` (`[]` when the read models
/// are empty, e.g. after a wipe).
pub async fn concerts(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    let documents = state.read.halls.documents().await.map_err(storage)?;
    let list = documents
        .into_iter()
        .map(|document| {
            let HallDocument {
                concert_id,
                version,
                view,
            } = mongodb::bson::from_document(document).map_err(storage)?;
            let concert = view.concert.as_ref();
            Ok(json!({
                "concert_id": concert_id,
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
            }))
        })
        .collect::<Result<Vec<Value>, ApiError>>()?;
    Ok(Json(Value::Array(list)))
}

/// `GET /api/concerts/{concert_id}` — the seat-map document `{ concert_id, version, view }`.
pub async fn concert(State(state): State<AppState>, Path(concert_id): Path<String>) -> Result<Json<Value>, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    match state.read.halls.document(concert_id.as_str()).await.map_err(storage)? {
        Some(document) => Ok(Json(view_document(document, "concert_id"))),
        None => Err(ApiError::not_found(
            "projection_missing",
            "no read model for this concert (wiped or never announced)",
        )),
    }
}

/// `GET /api/fans/{fan_id}/wallet`.
pub async fn wallet(State(state): State<AppState>, Path(fan_id): Path<String>) -> Result<Json<Value>, ApiError> {
    let fan_id = parse_fan_id(&fan_id)?;
    match state.read.wallets.document(fan_id.as_str()).await.map_err(storage)? {
        Some(document) => Ok(Json(view_document(document, "fan_id"))),
        None => Err(ApiError::not_found("projection_missing", "no wallet for this fan yet")),
    }
}

/// `GET /api/fans/{fan_id}/inbox` — e-mails from the simulated external mail service.
pub async fn inbox(State(state): State<AppState>, Path(fan_id): Path<String>) -> Result<Json<Value>, ApiError> {
    let fan_id = parse_fan_id(&fan_id)?;
    let documents = state.read.inbox_of(&fan_id).await.map_err(storage)?;
    Ok(Json(Value::Array(
        documents.into_iter().map(|d| view_document(d, "inbox_id")).collect(),
    )))
}
