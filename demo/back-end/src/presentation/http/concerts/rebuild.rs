//! `POST /api/concerts/{concert_id}/rebuild` — rebuild one concert hall from its history (`202`): the owner of
//! the halls' checkpoint replaces the document through the hall's own worker, nothing else is touched.
use crate::{
    bootstrap::AppState,
    presentation::http::{error::ApiError, parse_concert_id, projections::rebuild::refused},
};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

pub async fn handle(State(state): State<AppState>, Path(concert_id): Path<String>) -> Result<Response, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    state.rebuild.rebuild_concert(concert_id.as_str()).await.map_err(refused)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "outcome": "requested", "concert_id": concert_id })),
    )
        .into_response())
}
