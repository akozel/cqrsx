//! `POST /api/concerts/{concert_id}/rebuild` — rebuild one concert hall from its history (`202`): the owner of
//! the halls' checkpoint replaces the document through the hall's own worker, nothing else is touched.
use crate::{
    concert::application::rebuild::rebuild_hall,
    shared::{
        domain::ConcertId,
        presentation::http::{error::ApiError, parse_concert_id},
    },
};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use cqrsx_engine::Cqrs;
use serde::Serialize;
use std::sync::Arc;

/// `202 { outcome: "requested", concert_id }`.
#[derive(Debug, Serialize)]
pub struct ConcertHallRebuildResponse {
    outcome: &'static str,
    concert_id: ConcertId,
}

impl From<ConcertId> for ConcertHallRebuildResponse {
    fn from(concert_id: ConcertId) -> Self {
        Self {
            outcome: "requested",
            concert_id,
        }
    }
}

impl IntoResponse for ConcertHallRebuildResponse {
    fn into_response(self) -> Response {
        (StatusCode::ACCEPTED, Json(self)).into_response()
    }
}

pub async fn handle(
    State(cqrs): State<Arc<Cqrs>>,
    Path(concert_id): Path<String>,
) -> Result<ConcertHallRebuildResponse, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    rebuild_hall(&cqrs, &concert_id).await?;
    Ok(concert_id.into())
}
