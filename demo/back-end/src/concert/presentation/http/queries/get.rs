//! `GET /api/concerts/{concert_id}` — the seat-map document.
use crate::{
    concert::application::views::concert_hall::ConcertHallView,
    shared::presentation::http::{error::ApiError, parse_concert_id},
};
use axum::{
    Json,
    extract::{Path, State},
};
use cqrsx::view::repository::StoredView;
use cqrsx_framework::Cqrsx;
use serde::Serialize;
use std::sync::Arc;

/// `{ concert_id, revision, view }`: the stored document of the hall.
#[derive(Debug, Serialize)]
pub struct ConcertHallResponse {
    concert_id: String,
    revision: i64,
    view: ConcertHallView,
}

impl From<StoredView<ConcertHallView>> for ConcertHallResponse {
    fn from(stored: StoredView<ConcertHallView>) -> Self {
        Self {
            concert_id: stored.id,
            revision: stored.revision,
            view: stored.view,
        }
    }
}

pub async fn handle(
    State(cqrsx): State<Arc<Cqrsx>>,
    Path(concert_id): Path<String>,
) -> Result<Json<ConcertHallResponse>, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    match cqrsx
        .view::<ConcertHallView>()
        .get(&concert_id)
        .await
        .map_err(ApiError::read_model_unavailable)?
    {
        Some(found) => Ok(Json(found.into())),
        None => Err(ApiError::not_found(
            "projection_missing",
            "no read model for this concert (wiped or never announced)",
        )),
    }
}
