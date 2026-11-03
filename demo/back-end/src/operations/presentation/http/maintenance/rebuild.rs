//! `POST /api/projections/rebuild` — ⏪ Encore!: the owners of the checkpoints rebuild every read model from
//! the journal while the box office stays open (`202`). `pace_ms` is the pause between the events of one key
//! (D-18): the UI sends the slow-motion delay or 0.
mod request;
mod response;

use crate::{operations::application::rebuild::ReadSideService, shared::presentation::http::error::ApiError};
use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
};
use cqrsx_framework::Cqrsx;
use request::ProjectionRebuildRequest;
use response::ProjectionRebuildResponse;
use std::sync::Arc;

pub async fn handle(
    State(cqrsx): State<Arc<Cqrsx>>,
    body: Result<Json<ProjectionRebuildRequest>, JsonRejection>,
) -> Result<ProjectionRebuildResponse, ApiError> {
    let request = ProjectionRebuildRequest::from_body(body)?;
    let started = ReadSideService::of(&cqrsx)?.rebuild(request.pace()).await?;
    Ok(started.into())
}
