//! `POST /api/concerts/{concert_id}/rebuild` — rebuild one concert hall from its history (`202`): the owner of
//! the halls' checkpoint replaces the document through the hall's own worker, nothing else is touched.
use crate::{
    concert::application::rebuild::rebuild_hall,
    shared::presentation::{error::ApiError, parse_concert_id},
};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use cqrsx::engine::Cqrs;
use serde_json::json;
use std::sync::Arc;

pub async fn handle(State(cqrs): State<Arc<Cqrs>>, Path(concert_id): Path<String>) -> Result<Response, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    rebuild_hall(&cqrs, &concert_id).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "outcome": "requested", "concert_id": concert_id })),
    )
        .into_response())
}
