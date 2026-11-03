//! `GET /api/concerts/{concert_id}/timeline` — ⏳ one row per event, for the time-machine slider.
use crate::{
    concert::{
        application::time_travel::{self, RecordedEvent},
        presentation::http::history::summary::summary,
    },
    shared::presentation::http::{error::ApiError, parse_concert_id},
};
use axum::{
    Json,
    extract::{Path, State},
};
use cqrsx_framework::Cqrs;
use serde::Serialize;
use std::sync::Arc;

/// `[{ sequence, event_type, recorded_at, summary }]`; `summary` is a string like `"B4 → Fox"`.
#[derive(Debug, Serialize)]
#[serde(transparent)]
pub struct ConcertTimelineResponse(Vec<Row>);

#[derive(Debug, Serialize)]
struct Row {
    sequence: usize,
    event_type: &'static str,
    recorded_at: Option<String>,
    summary: String,
}

impl From<Vec<RecordedEvent>> for ConcertTimelineResponse {
    fn from(events: Vec<RecordedEvent>) -> Self {
        Self(
            events
                .iter()
                .map(|event| Row {
                    sequence: event.sequence,
                    event_type: event.event_type,
                    recorded_at: event.recorded_at().map(str::to_owned),
                    summary: summary(&event.payload),
                })
                .collect(),
        )
    }
}

pub async fn handle(
    State(cqrs): State<Arc<Cqrs>>,
    Path(concert_id): Path<String>,
) -> Result<Json<ConcertTimelineResponse>, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    let events = time_travel::timeline(&cqrs, &concert_id).await?;
    Ok(Json(events.into()))
}
