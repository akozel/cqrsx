//! `GET /api/concerts` — summary of every concert in `concert_halls` (`[]` when the read models
//! are empty, e.g. after a wipe).
use crate::{
    concert::application::views::concert_hall::{ConcertHallView, ConcertSummary},
    shared::presentation::http::error::ApiError,
};
use axum::{Json, extract::State};
use cqrsx::view::repository::StoredView;
use cqrsx_engine::Cqrs;
use serde::Serialize;
use std::sync::Arc;

/// One entry per hall: `{ concert_id, artist, city, date, sold, capacity, revenue, sold_out, last_sequence, revision }`;
/// the concert's own fields are `null` while the hall has no concert.
#[derive(Debug, Serialize)]
#[serde(transparent)]
pub struct ConcertHallListResponse(Vec<Entry>);

#[derive(Debug, Serialize)]
struct Entry {
    concert_id: String,
    artist: Option<String>,
    city: Option<String>,
    date: Option<String>,
    sold: i64,
    capacity: i64,
    revenue: i64,
    sold_out: bool,
    last_sequence: i64,
    revision: i64,
}

impl From<Vec<StoredView<ConcertHallView>>> for ConcertHallListResponse {
    fn from(halls: Vec<StoredView<ConcertHallView>>) -> Self {
        Self(
            halls
                .into_iter()
                .map(|StoredView { id, revision, view, .. }| {
                    let (artist, city, date) = match view.concert {
                        Some(ConcertSummary { artist, city, date }) => (Some(artist), Some(city), Some(date)),
                        None => (None, None, None),
                    };
                    Entry {
                        concert_id: id,
                        artist,
                        city,
                        date,
                        sold: view.sold,
                        capacity: view.capacity,
                        revenue: view.revenue,
                        sold_out: view.sold_out,
                        last_sequence: view.last_sequence,
                        revision,
                    }
                })
                .collect(),
        )
    }
}

pub async fn handle(State(cqrs): State<Arc<Cqrs>>) -> Result<Json<ConcertHallListResponse>, ApiError> {
    let halls = cqrs
        .view::<ConcertHallView>()
        .list()
        .await
        .map_err(ApiError::read_model_unavailable)?;
    Ok(Json(halls.into()))
}
