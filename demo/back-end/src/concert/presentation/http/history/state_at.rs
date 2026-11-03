//! `GET /api/concerts/{concert_id}/state-at/{sequence}` — ⏳ the concert after event `sequence`,
//! computed from its events alone.
use crate::{
    concert::{
        application::time_travel::{self, ConcertAt},
        domain::{Booking, ConcertEvent, ConcertInfo},
        presentation::http::history::summary::summary,
    },
    shared::{
        domain::{ConcertId, venue::SeatId},
        presentation::http::{error::ApiError, parse_concert_id},
    },
};
use axum::{
    Json,
    extract::{Path, State},
};
use cqrsx_framework::Cqrsx;
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

/// `{ concert_id, sequence, last_sequence, state, event }`: the state the events up to `sequence` fold to, with the
/// derived figures the UI shows, and the event the state stops at.
#[derive(Debug, Serialize)]
pub struct ConcertStateResponse {
    concert_id: ConcertId,
    sequence: usize,
    last_sequence: usize,
    state: ConcertState,
    event: Event,
}

#[derive(Debug, Serialize)]
struct ConcertState {
    info: Option<ConcertInfo>,
    bookings: BTreeMap<SeatId, Booking>,
    sold: usize,
    free_seats: u32,
    sold_out: bool,
    revenue: u32,
}

#[derive(Debug, Serialize)]
struct Event {
    sequence: usize,
    event_type: &'static str,
    payload: ConcertEvent,
    metadata: HashMap<String, String>,
    summary: String,
}

impl From<ConcertAt> for ConcertStateResponse {
    fn from(at: ConcertAt) -> Self {
        let ConcertAt {
            concert_id,
            concert,
            event,
            last_sequence,
        } = at;
        Self {
            concert_id,
            sequence: event.sequence,
            last_sequence,
            state: ConcertState {
                info: concert.info().cloned(),
                bookings: concert.bookings().clone(),
                sold: concert.bookings().len(),
                free_seats: concert.free_seats(),
                sold_out: concert.is_sold_out(),
                revenue: concert.revenue(),
            },
            event: Event {
                sequence: event.sequence,
                event_type: event.event_type,
                summary: summary(&event.payload),
                payload: event.payload,
                metadata: event.metadata,
            },
        }
    }
}

pub async fn handle(
    State(cqrsx): State<Arc<Cqrsx>>,
    Path((concert_id, sequence)): Path<(String, usize)>,
) -> Result<Json<ConcertStateResponse>, ApiError> {
    let concert_id = parse_concert_id(&concert_id)?;
    match time_travel::state_at(&cqrsx, &concert_id, sequence).await? {
        Some(at) => Ok(Json(at.into())),
        None => Err(ApiError::not_found("sequence_out_of_range", "this concert has no such event")),
    }
}
