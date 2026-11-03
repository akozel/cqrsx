//! `GET /api/config` — the venue and the rule settings the UI shows. The same document opens every
//! SSE stream (`hello`). Demo props (the cast, the default concerts) are not the box office's: the
//! sidecar serves them (`GET /inspect/demo`).
use crate::{
    operations::application::rebuild::RebuildPace,
    shared::{
        application::TicketLimit,
        domain::venue::{self, ROWS, SEATS_PER_ROW},
        presentation::http::{dependency, error::ApiError},
    },
};
use axum::{Json, extract::State};
use cqrsx_framework::Cqrsx;
use serde::Serialize;
use std::sync::Arc;

/// The venue, the ticket limit of the box office, the snapshot interval of the framework and the slow motion of Encore!.
#[derive(Debug, Serialize)]
pub struct ConfigResponse {
    venue: Venue,
    ticket_limit: usize,
    snapshot_every: usize,
    /// The pause a slow Encore! asks for between the events of one key (D-18).
    rebuild_slow_pace_ms: u64,
}

#[derive(Debug, Serialize)]
struct Venue {
    rows: Vec<String>,
    seats_per_row: u8,
    zones: Vec<Zone>,
}

#[derive(Debug, Serialize)]
struct Zone {
    id: &'static str,
    rows: Vec<String>,
    price: u32,
}

impl TryFrom<&Cqrsx> for ConfigResponse {
    type Error = ApiError;

    /// The settings of the framework and the dependencies of the modules, or `503` when one is missing.
    fn try_from(cqrsx: &Cqrsx) -> Result<Self, ApiError> {
        let (limit, rebuild_pace) = (dependency::<TicketLimit>(cqrsx)?, dependency::<RebuildPace>(cqrsx)?);
        Ok(Self {
            venue: Venue {
                rows: ROWS.iter().map(char::to_string).collect(),
                seats_per_row: SEATS_PER_ROW,
                zones: venue::Zone::ALL
                    .iter()
                    .map(|zone| Zone {
                        id: zone.id(),
                        rows: zone.rows().iter().map(char::to_string).collect(),
                        price: zone.price(),
                    })
                    .collect(),
            },
            ticket_limit: limit.0,
            snapshot_every: cqrsx.settings().snapshot_every().get(),
            rebuild_slow_pace_ms: u64::try_from(rebuild_pace.slow_pace.as_millis()).unwrap_or(u64::MAX),
        })
    }
}

pub async fn handle(State(cqrsx): State<Arc<Cqrsx>>) -> Result<Json<ConfigResponse>, ApiError> {
    Ok(Json(ConfigResponse::try_from(&*cqrsx)?))
}
