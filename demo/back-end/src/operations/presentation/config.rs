//! `GET /api/config` — the venue and the rule settings the UI shows. The same document opens every
//! SSE stream (`hello`). Demo props (the cast, the default concerts) are not the box office's: the
//! sidecar serves them (`GET /inspect/demo`).
use crate::{
    operations::application::rebuild::Replay,
    shared::{
        application::TicketLimit,
        domain::venue::{ROWS, SEATS_PER_ROW, Zone},
        presentation::{dependency, error::ApiError},
    },
};
use axum::{Json, extract::State};
use cqrsx::engine::Cqrs;
use serde_json::{Value, json};
use std::sync::Arc;

/// The venue, the ticket limit of the box office, the snapshot interval of the engine and the slow motion of Encore!.
pub fn config_json(cqrs: &Cqrs) -> Result<Value, ApiError> {
    let (limit, replay) = (dependency::<TicketLimit>(cqrs)?, dependency::<Replay>(cqrs)?);
    Ok(json!({
        "venue": {
            "rows": ROWS.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
            "seats_per_row": SEATS_PER_ROW,
            "zones": Zone::ALL.iter().map(|zone| json!({
                "id": zone.id(),
                "rows": zone.rows().iter().map(|r| r.to_string()).collect::<Vec<_>>(),
                "price": zone.price(),
            })).collect::<Vec<_>>(),
        },
        "ticket_limit": limit.0,
        "snapshot_every": cqrs.settings().snapshot_every().get(),
        // The pause a slow Encore! asks for between the events of one key (D-18).
        "replay_slow_delay_ms": u64::try_from(replay.slow_delay.as_millis()).unwrap_or(u64::MAX),
    }))
}

pub async fn handle(State(cqrs): State<Arc<Cqrs>>) -> Result<Json<Value>, ApiError> {
    Ok(Json(config_json(&cqrs)?))
}
