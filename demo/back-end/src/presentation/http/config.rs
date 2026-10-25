//! `GET /api/config` — the venue and the rule settings the UI shows. The same document opens every
//! SSE stream (`hello`). Demo props (the cast, the default concerts) are not the box office's: the
//! sidecar serves them (`GET /inspect/demo`).
use crate::{
    bootstrap::AppState,
    config::Config,
    domain::shared::venue::{ROWS, SEATS_PER_ROW, Zone},
};
use axum::{Json, extract::State};
use serde_json::{Value, json};

pub fn config_json(config: &Config) -> Value {
    json!({
        "venue": {
            "rows": ROWS.iter().map(|r| r.to_string()).collect::<Vec<_>>(),
            "seats_per_row": SEATS_PER_ROW,
            "zones": Zone::ALL.iter().map(|zone| json!({
                "id": zone.id(),
                "rows": zone.rows().iter().map(|r| r.to_string()).collect::<Vec<_>>(),
                "price": zone.price(),
            })).collect::<Vec<_>>(),
        },
        "ticket_limit": config.ticket_limit,
        "snapshot_every": config.snapshot_every,
        // The pause a slow Encore! asks for between the events of one key (D-18).
        "replay_slow_delay_ms": u64::try_from(config.replay_slow_delay.as_millis()).unwrap_or(u64::MAX),
    })
}

pub async fn handle(State(state): State<AppState>) -> Json<Value> {
    Json(config_json(&state.config))
}
