//! Read endpoints. Phase 3: health and config; Phase 5 adds the read-model queries.
use crate::{
    app::{personas::PERSONAS, seed::SEED_CONCERTS},
    config::Config,
    domain::venue::{ROWS, SEATS_PER_ROW, Zone},
    http::AppState,
};
use axum::{Json, extract::State};
use serde_json::{Value, json};

pub async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

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
        "personas": PERSONAS,
        "seed_concerts": SEED_CONCERTS.iter().map(|c| c.concert_id).collect::<Vec<_>>(),
        "ticket_limit": config.ticket_limit,
        "task_failure_rate": config.task_failure_rate,
        "task_max_attempts": config.task_max_attempts,
        "snapshot_every": config.snapshot_every,
    })
}

pub async fn config(State(state): State<AppState>) -> Json<Value> {
    Json(config_json(&state.config))
}
