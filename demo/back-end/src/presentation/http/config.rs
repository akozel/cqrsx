//! `GET /api/config` — the venue, the cast and the rule settings the UI shows. The same document
//! opens every SSE stream (`hello`).
use crate::{
    bootstrap::AppState,
    config::Config,
    domain::shared::venue::{ROWS, SEATS_PER_ROW, Zone},
};
use axum::{Json, extract::State};
use serde::Serialize;
use serde_json::{Value, json};

/// The cast. `crowd-<n>` fans are not listed (their avatar is derived from the id hash in the UI).

#[derive(Debug, Clone, Serialize)]
pub struct Persona {
    pub fan_id: &'static str,
    pub name: &'static str,
    pub avatar: &'static str,
}

pub const PERSONAS: [Persona; 4] = [
    Persona {
        fan_id: "fox",
        name: "Fox",
        avatar: "🦊",
    },
    Persona {
        fan_id: "panda",
        name: "Panda",
        avatar: "🐼",
    },
    Persona {
        fan_id: "unicorn",
        name: "Unicorn",
        avatar: "🦄",
    },
    Persona {
        fan_id: "arbitrage-bot",
        name: "Arbitrage Bot",
        avatar: "🤖",
    },
];

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
        "ticket_limit": config.ticket_limit,
        "task_failure_rate": config.task_failure_rate,
        "task_max_attempts": config.task_max_attempts,
        "snapshot_every": config.snapshot_every,
    })
}

pub async fn handle(State(state): State<AppState>) -> Json<Value> {
    Json(config_json(&state.config))
}
