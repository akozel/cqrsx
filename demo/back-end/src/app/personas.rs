//! The cast. The front-end gets this list from `GET /api/config`; `crowd-<n>` fans are not listed
//! (their avatar is derived from the id hash in the UI).
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Persona {
    pub fan_id: &'static str,
    pub name: &'static str,
    pub avatar: &'static str,
}

pub const PERSONAS: [Persona; 4] = [
    Persona {
        fan_id: "alice",
        name: "Alice",
        avatar: "🦊",
    },
    Persona {
        fan_id: "bob",
        name: "Bob",
        avatar: "🐼",
    },
    Persona {
        fan_id: "chloe",
        name: "Chloe",
        avatar: "🦄",
    },
    Persona {
        fan_id: "scalper-bot",
        name: "Scalper Bot",
        avatar: "🤖",
    },
];
