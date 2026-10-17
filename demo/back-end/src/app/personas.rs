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
