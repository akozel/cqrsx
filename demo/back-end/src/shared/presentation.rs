//! What the HTTP API (axum) of every module shares: path ids, the read-model document and the error shape. The
//! handlers live in the `presentation` of each module, one file per action; the route table is
//! `bootstrap::routes`; the shared state is `bootstrap::AppState`.
pub mod error;

use crate::shared::{
    application::read_model::Stored,
    domain::{ConcertId, FanId},
    presentation::error::ApiError,
};
use serde::Serialize;
use serde_json::{Value, json};

/// Path segment → validated id, or `400 invalid_request`.
pub fn parse_concert_id(raw: &str) -> Result<ConcertId, ApiError> {
    ConcertId::parse(raw).map_err(|e| ApiError::invalid(e.to_string()))
}

pub fn parse_fan_id(raw: &str) -> Result<FanId, ApiError> {
    FanId::parse(raw).map_err(|e| ApiError::invalid(e.to_string()))
}

/// A stored read model as the API returns it: `{ <id_field>, version, view }`.
pub fn stored_document<V: Serialize>(stored: Stored<V>, id_field: &str) -> Value {
    json!({ id_field: stored.id, "version": stored.version, "view": stored.view })
}
