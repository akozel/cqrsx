//! HTTP API (axum): one module per resource and one file per action. The route table is
//! `bootstrap::routes`; the shared state is `bootstrap::AppState`.
pub mod activity;
pub mod concerts;
pub mod config;
pub mod error;
pub mod fans;
pub mod health;
pub mod projections;
#[cfg(test)]
mod tests;

use crate::{
    application::ports::Stored,
    domain::{ConcertId, FanId},
    presentation::http::error::ApiError,
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
