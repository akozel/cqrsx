//! What the HTTP API (axum) of every module shares: path ids, the read-model document, the error shape and the
//! dependencies of the modules. The handlers live in the `presentation` of each module, one file per action, routed by
//! the module (`presentation::routes`); their state is the engine (`State<Arc<Cqrs>>`).
pub mod error;

use crate::shared::{
    domain::{ConcertId, FanId},
    presentation::error::ApiError,
};
use cqrsx::{engine::Cqrs, view::repository::StoredView};
use serde::Serialize;
use serde_json::{Value, json};
use std::sync::Arc;

/// Path segment → validated id, or `400 invalid_request`.
pub fn parse_concert_id(raw: &str) -> Result<ConcertId, ApiError> {
    ConcertId::parse(raw).map_err(|e| ApiError::invalid(e.to_string()))
}

pub fn parse_fan_id(raw: &str) -> Result<FanId, ApiError> {
    FanId::parse(raw).map_err(|e| ApiError::invalid(e.to_string()))
}

/// A stored read model as the API returns it: `{ <id_field>, version, view }`.
pub fn stored_document<V: Serialize>(stored: StoredView<V>, id_field: &str) -> Value {
    json!({ id_field: stored.id, "version": stored.version, "view": stored.view })
}

/// The dependency of type `T` a module provided, or `503 unavailable` when the engine has none.
pub fn dependency<T: ?Sized + 'static>(cqrs: &Cqrs) -> Result<Arc<T>, ApiError> {
    cqrs.dependency::<T>()
        .map_err(|missing| ApiError::unavailable(missing.to_string()))
}
