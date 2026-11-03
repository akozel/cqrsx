//! What the HTTP API (axum) of every module shares: path ids, the error shape and the dependencies of the modules.
pub mod error;

use crate::shared::{
    domain::{ConcertId, FanId},
    presentation::http::error::ApiError,
};
use cqrsx_framework::Cqrs;
use std::sync::Arc;

/// Path segment → validated id, or `400 invalid_request`.
pub fn parse_concert_id(raw: &str) -> Result<ConcertId, ApiError> {
    ConcertId::parse(raw).map_err(|e| ApiError::invalid(e.to_string()))
}

pub fn parse_fan_id(raw: &str) -> Result<FanId, ApiError> {
    FanId::parse(raw).map_err(|e| ApiError::invalid(e.to_string()))
}

/// The dependency of type `T` a module provided, or `503 unavailable` when the framework has none.
pub fn dependency<T: ?Sized + 'static>(cqrs: &Cqrs) -> Result<Arc<T>, ApiError> {
    cqrs.dependency::<T>()
        .map_err(|missing| ApiError::unavailable(missing.to_string()))
}
