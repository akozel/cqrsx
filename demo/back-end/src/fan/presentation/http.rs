//! The HTTP API of the ledger: the wallet of a fan, read from the read model.
use axum::{Router, routing::get};
use cqrsx_engine::Cqrs;
use std::sync::Arc;

/// Read a projection: `cqrs.view::<V>()`.
pub mod queries {
    pub mod wallet;
}

/// The URL table of the ledger: a section per kind, one line per endpoint.
pub fn routes() -> Router<Arc<Cqrs>> {
    Router::new()
        // queries: read the projection
        .route("/api/fans/{fan_id}/wallet", get(queries::wallet::handle))
}
