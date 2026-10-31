//! `/api/fans/{fan_id}`: what a fan sees in the box office — the wallet (read model). The fan's
//! e-mails live with the external mail service; the demo's X-ray (sidecar) shows them.
pub mod wallet;

use axum::{Router, routing::get};
use cqrsx_engine::Cqrs;
use std::sync::Arc;

/// The routes of the ledger.
pub fn routes() -> Router<Arc<Cqrs>> {
    Router::new().route("/api/fans/{fan_id}/wallet", get(wallet::handle))
}
