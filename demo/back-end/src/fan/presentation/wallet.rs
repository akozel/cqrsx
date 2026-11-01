//! `GET /api/fans/{fan_id}/wallet` — the fan's wallet assembled from their ledgers (D-14):
//! `{ fan_id, ledgers: [{ concert_id, revision, last_sequence }], view }`, where `view` has the
//! shape of one `fan_wallets` document (`tickets` keyed by `booking_id`, counters summed).
use crate::{
    fan::application::{views::fan_wallet::FanWalletView, wallet::wallet_of},
    shared::presentation::{error::ApiError, parse_fan_id},
};
use axum::{
    Json,
    extract::{Path, State},
};
use cqrsx_engine::Cqrs;
use serde_json::{Value, json};
use std::sync::Arc;

pub async fn handle(State(cqrs): State<Arc<Cqrs>>, Path(fan_id): Path<String>) -> Result<Json<Value>, ApiError> {
    let fan_id = parse_fan_id(&fan_id)?;
    match wallet_of(&cqrs.view::<FanWalletView>(), &fan_id)
        .await
        .map_err(ApiError::read_model_unavailable)?
    {
        Some(wallet) => Ok(Json(json!(wallet))),
        None => Err(ApiError::not_found("projection_missing", "no wallet for this fan yet")),
    }
}
