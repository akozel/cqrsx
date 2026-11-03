//! `GET /api/fans/{fan_id}/wallet` — the fan's wallet assembled from their ledgers (D-14).
use crate::{
    fan::application::{
        views::fan_wallet::FanWalletView,
        wallet::{FanWallet, wallet_of},
    },
    shared::presentation::http::{error::ApiError, parse_fan_id},
};
use axum::{
    Json,
    extract::{Path, State},
};
use cqrsx_framework::Cqrsx;
use serde::Serialize;
use std::sync::Arc;

/// `{ fan_id, ledgers: [{ concert_id, revision, last_sequence }], view }`, where `view` has the shape of one
/// `fan_wallets` document (`tickets` keyed by `booking_id`, counters summed).
#[derive(Debug, Serialize)]
pub struct FanWalletResponse {
    fan_id: String,
    ledgers: Vec<Ledger>,
    view: FanWalletView,
}

/// One ledger behind the wallet: the concert, the document revision and the last event applied.
#[derive(Debug, Serialize)]
struct Ledger {
    concert_id: String,
    revision: i64,
    last_sequence: i64,
}

impl From<FanWallet> for FanWalletResponse {
    fn from(wallet: FanWallet) -> Self {
        Self {
            fan_id: wallet.fan_id,
            ledgers: wallet
                .ledgers
                .into_iter()
                .map(|ledger| Ledger {
                    concert_id: ledger.concert_id,
                    revision: ledger.revision,
                    last_sequence: ledger.last_sequence,
                })
                .collect(),
            view: wallet.view,
        }
    }
}

pub async fn handle(State(cqrsx): State<Arc<Cqrsx>>, Path(fan_id): Path<String>) -> Result<Json<FanWalletResponse>, ApiError> {
    let fan_id = parse_fan_id(&fan_id)?;
    match wallet_of(&cqrsx.view::<FanWalletView>(), &fan_id)
        .await
        .map_err(ApiError::read_model_unavailable)?
    {
        Some(wallet) => Ok(Json(wallet.into())),
        None => Err(ApiError::not_found("projection_missing", "no wallet for this fan yet")),
    }
}
