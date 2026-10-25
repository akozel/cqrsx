//! The read-model collections of `encore_read`: their names and the repository types over them. The
//! repositories are built per read model in `bootstrap::projections`; the checkpoints of their projections
//! live in the same database (`cqrsx::mongodb::projection_checkpoints::PROJECTION_CHECKPOINTS_COLLECTION`).
use crate::{
    application::{concert::views::concert_hall::ConcertHallView, fan::views::fan_wallet::FanWalletView},
    domain::{concert::Concert, fan::Fan},
    infrastructure::mongo::read_model::MongoReadModel,
};

pub const CONCERT_HALLS: &str = "concert_halls";
pub const FAN_WALLETS: &str = "fan_wallets";

pub type HallRepository = MongoReadModel<ConcertHallView, Concert>;
pub type WalletRepository = MongoReadModel<FanWalletView, Fan>;
