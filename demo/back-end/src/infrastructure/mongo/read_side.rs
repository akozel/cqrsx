//! The read-model collections of `encore_read`: their names, the repository types over them and the
//! scratch copies a verify run compares against. The repositories are built per read model in
//! `bootstrap::projections`.
use crate::{
    application::{
        concert::views::concert_hall::ConcertHallView, fan::views::fan_wallet::FanWalletView,
        projections::maintenance::ScratchFactory,
    },
    domain::{concert::Concert, fan::Fan},
    infrastructure::mongo::view_repository::MongoViewRepository,
};
use mongodb::Database;
use std::sync::Arc;

pub const CONCERT_HALLS: &str = "concert_halls";
pub const FAN_WALLETS: &str = "fan_wallets";

pub type HallRepository = MongoViewRepository<ConcertHallView, Concert>;
pub type WalletRepository = MongoViewRepository<FanWalletView, Fan>;

/// A scratch copy of the `live` collection for one verify run: `<live>_verify_<run id>`.
pub fn scratch<V: 'static, A: 'static>(read_db: &Database, live: &'static str) -> ScratchFactory<MongoViewRepository<V, A>> {
    let read_db = read_db.clone();
    Arc::new(move |run_id| {
        Arc::new(MongoViewRepository::new(
            &read_db,
            &format!("{live}_verify_{}", run_id.simple()),
        ))
    })
}
