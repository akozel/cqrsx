//! The read-model collections of `encore_read` and the repositories over them.
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
/// The simulated external mail service's one collection (in the `mail_db` database, not in the
/// read-model database: wipe and replay never touch it).
pub const MAIL_INBOX: &str = "inbox";

pub type HallRepository = MongoViewRepository<ConcertHallView, Concert>;
pub type WalletRepository = MongoViewRepository<FanWalletView, Fan>;

pub struct ReadSide {
    pub halls: Arc<HallRepository>,
    pub wallets: Arc<WalletRepository>,
}

impl ReadSide {
    pub fn new(read_db: &Database) -> Self {
        Self {
            halls: Arc::new(MongoViewRepository::new(read_db, CONCERT_HALLS)),
            wallets: Arc::new(MongoViewRepository::new(read_db, FAN_WALLETS)),
        }
    }
}

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
