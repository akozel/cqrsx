//! Shared read-side handles: the two view repositories (MongoDB) and the mail inbox collection.
use crate::{
    application::{concert::views::concert_hall::ConcertHallView, fan::views::fan_wallet::FanWalletView},
    domain::{FanId, concert::Concert, fan::Fan},
    infrastructure::mongo::view_repository::MongoViewRepository,
};
use mongodb::{
    Collection, Database,
    bson::{Document, doc},
};
use std::sync::Arc;

pub const CONCERT_HALLS: &str = "concert_halls";
pub const FAN_WALLETS: &str = "fan_wallets";
/// The simulated external mail service's one collection (in the `mail_db` database, not in the
/// read-model database: wipe and replay never touch it).
pub const MAIL_INBOX: &str = "inbox";

pub struct ReadSide {
    pub halls: Arc<MongoViewRepository<ConcertHallView, Concert>>,
    pub wallets: Arc<MongoViewRepository<FanWalletView, Fan>>,
    pub inbox: Collection<Document>,
}

impl ReadSide {
    pub fn new(read_db: &Database, mail_db: &Database) -> Self {
        Self {
            halls: Arc::new(MongoViewRepository::new(read_db, CONCERT_HALLS)),
            wallets: Arc::new(MongoViewRepository::new(read_db, FAN_WALLETS)),
            inbox: mail_db.collection(MAIL_INBOX),
        }
    }

    /// E-mails of one fan, oldest first.
    pub async fn inbox_of(&self, fan_id: &FanId) -> Result<Vec<Document>, mongodb::error::Error> {
        use futures::TryStreamExt;
        self.inbox
            .find(doc! { "to_fan_id": fan_id.as_str() })
            .sort(doc! { "first_delivered_at": 1 })
            .await?
            .try_collect()
            .await
    }
}
