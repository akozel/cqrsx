//! The read side the HTTP queries use: both read models and the external mail inbox, behind
//! ports so that the presentation never touches a database driver.
use crate::application::{
    concert::views::concert_hall::ConcertHallView,
    fan::views::fan_wallet::FanWalletView,
    ports::{Inbox, ReadModelStore},
};
use std::sync::Arc;

pub struct ReadModels {
    pub halls: Arc<dyn ReadModelStore<ConcertHallView>>,
    pub wallets: Arc<dyn ReadModelStore<FanWalletView>>,
    pub inbox: Arc<dyn Inbox>,
}
