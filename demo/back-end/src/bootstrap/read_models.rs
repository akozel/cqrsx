//! The read side the HTTP queries use: both read models, behind ports so that the presentation
//! never touches a database driver.
use crate::{
    concert::application::views::concert_hall::ConcertHallView, fan::application::views::fan_wallet::FanWalletView,
    shared::application::read_model::ReadModelStore,
};
use std::sync::Arc;

pub struct ReadModels {
    pub halls: Arc<dyn ReadModelStore<ConcertHallView>>,
    pub wallets: Arc<dyn ReadModelStore<FanWalletView>>,
}
