//! Test fixtures that need no running database: lazily connecting PostgreSQL pools and MongoDB
//! clients, enough to wire services and exercise every branch that returns before storage.
use crate::infrastructure::mongo::read_side::ReadSide;
use mongodb::{Client, options::ClientOptions};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::time::Duration;

/// Nothing listens on port 1: the first acquire fails fast instead of retrying for the default 30 s.
pub fn lazy_pool() -> PgPool {
    PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(200))
        .connect_lazy_with("postgresql://encore:encore@127.0.0.1:1/encore".parse().unwrap())
}

pub async fn lazy_mongo() -> Client {
    let mut options = ClientOptions::parse("mongodb://127.0.0.1:1").await.unwrap();
    options.server_selection_timeout = Some(Duration::from_millis(50));
    Client::with_options(options).unwrap()
}

pub async fn lazy_read_side() -> ReadSide {
    let mongo = lazy_mongo().await;
    ReadSide::new(&mongo.database("encore_read_test"), &mongo.database("mail_service_test"))
}
