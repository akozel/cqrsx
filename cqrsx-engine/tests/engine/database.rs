//! The database of one test: a PostgreSQL container with the cqrsx schemas.
use cqrsx_fixtures::postgres::Postgres;
use cqrsx_postgres::{
    EVENT_STORE_SCHEMA, PROJECTION_CHECKPOINTS_SCHEMA, TRANSACTIONAL_OUTBOX_SCHEMA,
    pool::{ConnectionPingPolicy, DEFAULT_IDLE_PING_AFTER, StatementTimeout, connect_pool},
};
use sqlx::PgPool;
use std::time::Duration;

pub(crate) struct Database {
    /// The pool of the container, made like the one a host shares between the command executors and the task queue.
    pub(crate) pool: PgPool,
    container: Postgres,
}

impl Database {
    pub(crate) async fn new() -> Self {
        let container = Postgres::start(
            |options| {
                connect_pool(
                    options,
                    ConnectionPingPolicy::Idle(DEFAULT_IDLE_PING_AFTER),
                    StatementTimeout::Custom(Duration::from_secs(30)),
                )
            },
            &[EVENT_STORE_SCHEMA, TRANSACTIONAL_OUTBOX_SCHEMA, PROJECTION_CHECKPOINTS_SCHEMA],
        )
        .await;
        Self {
            pool: container.pool.clone(),
            container,
        }
    }

    pub(crate) async fn close(self) {
        self.container.close().await;
    }
}
