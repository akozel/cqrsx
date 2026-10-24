//! A PostgreSQL 18.6 container with the three cqrsx schemas, one per test, on a leased host port.
use crate::port_lease::PortLease;
use cqrsx::postgres::{
    EVENT_STORE_SCHEMA, PROJECTION_CHECKPOINTS_SCHEMA, TRANSACTIONAL_OUTBOX_SCHEMA,
    pool::{ConnectionPingPolicy, DEFAULT_IDLE_PING_AFTER, StatementTimeout, connect_pool},
};
use sqlx::PgPool;
use std::time::Duration;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

pub(crate) struct Postgres {
    pub(crate) pool: PgPool,
    container: ContainerAsync<GenericImage>,
    _port_lease: PortLease,
}

impl Postgres {
    pub(crate) async fn new() -> Self {
        let port_lease = PortLease::acquire();
        let container = tokio::time::timeout(
            Duration::from_secs(60),
            GenericImage::new("docker.io/library/postgres", "18.6-alpine")
                .with_exposed_port(5432.tcp())
                .with_wait_for(WaitFor::Nothing)
                .with_mapped_port(port_lease.port(), 5432.tcp())
                .with_env_var("POSTGRES_DB", "outbox_test")
                .with_env_var("POSTGRES_USER", "outbox_test")
                .with_env_var("POSTGRES_PASSWORD", "outbox_test")
                .start(),
        )
        .await
        .expect("bounded PostgreSQL container startup")
        .unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432.tcp()).await.unwrap();
        let options = format!("postgresql://outbox_test:outbox_test@{host}:{port}/outbox_test")
            .parse::<sqlx::postgres::PgConnectOptions>()
            .unwrap()
            // This local fixture has no TLS. Keep early connection closes on the
            // retryable I/O path instead of SQLx's SSLRequest protocol error path.
            .ssl_mode(sqlx::postgres::PgSslMode::Disable)
            .options([("lock_timeout", "15000")]);
        // Poll the actual protocol endpoint: log delivery may lag under Podman.
        // This is startup readiness, not a retry of a failed test or command.
        let pool = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                match tokio::time::timeout(
                    Duration::from_secs(2),
                    connect_pool(
                        options.clone(),
                        ConnectionPingPolicy::Idle(DEFAULT_IDLE_PING_AFTER),
                        StatementTimeout::Custom(Duration::from_secs(30)),
                    ),
                )
                .await
                {
                    Ok(Ok(pool)) => break pool,
                    Ok(Err(sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut)) | Err(_) => {}
                    Ok(Err(sqlx::Error::Database(error))) if matches!(error.code().as_deref(), Some("57P03" | "3D000")) => {}
                    Ok(Err(error)) => panic!("PostgreSQL fixture startup connection failed: {error}"),
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("bounded PostgreSQL SQL readiness");
        sqlx::raw_sql(EVENT_STORE_SCHEMA).execute(&pool).await.unwrap();
        sqlx::raw_sql(TRANSACTIONAL_OUTBOX_SCHEMA).execute(&pool).await.unwrap();
        sqlx::raw_sql(PROJECTION_CHECKPOINTS_SCHEMA).execute(&pool).await.unwrap();
        Self {
            pool,
            container,
            _port_lease: port_lease,
        }
    }

    pub(crate) async fn close(self) {
        self.pool.close().await;
        self.container.stop().await.unwrap();
    }
}
