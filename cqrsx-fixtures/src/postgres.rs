//! A PostgreSQL 18.6 container with the schemas the caller gives, one per test, on a leased host port.
use crate::port_lease::PortLease;
use sqlx::{PgPool, postgres::PgConnectOptions};
use std::{future::Future, time::Duration};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

/// A PostgreSQL container of one test with the schemas applied.
pub struct Postgres {
    /// The pool the caller's `connect` made on the container.
    pub pool: PgPool,
    container: ContainerAsync<GenericImage>,
    _port_lease: PortLease,
}

impl std::fmt::Debug for Postgres {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Postgres").finish_non_exhaustive()
    }
}

impl Postgres {
    /// Starts the container, waits until `connect` makes a pool on it and applies `schemas` in order.
    ///
    /// `connect` takes the connection options of the container (TLS off, `lock_timeout` 15 s) and makes the pool the
    /// way the code under test does, so that the fixture does not depend on the crate that owns that code. It is
    /// called again until it succeeds or 60 s pass: the server needs a moment to start. `schemas` are the SQL texts
    /// that create the tables.
    pub async fn start<C, F>(connect: C, schemas: &[&str]) -> Self
    where
        C: Fn(PgConnectOptions) -> F,
        F: Future<Output = Result<PgPool, sqlx::Error>>,
    {
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
            .parse::<PgConnectOptions>()
            .unwrap()
            // This local fixture has no TLS. Keep early connection closes on the
            // retryable I/O path instead of SQLx's SSLRequest protocol error path.
            .ssl_mode(sqlx::postgres::PgSslMode::Disable)
            .options([("lock_timeout", "15000")]);
        // Poll the actual protocol endpoint: log delivery may lag under Podman.
        // This is startup readiness, not a retry of a failed test or command.
        let pool = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                match tokio::time::timeout(Duration::from_secs(2), connect(options.clone())).await {
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
        for schema in schemas {
            sqlx::raw_sql(schema).execute(&pool).await.unwrap();
        }
        Self {
            pool,
            container,
            _port_lease: port_lease,
        }
    }

    /// Closes the pool and stops the container.
    pub async fn close(self) {
        self.pool.close().await;
        self.container.stop().await.unwrap();
    }
}
