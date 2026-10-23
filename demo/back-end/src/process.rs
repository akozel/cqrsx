//! The process around the box office: `.env`, logging, the configuration, the connections, the HTTP
//! server and the outbox task runtime, start and stop. Nothing here knows a concert from a fan: the
//! application is assembled from a [`Process`] by `bootstrap`.
use crate::{
    bootstrap::{Wiring, routes},
    config::{Config, ConfigError},
};
use cqrsx::{
    cqrs::event_handler::delivery::DeliveryBacklog,
    host::{lifecycle, signal::shutdown_signal},
    logging::LoggingConfig,
    postgres::{
        pool::{ConnectionPingPolicy, DEFAULT_IDLE_PING_AFTER, StatementTimeout, connect_pool_with},
        task_runtime::prepare_task_runtime,
    },
};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{future::IntoFuture, net::SocketAddr, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

/// How long a stopping process waits for the ordered projections to apply what is left in their buffers.
pub const PROJECTION_DRAIN_LIMIT: Duration = Duration::from_secs(5);

/// What the application is built from: the configuration, the connections and the stop signal.
pub struct Process {
    pub config: Arc<Config>,
    /// Commands and the outbox queue: short static statements, each under the pool's statement timeout.
    pub pool: PgPool,
    /// Whole-history reads (Encore!, verify, the ledger totals and the time machine): no statement
    /// timeout, and no connection kept while none of them runs.
    pub history_pool: PgPool,
    pub mongo: mongodb::Client,
    /// Cancelled when the process stops; the SSE streams and the task runtime end with it.
    pub shutdown: CancellationToken,
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("logging setup failed: {0}")]
    Logging(String),
    #[error("invalid configuration: {0}")]
    Config(#[from] ConfigError),
    #[error("ENCORE_DATABASE_URL: {0}")]
    DatabaseUrl(sqlx::Error),
    #[error("cannot connect to PostgreSQL: {0}")]
    Pool(sqlx::Error),
    #[error("cannot connect the history pool to PostgreSQL: {0}")]
    HistoryPool(sqlx::Error),
    #[error("ENCORE_MONGODB_URI: {0}")]
    MongoUri(mongodb::error::Error),
    #[error("MongoDB client: {0}")]
    MongoClient(mongodb::error::Error),
    #[error("cannot bind {addr}: {source}")]
    Bind { addr: SocketAddr, source: std::io::Error },
    #[error("task runtime setup: {0}")]
    TaskRuntime(String),
}

impl Process {
    /// Loads `demo/.env`, installs logging, reads the configuration and opens the connections. The pools
    /// and the client connect lazily: the readiness of the databases is the job of `scripts/wait-db.sh`.
    pub async fn start() -> Result<Self, ProcessError> {
        dotenvy::dotenv().ok();
        LoggingConfig::from_env()
            .map_err(|e| ProcessError::Logging(e.to_string()))?
            .try_init()
            .map_err(|e| ProcessError::Logging(e.to_string()))?;
        let config = Arc::new(Config::from_env()?);
        let (pool, history_pool) = postgres_pools(&config).await?;
        let mongo = mongo_client(&config).await?;
        Ok(Self {
            config,
            pool,
            history_pool,
            mongo,
            shutdown: CancellationToken::new(),
        })
    }

    /// Serves `app` until a stop signal: the HTTP API and the outbox task runtime side by side. Then the
    /// ordered projections get [`PROJECTION_DRAIN_LIMIT`] to apply what is left in their buffers (the
    /// events of commands whose clients went away; MongoDB is still open), and the pools close.
    pub async fn serve(self, app: Wiring) -> Result<(), ProcessError> {
        let listener = tokio::net::TcpListener::bind(self.config.http_addr)
            .await
            .map_err(|source| ProcessError::Bind {
                addr: self.config.http_addr,
                source,
            })?;
        tracing::info!("encore-backend listening on http://{}", self.config.http_addr);
        // The outbox task runtime on the main pool: claim, settlement and recovery are one statement
        // each. Prepared here (no background work yet), started by `lifecycle::run`.
        let runtime = prepare_task_runtime(self.pool.clone(), app.dispatcher, self.config.task_runtime.clone())
            .map_err(|e| ProcessError::TaskRuntime(e.to_string()))?;
        let task_runtime = runtime.run(self.shutdown.clone());
        // axum 0.8: `with_graceful_shutdown` returns an `IntoFuture`, not a `Future`.
        let inbound = axum::serve(listener, routes::router(app.state))
            .with_graceful_shutdown(self.shutdown.clone().cancelled_owned())
            .into_future();
        if let Err(error) = lifecycle::run(inbound, task_runtime, self.shutdown.clone(), shutdown_signal()).await {
            error.log("encore-backend stopped with an error");
        }
        match drain_projections(&app.projection_backlogs, PROJECTION_DRAIN_LIMIT).await {
            Ok(()) => tracing::info!("projections drained"),
            Err(pending) => tracing::warn!(
                pending,
                "projections not drained in time: the read models miss these commands until the next Encore!"
            ),
        }
        self.pool.close().await;
        self.history_pool.close().await;
        tracing::info!("encore-backend stopped");
        Ok(())
    }
}

/// The main pool and the history pool over the same address, both connecting lazily.
async fn postgres_pools(config: &Config) -> Result<(PgPool, PgPool), ProcessError> {
    let options = config
        .database_url
        .parse::<PgConnectOptions>()
        .map_err(ProcessError::DatabaseUrl)?
        .application_name("encore-backend");
    let pool = connect_pool_with(
        options.clone(),
        PgPoolOptions::new().max_connections(config.database_pool_size.get()),
        ConnectionPingPolicy::Idle(DEFAULT_IDLE_PING_AFTER),
        config.database_statement_timeout,
    )
    .await
    .map_err(ProcessError::Pool)?;
    let history_pool = connect_pool_with(
        options,
        PgPoolOptions::new()
            .max_connections(config.database_history_pool_size.get())
            .min_connections(0)
            .idle_timeout(Duration::from_secs(30)),
        ConnectionPingPolicy::Idle(DEFAULT_IDLE_PING_AFTER),
        StatementTimeout::Unlimited,
    )
    .await
    .map_err(ProcessError::HistoryPool)?;
    Ok((pool, history_pool))
}

/// Short server-selection and connect timeouts: when MongoDB is down a command still answers quickly
/// (200 + `projection_failed` warning) instead of hanging for the driver's default 30 s.
async fn mongo_client(config: &Config) -> Result<mongodb::Client, ProcessError> {
    let mut options = mongodb::options::ClientOptions::parse(&config.mongodb_uri)
        .await
        .map_err(ProcessError::MongoUri)?;
    options.server_selection_timeout = Some(Duration::from_secs(3));
    options.connect_timeout = Some(Duration::from_secs(3));
    options.app_name = Some("encore-backend".into());
    mongodb::Client::with_options(options).map_err(ProcessError::MongoClient)
}

/// Waits until the ordered projections have applied everything handed over to them, at most `limit`.
/// Called once the HTTP server and the task runtime have stopped: what is left then belongs to commands
/// whose clients went away. `Err` carries how many hand-overs were still pending; like a crash, the read
/// models then miss them until the next Encore!.
pub async fn drain_projections(backlogs: &[DeliveryBacklog], limit: Duration) -> Result<(), usize> {
    let drained = async {
        for backlog in backlogs {
            backlog.drained().await;
        }
    };
    tokio::time::timeout(limit, drained)
        .await
        .map_err(|_| backlogs.iter().map(DeliveryBacklog::len).sum())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::fan::{Fan, test_support::record};
    use cqrsx::cqrs::{
        command::CommandExecutor,
        event::EventEnvelope,
        event_handler::{EventHandler, delivery::DeliveryConfig},
        store::PersistenceError,
        testing::{command::TestCommandExecutor, mem_store::MemStore},
    };
    use uuid::Uuid;

    /// A projection that holds every call until the test lets it through.
    struct Held(tokio::sync::Semaphore);

    #[async_trait::async_trait]
    impl EventHandler<Fan> for Held {
        async fn handle(&self, _: &str, _: &[EventEnvelope<Fan>]) -> Result<(), PersistenceError> {
            self.0.acquire().await.unwrap().forget();
            Ok(())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn draining_waits_for_the_projections_and_reports_what_is_left_at_the_limit() {
        assert_eq!(drain_projections(&[], Duration::from_secs(5)).await, Ok(()));
        let held = Arc::new(Held(tokio::sync::Semaphore::new(0)));
        let executor =
            TestCommandExecutor::new(MemStore::<Fan>::default(), ()).with_event_handler(held.clone(), DeliveryConfig::default());
        // Two commands whose clients went away before the projection applied their events.
        for (ledger, booking) in [("fox", 1), ("wolf", 2)] {
            let executor = executor.clone();
            drop(tokio::spawn(async move {
                executor.execute(ledger, record(Uuid::from_u128(booking)), vec![]).await
            }));
        }
        let backlog = executor.backlog();
        for _ in 0..100 {
            if backlog.len() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(backlog.len(), 2, "both commands handed their events over");
        let backlogs = [DeliveryBacklog::default(), backlog];
        assert_eq!(drain_projections(&backlogs, Duration::from_secs(5)).await, Err(2));
        held.0.add_permits(2);
        assert_eq!(drain_projections(&backlogs, Duration::from_secs(5)).await, Ok(()));
    }
}
