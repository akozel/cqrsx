//! The process around the X-ray: `.env`, logging, the configuration, the inspectors over PostgreSQL
//! and MongoDB, the HTTP server, start and stop. What the sidecar serves is assembled from a
//! [`Process`] by `bootstrap`.
use crate::{
    bootstrap::App,
    config::{Config, ConfigError},
    mongo::MongoInspector,
    postgres::PgInspector,
};
use sqlx::postgres::PgConnectOptions;
use std::{future::IntoFuture, net::SocketAddr, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

/// What the sidecar is built from: the configuration, the read-only inspectors and the stop signal.
pub struct Process {
    pub config: Arc<Config>,
    pub postgres: PgInspector,
    pub mongo: MongoInspector,
    /// Cancelled when the process stops; the SSE streams and the poller end with it.
    pub shutdown: CancellationToken,
}

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("logging setup failed: {0}")]
    Logging(String),
    #[error("invalid configuration: {0}")]
    Config(#[from] ConfigError),
    #[error("SIDECAR_DATABASE_URL: {0}")]
    DatabaseUrl(sqlx::Error),
    #[error("SIDECAR_MONGODB_URI: {0}")]
    MongoUri(mongodb::error::Error),
    #[error("MongoDB client: {0}")]
    MongoClient(mongodb::error::Error),
    #[error("cannot bind {addr}: {source}")]
    Bind { addr: SocketAddr, source: std::io::Error },
}

impl Process {
    /// Loads `demo/.env`, installs logging, reads the configuration and builds the inspectors. Nothing
    /// connects yet: an unavailable database shows up in the X-ray as an error, never as a dead process.
    pub async fn start() -> Result<Self, ProcessError> {
        dotenvy::dotenv().ok();
        init_logging().map_err(ProcessError::Logging)?;
        let config = Arc::new(Config::from_env()?);
        let options = config
            .database_url
            .parse::<PgConnectOptions>()
            .map_err(ProcessError::DatabaseUrl)?
            .application_name("encore-sidecar");
        let postgres = PgInspector::new(options);
        let mut mongo_options = mongodb::options::ClientOptions::parse(&config.mongodb_uri)
            .await
            .map_err(ProcessError::MongoUri)?;
        mongo_options.server_selection_timeout = Some(Duration::from_secs(2));
        mongo_options.connect_timeout = Some(Duration::from_secs(2));
        mongo_options.app_name = Some("encore-sidecar".into());
        let client = mongodb::Client::with_options(mongo_options).map_err(ProcessError::MongoClient)?;
        let mongo = MongoInspector::new(client, config.read_db.clone(), config.mail_db.clone());
        Ok(Self {
            config,
            postgres,
            mongo,
            shutdown: CancellationToken::new(),
        })
    }

    /// Serves `app` until a stop signal or the server's end, then stops the poller and the seeder.
    pub async fn serve(self, app: App) -> Result<(), ProcessError> {
        let listener = tokio::net::TcpListener::bind(self.config.http_addr)
            .await
            .map_err(|source| ProcessError::Bind {
                addr: self.config.http_addr,
                source,
            })?;
        tracing::info!(
            "encore-sidecar listening on http://{} (poll every {:?}; mail service refuses {:.0} % of the deliveries)",
            self.config.http_addr,
            self.config.poll_interval,
            self.config.mail_failure_rate * 100.0
        );
        let server = axum::serve(listener, app.router)
            .with_graceful_shutdown(self.shutdown.clone().cancelled_owned())
            .into_future();
        tokio::pin!(server);
        tokio::select! {
            result = shutdown_signal() => {
                if let Err(error) = result { tracing::error!("signal handling failed: {error}"); }
                tracing::info!("shutdown requested");
            }
            result = &mut server => {
                if let Err(error) = result { tracing::error!("server stopped: {error}"); }
            }
        }
        self.shutdown.cancel();
        let _ = server.await;
        let _ = app.poller.await;
        if let Some(seeder) = app.seeder {
            seeder.abort();
        }
        tracing::info!("encore-sidecar stopped");
        Ok(())
    }
}

fn init_logging() -> Result<(), String> {
    // Same contract as the back-end: RUST_LOG is one level, not a directive list.
    let level: tracing::level_filters::LevelFilter = std::env::var("RUST_LOG")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "info".into())
        .parse()
        .map_err(|e| format!("RUST_LOG: {e}"))?;
    tracing_subscriber::fmt()
        .with_target(false)
        .with_max_level(level)
        .try_init()
        .map_err(|e| e.to_string())
}

async fn shutdown_signal() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}
