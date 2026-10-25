//! The process around the box office: `.env`, logging, the configuration, the connections, the HTTP
//! server, the outbox task runtime and the projection processors, start and stop. Nothing here knows a
//! concert from a fan: the application is assembled from a [`Process`] by `bootstrap`.
use crate::{
    bootstrap::{Wiring, routes},
    config::{Config, ConfigError},
};
use cqrsx::{
    cqrs::projection::runtime::ProjectionRuntime,
    host::{lifecycle, signal::shutdown_signal},
    logging::LoggingConfig,
    postgres::{
        pool::{ConnectionPingPolicy, DEFAULT_IDLE_PING_AFTER, StatementTimeout, connect_pool_with},
        task_runtime::prepare_task_runtime,
    },
    task::runtime::TaskRuntimeError,
};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{
    future::{Future, IntoFuture},
    io,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio_util::sync::CancellationToken;

/// What the application is built from: the configuration, the connections and the stop signal.
pub struct Process {
    pub config: Arc<Config>,
    /// Commands and the outbox queue: short static statements, each under the pool's statement timeout.
    pub pool: PgPool,
    /// Whole-history reads (verify, the ledger totals and the time machine): no statement timeout, and no
    /// connection kept while none of them runs.
    pub history_pool: PgPool,
    /// The projection processors' reads of the journal: pages and, on a rebuild, the history of every key
    /// of a page, under the main pool's statement timeout but apart from the commands' connections.
    pub projection_pool: PgPool,
    pub mongo: mongodb::Client,
    /// Cancelled when the process stops; the SSE streams, the task runtime and the processors end with it.
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
    #[error("cannot connect the projection pool to PostgreSQL: {0}")]
    ProjectionPool(sqlx::Error),
    #[error("ENCORE_MONGODB_URI: {0}")]
    MongoUri(mongodb::error::Error),
    #[error("MongoDB client: {0}")]
    MongoClient(mongodb::error::Error),
    #[error("cannot bind {addr}: {source}")]
    Bind { addr: SocketAddr, source: std::io::Error },
    #[error("task runtime setup: {0}")]
    TaskRuntime(String),
    #[error("the read side stopped: {0}")]
    Projections(String),
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
        let (pool, history_pool, projection_pool) = postgres_pools(&config).await?;
        let mongo = mongo_client(&config).await?;
        Ok(Self {
            config,
            pool,
            history_pool,
            projection_pool,
            mongo,
            shutdown: CancellationToken::new(),
        })
    }

    /// Serves `app` until a stop signal: the HTTP API, the outbox task runtime and the projection processors
    /// side by side. A stopping processor finishes what is in flight (up to one lease) and gives its
    /// checkpoint back; then the pools close.
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
        let served = run_components(
            inbound,
            task_runtime,
            app.projections,
            self.shutdown.clone(),
            shutdown_signal(),
        )
        .await;
        self.pool.close().await;
        self.history_pool.close().await;
        self.projection_pool.close().await;
        tracing::info!("encore-backend stopped");
        served
    }
}

/// Runs the inbound server and the task runtime under `lifecycle::run`, with the projection processors
/// beside them on the same token: the processors start only now, after everything that could fail at start
/// has succeeded, so a failed start never leaves a checkpoint held. Whatever stops first stops the rest
/// (the projection runtime cancels the token when it ends). A panicking processor is an error of the process;
/// a host error is logged, as before.
pub async fn run_components<I, T, S>(
    inbound: I,
    task_runtime: T,
    projections: ProjectionRuntime,
    shutdown: CancellationToken,
    signal: S,
) -> Result<(), ProcessError>
where
    I: Future<Output = io::Result<()>>,
    T: Future<Output = Result<(), TaskRuntimeError>> + Send + 'static,
    S: Future<Output = io::Result<()>>,
{
    let read_side = tokio::spawn(projections.run(shutdown.clone()));
    if let Err(error) = lifecycle::run(inbound, task_runtime, shutdown.clone(), signal).await {
        error.log("encore-backend stopped with an error");
    }
    match read_side.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(ProcessError::Projections(error.to_string())),
        Err(error) => Err(ProcessError::Projections(error.to_string())),
    }
}

/// The main, the history and the projection pool over the same address, all connecting lazily.
async fn postgres_pools(config: &Config) -> Result<(PgPool, PgPool, PgPool), ProcessError> {
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
    let projection_pool = connect_pool_with(
        options.clone(),
        PgPoolOptions::new().max_connections(config.database_projection_pool_size.get()),
        ConnectionPingPolicy::Idle(DEFAULT_IDLE_PING_AFTER),
        config.database_statement_timeout,
    )
    .await
    .map_err(ProcessError::ProjectionPool)?;
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
    Ok((pool, history_pool, projection_pool))
}

/// Short server-selection and connect timeouts: when MongoDB is down the read side fails fast and retries
/// (the processors back off, the status answers with the rows it saw last) instead of hanging for the
/// driver's default 30 s; the commands never touch MongoDB.
async fn mongo_client(config: &Config) -> Result<mongodb::Client, ProcessError> {
    let mut options = mongodb::options::ClientOptions::parse(&config.mongodb_uri)
        .await
        .map_err(ProcessError::MongoUri)?;
    options.server_selection_timeout = Some(Duration::from_secs(3));
    options.connect_timeout = Some(Duration::from_secs(3));
    options.app_name = Some("encore-backend".into());
    mongodb::Client::with_options(options).map_err(ProcessError::MongoClient)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::cqrs::{
        aggregate::Aggregate,
        event::{DomainEvent, EventEnvelope},
        event_sink::EventSink,
        projection::{
            View, ViewProjection,
            processor::{
                ProjectionProcessor,
                config::ProcessorConfig,
                observer::{LeaseEvent, ProjectionObserver},
            },
        },
        testing::{
            mem_checkpoint_store::MemCheckpointStore, mem_event_repository::MemEventRepository,
            mem_view_repository::MemViewRepository,
        },
    };
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Counted;

    impl DomainEvent for Counted {
        fn event_type(&self) -> String {
            "counted".into()
        }
        fn event_version(&self) -> String {
            "1".into()
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("never")]
    struct Never;

    #[derive(Debug, Default, Clone, Serialize, Deserialize)]
    struct Counter;

    impl Aggregate for Counter {
        const TYPE: &'static str = "counter";
        type Command = ();
        type Event = Counted;
        type Error = Never;
        type Services = ();
        async fn handle(&mut self, _: (), _: &(), _: &EventSink<Self>) -> Result<(), Never> {
            Ok(())
        }
        fn apply(&mut self, _: Counted) {}
    }

    #[derive(Debug, Default, Serialize, Deserialize)]
    struct Total;

    impl View<Counter> for Total {
        fn update(&mut self, _: &EventEnvelope<Counter>) {}
    }

    /// An observer that panics as soon as its processor holds the checkpoint: a bug in the read side.
    struct Panicking;

    impl ProjectionObserver for Panicking {
        fn lease(&self, event: LeaseEvent) {
            assert!(event != LeaseEvent::Acquired, "a bug in an observer");
        }
    }

    fn runtime(observer: Option<Arc<dyn ProjectionObserver>>) -> ProjectionRuntime {
        let processor = ProjectionProcessor::new(
            "totals",
            Arc::new(MemEventRepository::default()),
            Arc::new(MemCheckpointStore::default()),
            Arc::new(ViewProjection::new(Arc::new(MemViewRepository::<Total, Counter>::default()))),
            ProcessorConfig::default(),
        );
        let processor = match observer {
            Some(observer) => processor.with_observer(observer),
            None => processor,
        };
        ProjectionRuntime::new().with_processor(processor)
    }

    /// An inbound server and a task runtime that run until the token is cancelled.
    fn components(
        shutdown: &CancellationToken,
    ) -> (
        impl Future<Output = io::Result<()>>,
        impl Future<Output = Result<(), TaskRuntimeError>> + Send + 'static,
    ) {
        let (inbound, runtime) = (shutdown.clone(), shutdown.clone());
        (
            async move {
                inbound.cancelled().await;
                Ok(())
            },
            async move {
                runtime.cancelled().await;
                Ok(())
            },
        )
    }

    #[tokio::test]
    async fn a_panicking_processor_stops_the_server_and_fails_the_process() {
        let shutdown = CancellationToken::new();
        let (inbound, task_runtime) = components(&shutdown);
        let result = run_components(
            inbound,
            task_runtime,
            runtime(Some(Arc::new(Panicking))),
            shutdown.clone(),
            std::future::pending(),
        )
        .await;
        assert!(matches!(result, Err(ProcessError::Projections(message)) if message.contains("totals")));
        assert!(shutdown.is_cancelled(), "the panic stopped everything else");
    }

    #[tokio::test]
    async fn a_requested_stop_ends_every_component_and_the_processors_last() {
        let shutdown = CancellationToken::new();
        let (inbound, task_runtime) = components(&shutdown);
        let stop = shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            stop.cancel();
        });
        let result = run_components(inbound, task_runtime, runtime(None), shutdown.clone(), std::future::pending()).await;
        assert!(result.is_ok(), "{result:?}");
    }
}
