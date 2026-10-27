//! The process around the box office: `.env`, logging, the configuration, the connections, the HTTP
//! server, the outbox task runtime and the projection processors, start and stop. Nothing here knows a
//! concert from a fan: the application is assembled from a [`Process`] by `bootstrap`.
use crate::{
    bootstrap::{Wiring, routes},
    config::{Config, ConfigError, duration_text, statement_timeout_text},
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
use mongodb::options::ClientOptions;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{
    future::{Future, IntoFuture},
    io,
    net::SocketAddr,
    num::NonZeroU32,
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
        // each; a retry waits the pause of its task type's policy. Prepared here (no background work yet), started by
        // `lifecycle::run`.
        let runtime = prepare_task_runtime(
            self.pool.clone(),
            app.dispatcher,
            self.config.task_runtime.clone(),
            app.policies,
        );
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

/// The name the process gives its connections: PostgreSQL's `application_name`, MongoDB's `appName`.
const APPLICATION_NAME: &str = "encore-backend";

/// How the process connects one PostgreSQL pool: what it sets; everything else is SQLx's default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PostgresPool {
    pub max_connections: NonZeroU32,
    /// `None` keeps SQLx's minimum (none).
    pub min_connections: Option<u32>,
    /// `None` keeps SQLx's idle timeout (10 minutes).
    pub idle_timeout: Option<Duration>,
    pub ping: ConnectionPingPolicy,
    pub statement_timeout: StatementTimeout,
}

impl PostgresPool {
    /// SQLx's pool options with what this plan sets on top of its defaults.
    pub fn options(&self) -> PgPoolOptions {
        let mut options = PgPoolOptions::new().max_connections(self.max_connections.get());
        if let Some(min_connections) = self.min_connections {
            options = options.min_connections(min_connections);
        }
        if let Some(idle_timeout) = self.idle_timeout {
            options = options.idle_timeout(idle_timeout);
        }
        options
    }

    async fn connect(&self, connection: PgConnectOptions) -> Result<PgPool, sqlx::Error> {
        connect_pool_with(connection, self.options(), self.ping, self.statement_timeout).await
    }

    /// The pool as it is opened: SQLx's options read back, the ping policy and the statement timeout.
    fn describe(&self, name: &str) -> Vec<(String, String)> {
        let options = self.options();
        let optional = |duration: Option<Duration>| duration.map_or_else(|| "none".to_owned(), duration_text);
        let ping = match self.ping {
            ConnectionPingPolicy::Default => "always".to_owned(),
            ConnectionPingPolicy::Idle(after) => format!("idle {}", duration_text(after)),
        };
        [
            ("acquire_timeout", duration_text(options.get_acquire_timeout())),
            ("idle_timeout", optional(options.get_idle_timeout())),
            ("max_connections", options.get_max_connections().to_string()),
            ("max_lifetime", optional(options.get_max_lifetime())),
            ("min_connections", options.get_min_connections().to_string()),
            ("ping", ping),
            ("statement_timeout", statement_timeout_text(self.statement_timeout)),
        ]
        .into_iter()
        .map(|(field, value)| (format!("postgres.{name}.{field}"), value))
        .collect()
    }
}

/// The three PostgreSQL pools of the process over the same address, as `postgres_pools` opens them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolPlan {
    pub application_name: &'static str,
    /// Commands, the outbox queue and the checkpoints: short static statements, each under the statement timeout.
    pub main: PostgresPool,
    /// The projection processors' reads of the journal, under the main pool's statement timeout but apart from the
    /// commands' connections.
    pub projection: PostgresPool,
    /// Whole-history reads (verify, the ledger totals and the time machine): no statement timeout, and no connection
    /// kept while none of them runs.
    pub history: PostgresPool,
}

impl PoolPlan {
    /// Every pool as `postgres.<pool>.<field>` and the application name, sorted by key (see `Config::describe`).
    pub fn describe(&self) -> Vec<(String, String)> {
        let mut lines = vec![("postgres.application_name".to_owned(), self.application_name.to_owned())];
        lines.extend(self.main.describe("main"));
        lines.extend(self.projection.describe("projection"));
        lines.extend(self.history.describe("history"));
        lines.sort();
        lines
    }
}

/// The pools of `config`: the sizes and the statement timeout it names, a ping only after 100 ms idle, and a history
/// pool without a cap that keeps no idle connection longer than 30 s.
pub fn pool_plan(config: &Config) -> PoolPlan {
    let ping = ConnectionPingPolicy::Idle(DEFAULT_IDLE_PING_AFTER);
    PoolPlan {
        application_name: APPLICATION_NAME,
        main: PostgresPool {
            max_connections: config.database_pool_size,
            min_connections: None,
            idle_timeout: None,
            ping,
            statement_timeout: config.database_statement_timeout,
        },
        projection: PostgresPool {
            max_connections: config.database_projection_pool_size,
            min_connections: None,
            idle_timeout: None,
            ping,
            statement_timeout: config.database_statement_timeout,
        },
        history: PostgresPool {
            max_connections: config.database_history_pool_size,
            min_connections: Some(0),
            idle_timeout: Some(Duration::from_secs(30)),
            ping,
            statement_timeout: StatementTimeout::Unlimited,
        },
    }
}

/// The main, the history and the projection pool over the same address, as `pool_plan` says.
async fn postgres_pools(config: &Config) -> Result<(PgPool, PgPool, PgPool), ProcessError> {
    let plan = pool_plan(config);
    let connection = config
        .database_url
        .parse::<PgConnectOptions>()
        .map_err(ProcessError::DatabaseUrl)?
        .application_name(plan.application_name);
    let pool = plan.main.connect(connection.clone()).await.map_err(ProcessError::Pool)?;
    let projection_pool = plan
        .projection
        .connect(connection.clone())
        .await
        .map_err(ProcessError::ProjectionPool)?;
    let history_pool = plan.history.connect(connection).await.map_err(ProcessError::HistoryPool)?;
    Ok((pool, history_pool, projection_pool))
}

/// How the process connects to MongoDB, on top of what its URI says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MongoPlan {
    pub max_pool_size: NonZeroU32,
    pub server_selection_timeout: Duration,
    pub connect_timeout: Duration,
    pub app_name: &'static str,
}

impl MongoPlan {
    /// Sets the plan's options, whatever the URI said about them.
    pub fn apply(&self, options: &mut ClientOptions) {
        options.server_selection_timeout = Some(self.server_selection_timeout);
        options.connect_timeout = Some(self.connect_timeout);
        options.app_name = Some(self.app_name.into());
        options.max_pool_size = Some(self.max_pool_size.get());
    }

    /// The client as `mongodb.<option>`, sorted by key (see `Config::describe`).
    pub fn describe(&self) -> Vec<(String, String)> {
        vec![
            ("mongodb.app_name".to_owned(), self.app_name.to_owned()),
            ("mongodb.connect_timeout".to_owned(), duration_text(self.connect_timeout)),
            ("mongodb.max_pool_size".to_owned(), self.max_pool_size.to_string()),
            (
                "mongodb.server_selection_timeout".to_owned(),
                duration_text(self.server_selection_timeout),
            ),
        ]
    }
}

/// Short server-selection and connect timeouts: when MongoDB is down the read side fails fast and retries
/// (the processors back off, the status answers with the rows it saw last) instead of hanging for the
/// driver's default 30 s; the commands never touch MongoDB. The pool is `ENCORE_MONGODB_POOL_SIZE`.
pub fn mongo_plan(config: &Config) -> MongoPlan {
    MongoPlan {
        max_pool_size: config.mongodb_pool_size,
        server_selection_timeout: Duration::from_secs(3),
        connect_timeout: Duration::from_secs(3),
        app_name: APPLICATION_NAME,
    }
}

async fn mongo_client(config: &Config) -> Result<mongodb::Client, ProcessError> {
    let mut options = ClientOptions::parse(&config.mongodb_uri)
        .await
        .map_err(ProcessError::MongoUri)?;
    mongo_plan(config).apply(&mut options);
    mongodb::Client::with_options(options).map_err(ProcessError::MongoClient)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::cqrs::{
        aggregate::Aggregate,
        command::decide::Decide,
        event::{DomainEvent, EventEnvelope},
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
        type Id = String;
        type Command = ();
        type Event = Counted;
        type Error = Never;
        type Rules = ();
        fn apply(&mut self, _: Counted) {}
    }

    impl Decide<Counter> for () {
        fn decide(self, _: &Counter, _: &()) -> Result<Counted, Never> {
            Ok(Counted)
        }
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

    #[test]
    fn the_pool_plan_takes_the_sizes_and_the_timeout_of_the_configuration() {
        let mut config = crate::testing::config();
        config.database_pool_size = NonZeroU32::new(7).unwrap();
        config.database_projection_pool_size = NonZeroU32::new(3).unwrap();
        config.database_history_pool_size = NonZeroU32::new(2).unwrap();
        config.database_statement_timeout = StatementTimeout::Custom(Duration::from_millis(2500));
        let plan = pool_plan(&config);
        let ping = ConnectionPingPolicy::Idle(Duration::from_millis(100));
        let capped = |size| PostgresPool {
            max_connections: NonZeroU32::new(size).unwrap(),
            min_connections: None,
            idle_timeout: None,
            ping,
            statement_timeout: StatementTimeout::Custom(Duration::from_millis(2500)),
        };
        assert_eq!((plan.main, plan.projection), (capped(7), capped(3)));
        assert_eq!(
            plan.history,
            PostgresPool {
                max_connections: NonZeroU32::new(2).unwrap(),
                min_connections: Some(0),
                idle_timeout: Some(Duration::from_secs(30)),
                ping,
                statement_timeout: StatementTimeout::Unlimited,
            }
        );
        assert_eq!(plan.application_name, "encore-backend");
        let (main, history) = (plan.main.options(), plan.history.options());
        assert_eq!(
            (
                main.get_max_connections(),
                main.get_min_connections(),
                main.get_idle_timeout()
            ),
            (7, 0, Some(Duration::from_secs(600))),
            "SQLx's minimum and idle timeout where the plan sets none"
        );
        assert_eq!(
            (
                history.get_max_connections(),
                history.get_min_connections(),
                history.get_idle_timeout()
            ),
            (2, 0, Some(Duration::from_secs(30)))
        );
        let lines = plan.describe();
        assert!(lines.is_sorted());
        for line in [
            ("postgres.main.statement_timeout", "2500 ms"),
            ("postgres.history.statement_timeout", "unlimited"),
            ("postgres.projection.ping", "idle 100 ms"),
        ] {
            assert!(
                lines.contains(&(line.0.to_owned(), line.1.to_owned())),
                "{line:?} in {lines:?}"
            );
        }
        let always = PostgresPool {
            ping: ConnectionPingPolicy::Default,
            ..plan.main
        };
        assert!(
            always
                .describe("main")
                .contains(&("postgres.main.ping".to_owned(), "always".to_owned()))
        );
    }

    #[tokio::test]
    async fn the_mongo_plan_overrides_what_the_uri_says() {
        let mut config = crate::testing::config();
        config.mongodb_pool_size = NonZeroU32::new(12).unwrap();
        let plan = mongo_plan(&config);
        let mut options = ClientOptions::parse(
            "mongodb://localhost:27018/?maxPoolSize=50&appName=other&connectTimeoutMS=100&serverSelectionTimeoutMS=100",
        )
        .await
        .unwrap();
        plan.apply(&mut options);
        assert_eq!(
            (
                options.max_pool_size,
                options.server_selection_timeout,
                options.connect_timeout,
                options.app_name.as_deref()
            ),
            (
                Some(12),
                Some(Duration::from_secs(3)),
                Some(Duration::from_secs(3)),
                Some("encore-backend")
            )
        );
        assert_eq!(
            plan.describe(),
            [
                ("mongodb.app_name", "encore-backend"),
                ("mongodb.connect_timeout", "3 s"),
                ("mongodb.max_pool_size", "12"),
                ("mongodb.server_selection_timeout", "3 s"),
            ]
            .map(|(key, value)| (key.to_owned(), value.to_owned()))
        );
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
