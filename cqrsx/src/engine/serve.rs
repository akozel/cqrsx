//! The process of an engine (feature `http`): the HTTP API of its modules, the task runtime and the projection
//! processors side by side until a signal, then stopped in order: the server, the task runtime, the processors (they
//! finish what is in flight and give their checkpoints back), the pools.
#[cfg(feature = "postgres")]
use crate::postgres::task_runtime::prepare_task_runtime;
use crate::{
    engine::{
        Cqrs, Journal,
        build::{BuildError, Builder},
        setup::SetupError,
    },
    host::{
        lifecycle::{ReadSideStopped, run_with_processors},
        signal::shutdown_signal,
    },
    locking::lock,
    worker::runtime::{TaskRuntimeError, prepare},
};
use std::{
    future::{Future, IntoFuture},
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// Why a process did not start or stopped with a failure, as the process writes it: its logger, its settings or
/// modules (`invalid configuration: …`), a store it could not open, its address, or its read side.
#[derive(Debug, Error)]
pub enum StartupError {
    #[error("logging setup failed: {0}")]
    Logging(String),
    #[error("{}", built(.0))]
    Build(#[from] BuildError),
    #[error("cannot bind {addr}: {source}")]
    Bind { addr: SocketAddr, source: io::Error },
    #[error(transparent)]
    ReadSide(#[from] ReadSideStopped),
}

/// A setting, a module's variable, a URL or URI of a store and views without a durable journal are an invalid
/// configuration; the rest as it is. A key described twice with two values is not: the code of the modules (or of the
/// engine) reads one key in two ways, which no value of a variable mends for good.
fn built(error: &BuildError) -> String {
    let invalid = match error {
        BuildError::Settings(_) | BuildError::Setup(SetupError::Invalid { .. }) | BuildError::ViewsWithoutJournal(_) => true,
        #[cfg(feature = "postgres")]
        BuildError::DatabaseUrl { .. } => true,
        #[cfg(feature = "mongodb")]
        BuildError::MongoUri { .. } => true,
        BuildError::Setup(SetupError::Failed(_))
        | BuildError::AggregateTwice(_)
        | BuildError::ViewTwice(_)
        | BuildError::ObserverTwice(_)
        | BuildError::DescribedTwice { .. }
        | BuildError::Policy(_)
        | BuildError::Tasks(_) => false,
        #[cfg(feature = "postgres")]
        BuildError::Pool(_) | BuildError::HistoryPool(_) | BuildError::ProjectionPool(_) => false,
        #[cfg(feature = "mongodb")]
        BuildError::MongoClient(_) => false,
    };
    if invalid {
        format!("invalid configuration: {error}")
    } else {
        error.to_string()
    }
}

type TaskRuntimeRun = Pin<Box<dyn Future<Output = Result<(), TaskRuntimeError>> + Send>>;

impl Builder {
    /// The process: installs the logger (`RUST_LOG`, one level, once per process; feature `logging`), builds the
    /// engine, binds the address of its settings and [serves](Cqrs::serve) it until SIGINT or SIGTERM.
    pub async fn serve(self) -> Result<(), StartupError> {
        #[cfg(feature = "logging")]
        crate::logging::LoggingConfig::from_env()
            .map_err(|error| StartupError::Logging(error.to_string()))?
            .try_init()
            .map_err(|error| StartupError::Logging(error.to_string()))?;
        let cqrs = self.build().await?;
        let addr = cqrs.settings().http_addr();
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|source| StartupError::Bind { addr, source })?;
        cqrs.serve(listener, shutdown_signal()).await
    }
}

impl Cqrs {
    /// Serves the routes of the modules on `listener` next to the task runtime (over the engine's queue, the policies
    /// of the task types) and the projection processors, until `signal` or the engine is asked to stop. Then the server
    /// stops taking requests, the task runtime drains, the processors finish what is in flight and give their
    /// checkpoints back, and the pools close. A failure of the server or the task runtime is logged; a panicking
    /// processor fails the process.
    pub async fn serve(self, listener: TcpListener, signal: impl Future<Output = io::Result<()>>) -> Result<(), StartupError> {
        let name = self.settings.application_name().to_owned();
        if let Ok(addr) = listener.local_addr() {
            tracing::info!("{name} listening on http://{addr}");
        }
        let shutdown = self.shutdown.clone();
        let processors = std::mem::take(&mut *lock(&self.processors, "engine processors"));
        let task_runtime = self.task_runtime(shutdown.clone());
        let cqrs = Arc::new(self);
        // axum 0.8: `with_graceful_shutdown` returns an `IntoFuture`, not a `Future`.
        let inbound = axum::serve(listener, cqrs.router())
            .with_graceful_shutdown(shutdown.clone().cancelled_owned())
            .into_future();
        let stopped = run_with_processors(inbound, task_runtime, processors, shutdown, signal).await;
        if let Err(error) = &stopped.host {
            error.log(&format!("{name} stopped with an error"));
        }
        cqrs.close().await;
        tracing::info!("{name} stopped");
        Ok(stopped.read_side?)
    }

    /// The task runtime over the engine's queue, prepared now and started by its first poll; without a task type, a
    /// runtime with nothing to do until the stop.
    fn task_runtime(&self, shutdown: CancellationToken) -> TaskRuntimeRun {
        let Some(dispatcher) = self.dispatcher.clone() else {
            return Box::pin(async move {
                shutdown.cancelled().await;
                Ok(())
            });
        };
        let (config, policies) = (self.settings.task_runtime().clone(), self.policies.clone());
        match &self.journal {
            Journal::Memory => Box::pin(prepare(self.memory.queue.clone(), dispatcher, config, policies).run(shutdown)),
            #[cfg(feature = "postgres")]
            Journal::Postgres(pools) => {
                Box::pin(prepare_task_runtime(pools.main.clone(), dispatcher, config, policies).run(shutdown))
            }
        }
    }

    /// Closes the pools of an engine over PostgreSQL: the main pool, the history pool, the projection pool.
    async fn close(&self) {
        #[cfg(feature = "postgres")]
        if let Journal::Postgres(pools) = &self.journal {
            pools.main.close().await;
            pools.history.close().await;
            pools.projection.close().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        engine::{
            module::Module,
            settings::SettingsError,
            setup::{Setup, SetupError},
            test_support::*,
        },
        projection::checkpoint::CheckpointStore,
    };
    use axum::{
        Router,
        extract::{Path, State},
        routing::get,
    };
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    /// The front of the box office: a hall by its show, and a health check, routed in two calls.
    fn front(_: &Setup) -> Result<Module, SetupError> {
        async fn hall(State(cqrs): State<Arc<Cqrs>>, Path(show): Path<String>) -> String {
            match cqrs.view::<Hall>().get(&show).await {
                Ok(Some(hall)) => format!("{:?}", hall.view.seats),
                _ => "none".to_owned(),
            }
        }
        Ok(Module::new("front")
            .routes(Router::new().route("/halls/{show}", get(hall)))
            .routes(Router::new().route("/health", get(|| async { "ok" }))))
    }

    /// A `GET` over a connection of its own: the status line and the body.
    async fn fetch(addr: SocketAddr, path: &str) -> (String, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let request = format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        let status = response.lines().next().unwrap_or_default().to_owned();
        let body = response.split("\r\n\r\n").nth(1).unwrap_or_default().to_owned();
        (status, body)
    }

    /// G-30: the routes of the modules are served next to the task runtime and the processors; on the signal the
    /// server stops, the runtime drains and the processors give their checkpoints back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serve_answers_the_routes_of_its_modules_and_stops_on_the_signal() {
        let mailbox = Arc::new(Mailbox::default());
        let cqrs = Cqrs::in_memory()
            .module(shows)
            .module(wallets)
            .module(front)
            .provide(mailbox.clone())
            .build()
            .await
            .unwrap();
        cqrs.from("ui")
            .execute::<Show>(&"s-1".into(), book("A1", "fox"))
            .await
            .unwrap();
        let checkpoints = cqrs.checkpoints().clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let served = tokio::spawn(cqrs.serve(listener, async move {
            let _ = stopped.await;
            Ok(())
        }));
        assert_eq!(fetch(addr, "/health").await, ("HTTP/1.1 200 OK".to_owned(), "ok".to_owned()));
        tokio::time::timeout(Duration::from_secs(10), async {
            while mailbox.sent().is_empty() || fetch(addr, "/halls/s-1").await.1 == "none" {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the task runtime ran the chain and the processor wrote the hall");
        assert_eq!(fetch(addr, "/halls/s-1").await.1, r#"{"A1": "fox"}"#);
        assert_eq!(fetch(addr, "/nowhere").await.0, "HTTP/1.1 404 Not Found");
        stop.send(()).unwrap();
        served.await.unwrap().unwrap();
        let rows = checkpoints.status(None).await.unwrap();
        assert!(
            rows.iter().all(|row| row.owner.is_none()),
            "the processors gave their checkpoints back: {rows:?}"
        );
        assert!(TcpStream::connect(addr).await.is_err(), "the server stopped");
    }

    #[tokio::test]
    async fn an_engine_without_task_types_serves_until_the_signal() {
        let cqrs = Cqrs::in_memory().module(front).build().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        cqrs.serve(listener, async { Ok(()) }).await.unwrap();
    }

    #[test]
    fn startup_errors_read_as_the_process_wrote_them() {
        let invalid = |variable: &str| SettingsError::Invalid {
            variable: variable.to_owned(),
            message: "must be positive".to_owned(),
        };
        let errors = [
            StartupError::Logging("unable to initialize process logger".into()),
            StartupError::Build(BuildError::Settings(invalid("ENCORE_SNAPSHOT_EVERY"))),
            StartupError::Build(BuildError::Setup(SetupError::Invalid {
                variable: "ENCORE_TICKET_LIMIT".into(),
                message: "must be positive".into(),
            })),
            StartupError::Build(BuildError::Setup(SetupError::failed(io::Error::other("no client")))),
            StartupError::Build(BuildError::AggregateTwice("show")),
            StartupError::Build(BuildError::DescribedTwice {
                key: "ENCORE_TICKET_LIMIT".into(),
                first: "4".into(),
                second: "5".into(),
            }),
            StartupError::Build(BuildError::ViewsWithoutJournal("MongoDB")),
            StartupError::Bind {
                addr: "127.0.0.1:8080".parse().unwrap(),
                source: io::Error::other("in use"),
            },
            StartupError::ReadSide(ReadSideStopped("projection processor halls panicked".into())),
        ];
        assert_eq!(
            errors.map(|error| error.to_string()),
            [
                "logging setup failed: unable to initialize process logger",
                "invalid configuration: ENCORE_SNAPSHOT_EVERY: must be positive",
                "invalid configuration: ENCORE_TICKET_LIMIT: must be positive",
                "no client",
                "the aggregate type `show` is registered twice: one executor per aggregate type",
                "the key `ENCORE_TICKET_LIMIT` is described twice, as `4` and as `5`",
                "invalid configuration: the views in MongoDB need the journal in PostgreSQL",
                "cannot bind 127.0.0.1:8080: in use",
                "the read side stopped: projection processor halls panicked",
            ]
        );
    }

    /// A URL or URI of a store that does not parse is an invalid configuration too.
    #[test]
    fn an_unparsable_store_address_is_an_invalid_configuration() {
        #[cfg(feature = "postgres")]
        assert_eq!(
            StartupError::from(BuildError::DatabaseUrl {
                variable: "SHOP_DATABASE_URL".into(),
                source: sqlx::Error::Configuration("invalid port number".into()),
            })
            .to_string(),
            "invalid configuration: SHOP_DATABASE_URL: error with configuration: invalid port number"
        );
        #[cfg(feature = "mongodb")]
        assert!(
            StartupError::from(BuildError::MongoUri {
                variable: "SHOP_MONGODB_URI".into(),
                source: ::mongodb::error::Error::custom("no scheme"),
            })
            .to_string()
            .starts_with("invalid configuration: SHOP_MONGODB_URI: ")
        );
        #[cfg(feature = "postgres")]
        assert_eq!(
            StartupError::from(BuildError::Pool(sqlx::Error::PoolTimedOut)).to_string(),
            "cannot connect to PostgreSQL: pool timed out while waiting for an open connection"
        );
    }

    /// The process: the logger once, the engine, its address. A taken address fails the start; the logger of a second
    /// start in one process fails it before anything else.
    #[test]
    fn a_taken_address_fails_the_start() {
        use crate::env::test_support;
        if test_support::scenario().is_some() {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async {
                let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let addr = taken.local_addr().unwrap().to_string();
                let serve = || {
                    let addr = addr.clone();
                    Cqrs::from_lookup("SHOP", move |name| (name == "SHOP_HTTP_ADDR").then(|| addr.clone()))
                        .module(shows)
                        .serve()
                };
                let refused = serve().await.unwrap_err();
                assert!(matches!(refused, StartupError::Bind { .. }), "{refused:?}");
                assert!(refused.to_string().starts_with(&format!("cannot bind {addr}: ")), "{refused}");
                #[cfg(feature = "logging")]
                assert!(
                    matches!(serve().await, Err(StartupError::Logging(_))),
                    "one logger per process"
                );
                let invalid = Cqrs::from_lookup("SHOP", |name| (name == "SHOP_SNAPSHOT_EVERY").then(|| "0".to_owned()));
                #[cfg(not(feature = "logging"))]
                assert_eq!(
                    invalid.serve().await.unwrap_err().to_string(),
                    "invalid configuration: SHOP_SNAPSHOT_EVERY: must be positive"
                );
                #[cfg(feature = "logging")]
                drop(invalid);
            });
            return;
        }
        test_support::isolated(concat!(module_path!(), "::a_taken_address_fails_the_start"), "taken", |_| {});
    }
}
