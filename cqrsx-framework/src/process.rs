//! The process of a framework: the task runtime and the projection processors side by side with the server of the
//! application until a signal, then stopped in order: the server, the task runtime, the processors (they finish what is
//! in flight and give their checkpoints back), the pools. The server is any future over the framework that ends when
//! [`Cqrs::stopping`] is cancelled: `cqrsx_framework::axum::server` (feature `axum`), a server of another protocol the
//! application brings, or none.
//!
//! Requires a running Tokio runtime. The process observes the shutdown signal, shares one cancellation token and drains
//! its components; the task runtime enforces its own drain budget, and the process imposes no inbound or cleanup
//! timeout. Dropping a polled process cancels its token and detaches the task runtime task for independent drain,
//! without guaranteeing inbound or resource cleanup. The parts are private: [`Builder::serve`] and [`Cqrs::run`] are
//! the way in, [`ServeError`] is what comes out, [`shutdown_signal`] is the signal.
mod error;
mod lifecycle;
mod logging;

use crate::{
    Cqrs,
    build::{BuildError, Builder},
    setup::SetupError,
};
use cqrsx::{
    mutex::lock,
    worker::runtime::{TaskRuntimeError, prepare},
};
use lifecycle::run_with_processors;
use std::{future::Future, io, net::SocketAddr, pin::Pin, sync::Arc};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// Why a process did not start or stopped with a failure, as the process writes it: its logger, its settings or
/// modules (`invalid configuration: …`), a store it could not open, its address, or its read side.
#[derive(Debug, Error)]
pub enum ServeError {
    #[error("logging setup failed: {0}")]
    Logging(String),
    #[error("{}", build_error_text(.0))]
    Build(#[from] BuildError),
    #[error("cannot bind {addr}: {source}")]
    Bind { addr: SocketAddr, source: io::Error },
    #[error(transparent)]
    ReadSide(#[from] ReadSideStopped),
}

/// A setting, a module's variable, a URL or URI of a store, views without a durable journal and a journal or views
/// without a store are an invalid configuration; the rest as it is. A key described twice with two values is not: the
/// code of the modules (or of the framework) reads one key in two ways, which no value of a variable mends for good.
fn build_error_text(error: &BuildError) -> String {
    let invalid = match error {
        BuildError::Settings(_)
        | BuildError::Setup(SetupError::Invalid { .. })
        | BuildError::ViewsWithoutJournal(_)
        | BuildError::NoJournal
        | BuildError::ViewsWithoutStore => true,
        #[cfg(feature = "event-store-postgres")]
        BuildError::DatabaseUrl { .. } => true,
        #[cfg(feature = "projections-mongodb")]
        BuildError::MongoUri { .. } => true,
        BuildError::Setup(SetupError::Failed(_))
        | BuildError::AggregateTwice(_)
        | BuildError::ViewTwice(_)
        | BuildError::ObserverTwice(_)
        | BuildError::DescribedTwice { .. }
        | BuildError::Policy(_)
        | BuildError::Tasks(_) => false,
        #[cfg(feature = "event-store-postgres")]
        BuildError::MainPool(_) | BuildError::HistoryPool(_) | BuildError::ProjectionPool(_) => false,
        #[cfg(feature = "projections-mongodb")]
        BuildError::MongoClient(_) => false,
    };
    if invalid {
        format!("invalid configuration: {error}")
    } else {
        error.to_string()
    }
}

/// The projection processors failed: one panicked (and stopped the others), or their runtime did.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("the read side stopped: {0}")]
pub struct ReadSideStopped(pub String);

type TaskRuntimeRun = Pin<Box<dyn Future<Output = Result<(), TaskRuntimeError>> + Send>>;

impl Builder {
    /// The process: installs the logger (`RUST_LOG`, one level, once per process), builds the framework, binds the
    /// address of its settings and [runs](Cqrs::run) it until SIGINT or SIGTERM with `server`, which gets the listener and
    /// the framework and returns the future that serves it: `cqrsx_framework::axum::server` (feature `axum`), or the
    /// server of another protocol the application brings.
    pub async fn serve<S, F>(self, server: S) -> Result<(), ServeError>
    where
        S: FnOnce(TcpListener, Arc<Cqrs>) -> F,
        F: Future<Output = io::Result<()>>,
    {
        logging::LoggingConfig::from_env()
            .map_err(|error| ServeError::Logging(error.to_string()))?
            .try_init()
            .map_err(|error| ServeError::Logging(error.to_string()))?;
        let cqrs = Arc::new(self.build().await?);
        let addr = cqrs.settings().http_addr();
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|source| ServeError::Bind { addr, source })?;
        tracing::info!("{} listening on {addr}", cqrs.settings().application_name());
        let inbound = server(listener, cqrs.clone());
        Ok(cqrs.run(inbound, shutdown_signal()).await?)
    }
}

/// Observe SIGINT or SIGTERM on Unix, and Ctrl-C on other platforms.
///
/// Failure to install/observe a signal handler is returned to the process. A closed
/// Unix SIGTERM stream is an error rather than a successful stop request.
#[cfg(unix)]
pub async fn shutdown_signal() -> io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    wait_for_signal(tokio::signal::ctrl_c(), terminate.recv()).await
}

/// Observe SIGINT or SIGTERM on Unix, and Ctrl-C on other platforms.
#[cfg(not(unix))]
pub async fn shutdown_signal() -> io::Result<()> {
    tokio::signal::ctrl_c().await
}

#[cfg(unix)]
async fn wait_for_signal(
    interrupt: impl Future<Output = io::Result<()>>,
    terminate: impl Future<Output = Option<()>>,
) -> io::Result<()> {
    tokio::select! {
        result = interrupt => result,
        signal = terminate => signal
            .ok_or_else(|| io::Error::other("SIGTERM signal stream closed")),
    }
}

impl Cqrs {
    /// Runs the framework next to the task runtime (over the framework's queue, the policies of the task types) and the
    /// projection processors until `signal` or the framework is asked to stop. `inbound` is a server over the framework
    /// that ends when [`stopping`](Cqrs::stopping) is cancelled (`cqrsx_framework::axum::server` with the feature `axum`,
    /// or a server the application brings), or for a process without one a future that waits for the same
    /// (`async { stopping.cancelled().await; Ok(()) }` over a clone of `stopping()`): an `inbound` that ends before the
    /// stop is a failure of the process, and the stop waits for it. `signal` ends the process ([`shutdown_signal`], or a
    /// test's future). Then the server stops taking requests, the task runtime drains,
    /// the processors finish what is in flight and give their checkpoints back, and the pools close. A failure of the
    /// server or the task runtime is logged; a panicking processor fails the process. The processors run once: a second
    /// call runs the rest without them.
    pub async fn run(
        &self,
        inbound: impl Future<Output = io::Result<()>>,
        signal: impl Future<Output = io::Result<()>>,
    ) -> Result<(), ReadSideStopped> {
        let name = self.settings.application_name().to_owned();
        let shutdown = self.shutdown.clone();
        let processors = std::mem::take(&mut *lock(&self.processors, "framework processors"));
        let task_runtime = self.task_runtime(shutdown.clone());
        let stopped = run_with_processors(inbound, task_runtime, processors, shutdown, signal).await;
        if let Err(error) = &stopped.process {
            error.log(&format!("{name} stopped with an error"));
        }
        self.close().await;
        tracing::info!("{name} stopped");
        stopped.read_side
    }

    /// The task runtime over the framework's queue, prepared now and started by its first poll; without a task type, a
    /// runtime with nothing to do until the stop.
    fn task_runtime(&self, shutdown: CancellationToken) -> TaskRuntimeRun {
        let Some(dispatcher) = self.dispatcher.clone() else {
            return Box::pin(async move {
                shutdown.cancelled().await;
                Ok(())
            });
        };
        let (config, policies) = (self.settings.task_runtime().clone(), self.policies.clone());
        Box::pin(prepare(self.journal.task_queue(), dispatcher, config, policies).run(shutdown))
    }

    /// Closes the pools of a framework over PostgreSQL: the main pool, the history pool, the projection pool.
    async fn close(&self) {
        self.journal.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{memory::InMemory, settings::SettingsError, setup::SetupError, test_support::*};
    use cqrsx::projection::checkpoint::CheckpointStore;
    use std::{
        future::{pending, ready},
        time::Duration,
    };

    #[test]
    fn startup_errors_read_as_the_process_wrote_them() {
        let invalid = |variable: &str| SettingsError::Invalid {
            variable: variable.to_owned(),
            message: "must be positive".to_owned(),
        };
        let errors = [
            ServeError::Logging("unable to initialize process logger".into()),
            ServeError::Build(BuildError::Settings(invalid("ENCORE_SNAPSHOT_EVERY"))),
            ServeError::Build(BuildError::Setup(SetupError::Invalid {
                variable: "ENCORE_TICKET_LIMIT".into(),
                message: "must be positive".into(),
            })),
            ServeError::Build(BuildError::Setup(SetupError::failed(io::Error::other("no client")))),
            ServeError::Build(BuildError::AggregateTwice("show")),
            ServeError::Build(BuildError::DescribedTwice {
                key: "ENCORE_TICKET_LIMIT".into(),
                first: "4".into(),
                second: "5".into(),
            }),
            ServeError::Build(BuildError::ViewsWithoutJournal("MongoDB")),
            ServeError::Build(BuildError::NoJournal),
            ServeError::Build(BuildError::ViewsWithoutStore),
            ServeError::Bind {
                addr: "127.0.0.1:8080".parse().unwrap(),
                source: io::Error::other("in use"),
            },
            ServeError::ReadSide(ReadSideStopped("projection processor halls panicked".into())),
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
                "invalid configuration: the framework has no journal: give the builder `event_store(..)`",
                "invalid configuration: the views have no store: give the builder `projections(..)`",
                "cannot bind 127.0.0.1:8080: in use",
                "the read side stopped: projection processor halls panicked",
            ]
        );
    }

    /// A URL or URI of a store that does not parse is an invalid configuration too.
    #[test]
    fn an_unparsable_store_address_is_an_invalid_configuration() {
        #[cfg(feature = "event-store-postgres")]
        assert_eq!(
            ServeError::from(BuildError::DatabaseUrl {
                variable: "SHOP_DATABASE_URL".into(),
                source: sqlx::Error::Configuration("invalid port number".into()),
            })
            .to_string(),
            "invalid configuration: SHOP_DATABASE_URL: error with configuration: invalid port number"
        );
        #[cfg(feature = "projections-mongodb")]
        assert!(
            ServeError::from(BuildError::MongoUri {
                variable: "SHOP_MONGODB_URI".into(),
                source: ::mongodb::error::Error::custom("no scheme"),
            })
            .to_string()
            .starts_with("invalid configuration: SHOP_MONGODB_URI: ")
        );
        #[cfg(feature = "event-store-postgres")]
        assert_eq!(
            ServeError::from(BuildError::MainPool(sqlx::Error::PoolTimedOut)).to_string(),
            "cannot connect to PostgreSQL: pool timed out while waiting for an open connection"
        );
    }

    /// The process: the logger once, the framework, its address. A taken address fails the start; the logger of a second
    /// start in one process fails it before anything else.
    #[test]
    fn a_taken_address_fails_the_start() {
        use crate::test_support;
        if test_support::scenario().is_some() {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async {
                let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let addr = taken.local_addr().unwrap().to_string();
                let serve = || {
                    let addr = addr.clone();
                    Cqrs::from_lookup("SHOP", move |name| (name == "SHOP_HTTP_ADDR").then(|| addr.clone()))
                        .event_store(InMemory)
                        .projections(InMemory)
                        .module(shows)
                        .serve(|_, _| ready(Ok(())))
                };
                let refused = serve().await.unwrap_err();
                assert!(matches!(refused, ServeError::Bind { .. }), "{refused:?}");
                assert!(refused.to_string().starts_with(&format!("cannot bind {addr}: ")), "{refused}");
                assert!(matches!(serve().await, Err(ServeError::Logging(_))), "one logger per process");
            });
            return;
        }
        test_support::isolated(concat!(module_path!(), "::a_taken_address_fails_the_start"), "taken", |_| {});
    }

    /// G-30 without a server: the task runtime runs the chain and the processor writes the hall until the signal; then
    /// the processors give their checkpoints back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_process_without_a_server_runs_the_tasks_and_the_processors_until_the_signal() {
        let mailbox = Arc::new(Mailbox::default());
        let cqrs = Arc::new(
            memory_builder()
                .module(shows)
                .module(wallets)
                .provide(mailbox.clone())
                .build()
                .await
                .unwrap(),
        );
        cqrs.source("ui")
            .execute::<Show>(&"s-1".into(), book("A1", "fox"))
            .await
            .unwrap();
        let checkpoints = cqrs.checkpoints().unwrap().clone();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let running = tokio::spawn({
            let cqrs = cqrs.clone();
            async move {
                let stopping = cqrs.stopping();
                let no_server = async move {
                    stopping.cancelled().await;
                    Ok(())
                };
                cqrs.run(no_server, async move {
                    let _ = stopped.await;
                    Ok(())
                })
                .await
            }
        });
        tokio::time::timeout(Duration::from_secs(10), async {
            while mailbox.sent().is_empty() || cqrs.view::<Hall>().get(&"s-1").await.unwrap().is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the task runtime ran the chain and the processor wrote the hall");
        stop.send(()).unwrap();
        assert_eq!(running.await.unwrap(), Ok(()));
        let rows = checkpoints.status(None).await.unwrap();
        assert!(
            rows.iter().all(|row| row.owner.is_none()),
            "the processors gave their checkpoints back: {rows:?}"
        );
    }

    #[test]
    fn registering_signal_observation_waits_without_sending_os_signals() {
        use crate::test_support;
        if test_support::scenario().is_some() {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            runtime.block_on(async {
                assert!(
                    tokio::time::timeout(std::time::Duration::ZERO, shutdown_signal())
                        .await
                        .is_err()
                );
            });
            return;
        }
        test_support::isolated(
            concat!(
                module_path!(),
                "::registering_signal_observation_waits_without_sending_os_signals"
            ),
            "signal_registration",
            |_| {},
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn interrupt_success() {
        assert!(wait_for_signal(ready(Ok(())), pending()).await.is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn interrupt_error_preserves_source() {
        let result = wait_for_signal(
            ready(Err(io::Error::new(io::ErrorKind::PermissionDenied, "interrupt failed"))),
            pending(),
        )
        .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn terminate_success() {
        assert!(wait_for_signal(pending(), ready(Some(()))).await.is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn terminate_closed_stream_is_error() {
        let error = wait_for_signal(pending(), ready(None)).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(error.to_string(), "SIGTERM signal stream closed");
    }
}
