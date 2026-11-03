//! The HTTP API of a framework (feature `axum`, axum 0.8): every module routes its requests under the framework as their
//! state ([`Module::routes`]); [`Cqrs::router`] is the routes of every module with a trace of every request, and
//! [`server`] serves it for [`Builder::serve`](crate::build::Builder::serve) and [`Cqrs::run`].
use crate::{Cqrs, module::Module};
use ::axum::Router;
use std::{io, sync::Arc};
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;

impl Module {
    /// The routes of the module, one of its [endpoints](Module::endpoints): its handlers take the framework as their
    /// state (`State<Arc<Cqrs>>`) and reach the commands, the views and the module's dependencies through it. Two
    /// modules routing one path fail the build of the framework with axum's panic.
    pub fn routes(self, routes: Router<Arc<Cqrs>>) -> Self {
        self.endpoints(routes)
    }
}

impl Cqrs {
    /// The routes of every module under this framework, in the order the modules were registered, with a trace of every
    /// request (`TraceLayer::new_for_http`). Two modules routing one path fail with axum's panic.
    pub fn router(self: &Arc<Self>) -> Router {
        self.endpoints::<Router<Arc<Cqrs>>>()
            .fold(Router::new(), |router, routes| router.merge(routes.clone()))
            .layer(TraceLayer::new_for_http())
            .with_state(self.clone())
    }
}

/// Serves [`Cqrs::router`] on `listener` until the framework stops ([`Cqrs::stopping`]): the server of
/// [`Builder::serve`](crate::build::Builder::serve) and the `inbound` of [`Cqrs::run`].
pub async fn server(listener: TcpListener, cqrs: Arc<Cqrs>) -> io::Result<()> {
    // axum 0.8: `with_graceful_shutdown` returns an `IntoFuture`, not a `Future`: awaiting it converts.
    ::axum::serve(listener, cqrs.router())
        .with_graceful_shutdown(cqrs.stopping().cancelled_owned())
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        setup::{Setup, SetupError},
        test_support::*,
    };
    use ::axum::{
        extract::{Path, State},
        routing::get,
    };
    use cqrsx::projection::checkpoint::CheckpointStore;
    use std::{net::SocketAddr, time::Duration};
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
    async fn the_axum_server_answers_the_routes_of_its_modules_and_stops_on_the_signal() {
        let mailbox = Arc::new(Mailbox::default());
        let cqrs = Arc::new(
            memory_builder()
                .module(shows)
                .module(wallets)
                .module(front)
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
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let served = tokio::spawn({
            let cqrs = cqrs.clone();
            async move {
                cqrs.run(server(listener, cqrs.clone()), async move {
                    let _ = stopped.await;
                    Ok(())
                })
                .await
            }
        });
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
    async fn a_framework_without_task_types_runs_the_axum_server_until_the_signal() {
        let cqrs = Arc::new(memory_builder().module(front).build().await.unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        cqrs.run(server(listener, cqrs.clone()), async { Ok(()) }).await.unwrap();
    }
}
