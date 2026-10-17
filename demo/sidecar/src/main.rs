//! Encore sidecar: a read-only window into PostgreSQL (events, snapshots, outbox) and MongoDB
//! (read models, mail inbox). Independent of `cqrsx`: it shows the databases "from the outside".
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
mod config;
mod http;
mod mongo;
mod postgres;
mod state;

use chrono::{SecondsFormat, Utc};
use config::Config;
use mongo::{MongoInspector, WHITELIST};
use postgres::PgInspector;
use sqlx::postgres::PgConnectOptions;
use state::{SourceError, State};
use std::{future::IntoFuture, sync::Arc};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

fn fail(message: impl std::fmt::Display) -> ! {
    tracing::error!("{message}");
    eprintln!("encore-sidecar: {message}");
    std::process::exit(2)
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

/// One poll: everything from PostgreSQL in one snapshot, then the whitelisted collections.
/// An unavailable database becomes an entry in `errors`; the process never dies because of it.
async fn poll(postgres: &PgInspector, mongo: &MongoInspector, limits: &postgres::Limits) -> State {
    let mut state = State {
        polled_at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        ..State::default()
    };
    match postgres.snapshot(limits).await {
        Ok(pg) => state = state.with_postgres(pg),
        Err(error) => state.errors.push(SourceError {
            source: "postgres",
            message: error.to_string(),
        }),
    }
    let mut documents = serde_json::Map::new();
    for (database, collection) in WHITELIST {
        match mongo.collection(database, collection, 100).await {
            Ok(view) => {
                documents.insert(collection.to_owned(), serde_json::json!(view.total));
                state.mongo.insert(collection.to_owned(), view);
            }
            Err(error) => {
                state.errors.push(SourceError {
                    source: "mongodb",
                    message: error.to_string(),
                });
                break;
            }
        }
    }
    if let serde_json::Value::Object(stats) = &mut state.stats {
        stats.insert("documents".into(), serde_json::Value::Object(documents));
    } else {
        state.stats = serde_json::json!({ "documents": documents });
    }
    state
}

async fn shutdown_signal() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    if let Err(error) = init_logging() {
        eprintln!("encore-sidecar: logging setup failed: {error}");
        std::process::exit(2);
    }
    let config = Arc::new(Config::from_env().unwrap_or_else(|e| fail(format!("invalid configuration: {e}"))));
    let options = config
        .database_url
        .parse::<PgConnectOptions>()
        .unwrap_or_else(|e| fail(format!("SIDECAR_DATABASE_URL: {e}")))
        .application_name("encore-sidecar");
    let postgres = PgInspector::new(options);
    let mut mongo_options = mongodb::options::ClientOptions::parse(&config.mongodb_uri)
        .await
        .unwrap_or_else(|e| fail(format!("SIDECAR_MONGODB_URI: {e}")));
    mongo_options.server_selection_timeout = Some(std::time::Duration::from_secs(2));
    mongo_options.connect_timeout = Some(std::time::Duration::from_secs(2));
    mongo_options.app_name = Some("encore-sidecar".into());
    let mongo = MongoInspector::new(
        mongodb::Client::with_options(mongo_options).unwrap_or_else(|e| fail(format!("MongoDB client: {e}"))),
        config.read_db.clone(),
        config.mail_db.clone(),
    );

    let shutdown = CancellationToken::new();
    let (sender, receiver) = watch::channel(Arc::new(State::default()));

    // One shared poller for all clients: broadcast only when the content hash changes.
    let poller = {
        let postgres = postgres.clone();
        let mongo = mongo.clone();
        let interval = config.poll_interval;
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let limits = http::default_limits();
            let mut last_hash = String::new();
            loop {
                let state = poll(&postgres, &mongo, &limits).await;
                let hash = state.content_hash();
                if hash != last_hash {
                    last_hash = hash;
                    let _ = sender.send(Arc::new(state));
                }
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = shutdown.cancelled() => break,
                }
            }
        })
    };

    let app = http::router(http::AppState {
        postgres,
        mongo,
        state: receiver,
        shutdown: shutdown.clone(),
    });
    let listener = tokio::net::TcpListener::bind(config.http_addr)
        .await
        .unwrap_or_else(|e| fail(format!("cannot bind {}: {e}", config.http_addr)));
    tracing::info!(
        "encore-sidecar listening on http://{} (poll every {:?})",
        config.http_addr,
        config.poll_interval
    );
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
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
    shutdown.cancel();
    let _ = server.await;
    let _ = poller.await;
    tracing::info!("encore-sidecar stopped");
}
