//! Encore back-end: the process lifecycle. The service graph is built by `bootstrap::wire`.
#![allow(clippy::result_large_err)]
use cqrsx::{
    host::{lifecycle, signal::shutdown_signal},
    logging::LoggingConfig,
    postgres::{pool::connect_pool, task_runtime::prepare_task_runtime},
};
use encore_backend::{bootstrap, config::Config, presentation::http};
use sqlx::postgres::PgConnectOptions;
use std::{future::IntoFuture, sync::Arc};
use tokio_util::sync::CancellationToken;

fn fail(message: impl std::fmt::Display) -> ! {
    tracing::error!("{message}");
    eprintln!("encore-backend: {message}");
    std::process::exit(2)
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    if let Err(error) = LoggingConfig::from_env()
        .map_err(|e| e.to_string())
        .and_then(|c| c.try_init().map_err(|e| e.to_string()))
    {
        eprintln!("encore-backend: logging setup failed: {error}");
        std::process::exit(2);
    }
    let config = Arc::new(Config::from_env().unwrap_or_else(|e| fail(format!("invalid configuration: {e}"))));

    // PostgreSQL: event store + outbox. Readiness is the job of scripts/wait-db.sh, not ours.
    let options = config
        .database_url
        .parse::<PgConnectOptions>()
        .unwrap_or_else(|e| fail(format!("ENCORE_DATABASE_URL: {e}")))
        .application_name("encore-backend");
    let pool = connect_pool(options)
        .await
        .unwrap_or_else(|e| fail(format!("cannot connect to PostgreSQL: {e}")));

    // MongoDB: short server-selection timeout, so when MongoDB is down a command still answers
    // quickly (200 + `projection_failed` warning) instead of hanging for the driver's default 30 s.
    let mut mongo_options = mongodb::options::ClientOptions::parse(&config.mongodb_uri)
        .await
        .unwrap_or_else(|e| fail(format!("ENCORE_MONGODB_URI: {e}")));
    mongo_options.server_selection_timeout = Some(std::time::Duration::from_secs(3));
    mongo_options.connect_timeout = Some(std::time::Duration::from_secs(3));
    mongo_options.app_name = Some("encore-backend".into());
    let mongo = mongodb::Client::with_options(mongo_options).unwrap_or_else(|e| fail(format!("MongoDB client: {e}")));

    let shutdown = CancellationToken::new();
    let wiring = bootstrap::wire(config.clone(), pool.clone(), &mongo, shutdown.clone()).unwrap_or_else(|e| fail(e));

    let listener = tokio::net::TcpListener::bind(config.http_addr)
        .await
        .unwrap_or_else(|e| fail(format!("cannot bind {}: {e}", config.http_addr)));
    tracing::info!("encore-backend listening on http://{}", config.http_addr);

    // Outbox task runtime: prepared here (no background work yet), started by `lifecycle::run`.
    let runtime = prepare_task_runtime(
        pool.clone(),
        wiring.dispatcher,
        config.task_queue.clone(),
        config.task_runtime.clone(),
    )
    .unwrap_or_else(|e| fail(format!("task runtime setup: {e}")));
    let task_runtime = runtime.run(shutdown.clone());

    // axum 0.8: `with_graceful_shutdown` returns an `IntoFuture`, not a `Future`.
    let inbound = axum::serve(listener, http::router(wiring.state))
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .into_future();
    if let Err(error) = lifecycle::run(inbound, task_runtime, shutdown.clone(), shutdown_signal()).await {
        error.log("encore-backend stopped with an error");
    }
    pool.close().await;
    tracing::info!("encore-backend stopped");
}
