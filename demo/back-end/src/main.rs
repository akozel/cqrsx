//! Encore back-end: composition root (see PLAN.md §6.3).
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
// Items for the outbox chain and the read side land in Phases 4–6; until then they are unused.
#![allow(dead_code)]
// `ApiError` is an HTTP response payload built only on error paths; its size is irrelevant.
#![allow(clippy::result_large_err)]
mod app;
mod config;
mod domain;
mod http;

use app::{
    activity::ActivityHub,
    command_service::{ConcertCommands, Executors, FanCommands},
    rebuild::RebuildService,
};
use config::Config;
use cqrsx::{
    cqrs::command::retry::AggregateConflictRetryPolicy,
    host::{lifecycle, signal::shutdown_signal},
    logging::LoggingConfig,
    postgres::{command::PostgresCommandConfig, pool::connect_pool},
};
use domain::concert::ConcertServices;
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

    let activity = Arc::new(ActivityHub::new(1024, 100));
    let rebuild = Arc::new(RebuildService::new());
    let command_config = PostgresCommandConfig::new(config.snapshot_every, AggregateConflictRetryPolicy::default())
        .unwrap_or_else(|e| fail(format!("snapshot configuration: {e}")));
    let executors = Executors {
        pool: pool.clone(),
        snapshot_every: command_config.snapshot_interval(),
        retry: command_config.conflict_retry(),
    };
    // Event handlers (projections) are registered in Phase 5; until then every framework has none.
    let concert_commands = Arc::new(ConcertCommands::new(
        &executors,
        Arc::new(Vec::new),
        vec![],
        ConcertServices {
            ticket_limit: config.ticket_limit,
        },
        activity.clone(),
        rebuild.clone(),
        config.task_max_attempts,
    ));
    let fan_commands = Arc::new(FanCommands::new(&executors, Arc::new(Vec::new), vec![], activity.clone()));

    if config.seed {
        app::seed::run(&concert_commands).await;
    }

    let shutdown = CancellationToken::new();
    let state = http::AppState {
        config: config.clone(),
        concert_commands,
        fan_commands,
        activity,
        rebuild,
        shutdown: shutdown.clone(),
    };
    let listener = tokio::net::TcpListener::bind(config.http_addr)
        .await
        .unwrap_or_else(|e| fail(format!("cannot bind {}: {e}", config.http_addr)));
    tracing::info!("encore-backend listening on http://{}", config.http_addr);

    // Phase 4 replaces this placeholder with the outbox task runtime.
    let task_runtime = {
        let token = shutdown.clone();
        async move {
            token.cancelled().await;
            Ok(())
        }
    };
    // axum 0.8: `with_graceful_shutdown` returns an `IntoFuture`, not a `Future`.
    let inbound = axum::serve(listener, http::router(state))
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .into_future();
    if let Err(error) = lifecycle::run(inbound, task_runtime, shutdown.clone(), shutdown_signal()).await {
        error.log("encore-backend stopped with an error");
    }
    pool.close().await;
    tracing::info!("encore-backend stopped");
}
