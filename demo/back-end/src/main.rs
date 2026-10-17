//! Encore back-end: composition root. Phase 1 serves only `/api/health`.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
mod config;
#[allow(dead_code)] // wired up from Phase 3 on
mod domain;

use axum::{Json, Router, routing::get};
use config::Config;
use cqrsx::logging::LoggingConfig;
use serde_json::json;

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
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            tracing::error!("invalid configuration: {error}");
            std::process::exit(2);
        }
    };
    let router = Router::new().route("/api/health", get(|| async { Json(json!({ "status": "ok" })) }));
    let listener = match tokio::net::TcpListener::bind(config.http_addr).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!("cannot bind {}: {error}", config.http_addr);
            std::process::exit(2);
        }
    };
    tracing::info!("encore-backend listening on http://{}", config.http_addr);
    if let Err(error) = axum::serve(listener, router).await {
        tracing::error!("server stopped: {error}");
    }
}
