//! Encore sidecar: read-only window into the databases. Phase 1 serves only `/inspect/health`.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]
mod config;

use axum::{Json, Router, routing::get};
use config::Config;
use serde_json::json;

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

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    if let Err(error) = init_logging() {
        eprintln!("encore-sidecar: logging setup failed: {error}");
        std::process::exit(2);
    }
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => {
            tracing::error!("invalid configuration: {error}");
            std::process::exit(2);
        }
    };
    let router = Router::new().route("/inspect/health", get(|| async { Json(json!({ "status": "ok" })) }));
    let listener = match tokio::net::TcpListener::bind(config.http_addr).await {
        Ok(listener) => listener,
        Err(error) => {
            tracing::error!("cannot bind {}: {error}", config.http_addr);
            std::process::exit(2);
        }
    };
    tracing::info!("encore-sidecar listening on http://{}", config.http_addr);
    if let Err(error) = axum::serve(listener, router).await {
        tracing::error!("server stopped: {error}");
    }
}
