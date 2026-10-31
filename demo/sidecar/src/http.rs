//! HTTP API of the sidecar under `/inspect`: tables, collections, stats, the SSE stream of the
//! shared poller, the fan's mailbox, and the demo lifecycle (`GET /inspect/demo`,
//! `POST /inspect/projections/wipe`, `POST /inspect/reset`). The mail service's own API (`POST
//! /mail`) is a separate router, see `mail`.
use crate::{
    demo::{DEFAULT_CONCERTS, DemoAdmin, PERSONAS},
    mongo::{MongoInspector, allowed},
    postgres::{Limits, PgInspector},
    state::State,
};
use axum::{
    Json, Router,
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use futures::{StreamExt, stream};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
pub struct AppState {
    pub postgres: PgInspector,
    pub mongo: MongoInspector,
    pub state: watch::Receiver<Arc<State>>,
    pub shutdown: CancellationToken,
    /// DEMO ONLY: the demo lifecycle (seed, wipe, reset).
    pub demo: DemoAdmin,
    /// DEMO ONLY: `false` hides `POST /inspect/reset` (`SIDECAR_RESET_ENABLED=false`); a wipe is
    /// always allowed, it destroys nothing the ledger cannot rebuild.
    pub reset_enabled: bool,
}

fn error(status: StatusCode, source: &str, message: impl std::fmt::Display) -> Response {
    (
        status,
        Json(json!({ "error": { "source": source, "message": message.to_string() } })),
    )
        .into_response()
}

#[derive(Deserialize)]
pub struct TableQuery {
    aggregate_id: Option<String>,
    limit: Option<i64>,
}

fn limit(requested: Option<i64>, default: i64, max: i64) -> i64 {
    requested.unwrap_or(default).clamp(1, max)
}

async fn health(AxumState(state): AxumState<AppState>) -> Json<Value> {
    let postgres = state.postgres.ping().await.map_err(|e| e.to_string());
    let mongo = state.mongo.ping().await.map_err(|e| e.to_string());
    Json(json!({
        "status": if postgres.is_ok() && mongo.is_ok() { "ok" } else { "degraded" },
        "postgres": postgres.as_ref().map(|_| "ok").unwrap_or_else(|e| e.as_str()),
        "mongodb": mongo.as_ref().map(|_| "ok").unwrap_or_else(|e| e.as_str()),
    }))
}

async fn events(AxumState(state): AxumState<AppState>, Query(q): Query<TableQuery>) -> Response {
    match state
        .postgres
        .events(q.aggregate_id.as_deref(), limit(q.limit, 200, 1000))
        .await
    {
        Ok(table) => Json(table).into_response(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, "postgres", e),
    }
}

async fn snapshots(AxumState(state): AxumState<AppState>, Query(q): Query<TableQuery>) -> Response {
    match state
        .postgres
        .snapshots(q.aggregate_id.as_deref(), limit(q.limit, 50, 500))
        .await
    {
        Ok(table) => Json(table).into_response(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, "postgres", e),
    }
}

async fn outbox(AxumState(state): AxumState<AppState>, Query(q): Query<TableQuery>) -> Response {
    match state.postgres.outbox(limit(q.limit, 200, 1000)).await {
        Ok(table) => Json(table).into_response(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, "postgres", e),
    }
}

async fn tables(AxumState(state): AxumState<AppState>) -> Response {
    match state.postgres.tables().await {
        Ok(json) => Json(json).into_response(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, "postgres", e),
    }
}

async fn collection(
    AxumState(state): AxumState<AppState>,
    Path((database, collection)): Path<(String, String)>,
    Query(q): Query<TableQuery>,
) -> Response {
    if !allowed(&database, &collection) {
        return error(
            StatusCode::NOT_FOUND,
            "mongodb",
            format!("{database}.{collection} is not inspectable"),
        );
    }
    match state
        .mongo
        .collection(&database, &collection, limit(q.limit, 100, 1000))
        .await
    {
        Ok(view) => Json(view).into_response(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, "mongodb", e),
    }
}

/// `GET /inspect/mail/{fan_id}`: the fan's e-mails, oldest first, as the mail service stored them.
async fn inbox(AxumState(state): AxumState<AppState>, Path(fan_id): Path<String>, Query(q): Query<TableQuery>) -> Response {
    match state.mongo.inbox_of(&fan_id, limit(q.limit, 100, 1000)).await {
        Ok(view) => Json(view).into_response(),
        Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, "mongodb", e),
    }
}

async fn stats(AxumState(state): AxumState<AppState>) -> Json<Value> {
    let current = state.state.borrow().clone();
    Json(json!({ "stats": current.stats, "db_time": current.db_time, "polled_at": current.polled_at, "errors": current.errors }))
}

/// `GET /inspect/stream`: the current state at once, then every change; comments keep the
/// connection alive; the stream ends on shutdown.
async fn stream(AxumState(state): AxumState<AppState>) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let mut receiver = state.state.clone();
    receiver.mark_changed();
    let changes = stream::unfold(receiver, |mut receiver| async move {
        receiver.changed().await.ok()?;
        let current = receiver.borrow_and_update().clone();
        let data = serde_json::to_string(&*current).unwrap_or_else(|_| "{}".to_owned());
        Some((Ok(Event::default().event("state").data(data)), receiver))
    });
    Sse::new(changes.take_until(state.shutdown.clone().cancelled_owned()))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)).text("heartbeat"))
}

/// `GET /inspect/demo`: the props of the demo — the default concerts, the cast — and whether a
/// reset is allowed.
async fn demo(AxumState(state): AxumState<AppState>) -> Json<Value> {
    Json(json!({
        "default_concerts": DEFAULT_CONCERTS,
        "personas": PERSONAS,
        "reset_enabled": state.reset_enabled,
    }))
}

/// `POST /inspect/projections/wipe`: 🧨 DEMO ONLY — drop the read models and their checkpoints; the ledger
/// is intact and the back-end's projection processors rebuild every document by themselves. Shows that the
/// rules keep working from the ledger alone.
async fn wipe(AxumState(state): AxumState<AppState>) -> Response {
    match state.demo.wipe_read_models().await {
        Ok(dropped) => {
            tracing::info!("read models wiped: {}", dropped.join(", "));
            Json(json!({ "outcome": "wiped", "dropped": dropped })).into_response()
        }
        Err(e) => {
            tracing::warn!("wipe failed: {e}");
            error(StatusCode::SERVICE_UNAVAILABLE, "demo", e)
        }
    }
}

/// `POST /inspect/reset`: DEMO ONLY — empty everything and announce the default concerts again.
async fn reset(AxumState(state): AxumState<AppState>) -> Response {
    if !state.reset_enabled {
        return error(
            StatusCode::NOT_FOUND,
            "demo",
            "reset is disabled (SIDECAR_RESET_ENABLED=false)",
        );
    }
    match state.demo.reset().await {
        Ok(report) => {
            tracing::info!("demo reset: {} announced", report.announced.len());
            Json(json!({ "outcome": "reset", "report": report })).into_response()
        }
        Err(e) => {
            tracing::warn!("demo reset failed: {e}");
            error(StatusCode::SERVICE_UNAVAILABLE, "demo", e)
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/inspect/health", get(health))
        .route("/inspect/postgres/events", get(events))
        .route("/inspect/postgres/snapshots", get(snapshots))
        .route("/inspect/postgres/outbox", get(outbox))
        .route("/inspect/postgres/tables", get(tables))
        .route("/inspect/mongo/{database}/{collection}", get(collection))
        .route("/inspect/mail/{fan_id}", get(inbox))
        .route("/inspect/stats", get(stats))
        .route("/inspect/stream", get(stream))
        .route("/inspect/demo", get(demo))
        .route("/inspect/projections/wipe", post(wipe))
        .route("/inspect/reset", post(reset))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

pub fn default_limits() -> Limits {
    Limits::default()
}
