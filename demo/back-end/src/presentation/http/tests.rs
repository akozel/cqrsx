//! Router tests without a database: every branch that answers before storage is touched
//! (validation, the box-office lock, the error shape) and the few endpoints that need no storage.
use crate::{application::lock::BoxOfficeLock, bootstrap, config::Config, presentation::http::router, testing};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use cqrsx::{postgres::task_queue::PostgresTaskQueueConfig, task::runtime::config::TaskRuntimeConfig};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

fn config() -> Config {
    let outbox = |name: &str| {
        Some(
            match name {
                "OUTBOX_WORKER_CONCURRENCY" => "2",
                "OUTBOX_QUEUE_CAPACITY" => "2",
                "OUTBOX_POLL_INTERVAL_MS" => "300",
                "OUTBOX_HANDLER_TIMEOUT_SECS" => "5",
                "OUTBOX_LEASE_SECS" => "30",
                "OUTBOX_STORAGE_TIMEOUT_MS" => "3000",
                "OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS" => "4000",
                _ => return None,
            }
            .to_owned(),
        )
    };
    Config {
        database_url: String::new(),
        mongodb_uri: String::new(),
        read_db: "encore_read_test".into(),
        mail_db: "mail_service_test".into(),
        http_addr: "127.0.0.1:0".parse().unwrap(),
        task_failure_rate: 0.2,
        task_latency: (Duration::ZERO, Duration::ZERO),
        task_max_attempts: 6,
        chain_max_attempts: 60,
        task_retry_base: Duration::from_secs(1),
        task_retry_max: Duration::from_secs(8),
        chaos_seed: None,
        ticket_limit: 4,
        snapshot_every: 10,
        replay_slow_delay: Duration::ZERO,
        task_runtime: TaskRuntimeConfig::from_lookup(outbox).unwrap(),
        task_queue: PostgresTaskQueueConfig::from_lookup(outbox).unwrap(),
    }
}

struct App {
    router: Router,
    lock: Arc<BoxOfficeLock>,
}

async fn app() -> App {
    let mongo = testing::lazy_mongo().await;
    let wiring = bootstrap::wire(Arc::new(config()), testing::lazy_pool(), &mongo, CancellationToken::new()).unwrap();
    let lock = wiring.state.rebuild.lock().clone();
    App {
        router: router(wiring.state),
        lock,
    }
}

async fn send(router: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, body)
}

fn get(uri: &str) -> Request<Body> {
    Request::get(uri).body(Body::empty()).unwrap()
}

/// A `POST` without a body (and without a content type): what the UI buttons send.
fn post(uri: &str) -> Request<Body> {
    Request::post(uri).body(Body::empty()).unwrap()
}

fn post_json(uri: &str, body: &str) -> Request<Body> {
    Request::post(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_owned()))
        .unwrap()
}

#[tokio::test]
async fn health_and_config_need_no_storage() {
    let app = app().await;
    let (status, body) = send(&app.router, get("/api/health")).await;
    assert_eq!((status, body["status"].as_str()), (StatusCode::OK, Some("ok")));
    let (status, body) = send(&app.router, get("/api/config")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ticket_limit"], 4);
    assert_eq!(body["venue"]["seats_per_row"], 8);
    assert_eq!(body["personas"][0]["fan_id"], "fox");
    let (status, body) = send(&app.router, get("/api/projections/status")).await;
    assert_eq!((status, body["state"].as_str()), (StatusCode::OK, Some("idle")));
}

#[tokio::test]
async fn malformed_ids_and_bodies_are_rejected_with_the_shared_error_shape() {
    let app = app().await;
    for uri in [
        "/api/concerts/Tbilisi%202026",
        "/api/concerts/Tbilisi%202026/timeline",
        "/api/concerts/Tbilisi%202026/state-at/1",
        "/api/fans/Fox!/wallet",
        "/api/fans/Fox!/inbox",
    ] {
        let (status, body) = send(&app.router, get(uri)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(body["outcome"], "invalid", "{uri}");
        assert_eq!(body["error"]["code"], "invalid_request", "{uri}");
        assert!(body["error"]["message"].as_str().unwrap().starts_with("invalid "), "{uri}");
    }
    let (status, body) = send(&app.router, post_json("/api/concerts/c-1/commands", "{not json")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]["message"].as_str().unwrap().starts_with("invalid JSON body"));
    let (status, body) = send(
        &app.router,
        post_json("/api/concerts/c-1/commands", r#"{ "type": "RecordTicket" }"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]["message"].as_str().unwrap().starts_with("invalid command"));
    let (status, body) = send(
        &app.router,
        post_json(
            "/api/concerts/c-1/commands",
            r#"{ "type": "BookSeat", "seat": "B4", "fan_id": "Fox!", "fan_name": "" }"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"]["message"].as_str().unwrap().contains("invalid fan id"));
    let (status, body) = send(&app.router, post_json("/api/projections/rebuild", r#"{ "speed": "warp" }"#)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("invalid rebuild request")
    );
}

#[tokio::test]
async fn a_closed_box_office_answers_423_before_anything_else() {
    let app = app().await;
    let _guard = app.lock.try_acquire().unwrap();
    // Even an invalid command body is told the box office is closed (§6.4 step 1).
    let (status, body) = send(&app.router, post_json("/api/concerts/c-1/commands", "{not json")).await;
    assert_eq!(status, StatusCode::LOCKED);
    assert_eq!(body["outcome"], "locked");
    assert_eq!(body["error"]["code"], "rebuild_in_progress");
    assert_eq!(body["concert_id"], "c-1");
    let (status, body) = send(&app.router, post("/api/projections/wipe")).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::LOCKED, Some("rebuild_in_progress"))
    );
    let (status, body) = send(&app.router, get("/api/projections/verify")).await;
    assert_eq!(
        (status, body["error"]["code"].as_str()),
        (StatusCode::LOCKED, Some("rebuild_in_progress"))
    );
    let (status, body) = send(&app.router, post("/api/projections/rebuild")).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["outcome"], "conflict");
    assert_eq!(body["error"]["code"], "rebuild_in_progress");
}

#[tokio::test]
async fn an_unreachable_store_is_reported_as_unavailable() {
    let app = app().await;
    let (status, body) = send(&app.router, get("/api/concerts/c-1/timeline")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["outcome"], "unavailable");
    let (status, body) = send(&app.router, get("/api/concerts")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("read model storage unavailable")
    );
    let (status, body) = send(
        &app.router,
        post_json(
            "/api/concerts/c-1/commands",
            &json!({ "type": "CancelBooking", "seat": "B4", "fan_id": "fox" }).to_string(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["concert_id"], "c-1");
    assert!(body["command_id"].is_string());
}
