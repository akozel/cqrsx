//! Router tests without a database: every branch that answers before storage is touched
//! (validation, the box-office lock, the error shape) and the few endpoints that need no storage.
use crate::{
    application::{
        concert::views::concert_hall::{ConcertHallView, ConcertSummary},
        fan::views::fan_wallet::FanWalletView,
        lock::BoxOfficeLock,
        ports::{DeliveredMail, Stored},
        read_models::ReadModels,
    },
    bootstrap,
    domain::{concert::Concert, fan::Fan},
    presentation::http::router,
    testing::{self, MemoryInbox},
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

struct App {
    router: Router,
    lock: Arc<BoxOfficeLock>,
}

async fn app() -> App {
    let mongo = testing::lazy_mongo().await;
    let wiring = bootstrap::wire(
        Arc::new(testing::config()),
        testing::lazy_pool(),
        &mongo,
        CancellationToken::new(),
    )
    .unwrap();
    let lock = wiring.state.rebuild.lock().clone();
    App {
        router: router(wiring.state),
        lock,
    }
}

/// The real graph, with the read side served from memory.
async fn app_reading(read: ReadModels) -> Router {
    let mongo = testing::lazy_mongo().await;
    let mut wiring = bootstrap::wire(
        Arc::new(testing::config()),
        testing::lazy_pool(),
        &mongo,
        CancellationToken::new(),
    )
    .unwrap();
    wiring.state.read = Arc::new(read);
    router(wiring.state)
}

fn read_models(
    halls: Vec<Stored<ConcertHallView>>,
    wallets: Vec<Stored<FanWalletView>>,
    mails: Vec<DeliveredMail>,
) -> ReadModels {
    ReadModels {
        halls: testing::read_model::<_, Concert>(halls),
        wallets: testing::read_model::<_, Fan>(wallets),
        inbox: Arc::new(MemoryInbox(mails)),
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

#[tokio::test]
async fn empty_read_models_answer_an_empty_list_and_404s() {
    let router = app_reading(read_models(vec![], vec![], vec![])).await;
    let (status, body) = send(&router, get("/api/concerts")).await;
    assert_eq!((status, body), (StatusCode::OK, json!([])));
    for uri in ["/api/concerts/c-1", "/api/fans/fox/wallet"] {
        let (status, body) = send(&router, get(uri)).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
        assert_eq!(body["error"]["code"], "projection_missing", "{uri}");
    }
    let (status, body) = send(&router, get("/api/fans/fox/inbox")).await;
    assert_eq!((status, body), (StatusCode::OK, json!([])));
}

#[tokio::test]
async fn read_models_are_served_in_the_documented_shapes() {
    let hall = ConcertHallView {
        concert: Some(ConcertSummary {
            artist: "Radiohead".into(),
            city: "Tbilisi".into(),
            date: "2026-11-21".into(),
        }),
        capacity: 40,
        sold: 1,
        revenue: 80,
        last_sequence: 2,
        ..ConcertHallView::default()
    };
    let wallet = FanWalletView {
        fan_id: "fox".into(),
        fan_name: "Fox".into(),
        active_count: 1,
        total_spent: 80,
        last_sequence: 1,
        ..FanWalletView::default()
    };
    let mail = DeliveredMail {
        inbox_id: "e_ticket:b-1".into(),
        kind: "e_ticket".into(),
        to_fan_id: "fox".into(),
        to_name: "Fox".into(),
        subject: "🎫".into(),
        concert_id: "c-1".into(),
        seat: "B4".into(),
        amount: 80,
        task_id: "t-1".into(),
        delivered_on_attempt: 2,
        deliveries: 1,
        first_delivered_at: "2026-10-17T09:00:00.000Z".into(),
    };
    let router = app_reading(read_models(
        vec![Stored {
            id: "c-1".into(),
            version: 3,
            view: hall.clone(),
        }],
        vec![Stored {
            id: "fox".into(),
            version: 1,
            view: wallet.clone(),
        }],
        vec![mail.clone()],
    ))
    .await;
    let (_, body) = send(&router, get("/api/concerts")).await;
    assert_eq!(
        body,
        json!([{ "concert_id": "c-1", "artist": "Radiohead", "city": "Tbilisi", "date": "2026-11-21", "sold": 1,
                 "capacity": 40, "revenue": 80, "sold_out": false, "incomplete": false, "last_sequence": 2, "version": 3 }])
    );
    let (_, body) = send(&router, get("/api/concerts/c-1")).await;
    assert_eq!(body, json!({ "concert_id": "c-1", "version": 3, "view": hall }));
    let (_, body) = send(&router, get("/api/fans/fox/wallet")).await;
    assert_eq!(body, json!({ "fan_id": "fox", "version": 1, "view": wallet }));
    let (_, body) = send(&router, get("/api/fans/fox/inbox")).await;
    assert_eq!(body, json!([mail]));
    assert_eq!(body[0]["inbox_id"], "e_ticket:b-1");
    let (_, body) = send(&router, get("/api/fans/panda/inbox")).await;
    assert_eq!(body, json!([]));
}
