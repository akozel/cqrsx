//! Router tests without a database: every branch that answers before storage is touched
//! (validation, the box-office lock, the error shape) and the few endpoints that need no storage.
use crate::{
    application::{
        concert::views::concert_hall::{ConcertHallView, ConcertSummary},
        fan::views::fan_wallet::{FanWalletView, TicketEntry, TicketEntryState},
        lock::BoxOfficeLock,
        ports::Stored,
        read_models::ReadModels,
    },
    bootstrap::{self, routes::router},
    domain::{concert::Concert, fan::Fan},
    testing,
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tower::ServiceExt;

struct App {
    router: Router,
    lock: Arc<BoxOfficeLock>,
}

async fn app() -> App {
    let wiring = bootstrap::wire(&testing::process().await).unwrap();
    let lock = wiring.state.rebuild.lock().clone();
    App {
        router: router(wiring.state),
        lock,
    }
}

/// The real graph, with the read side served from memory.
async fn app_reading(read: ReadModels) -> Router {
    let mut wiring = bootstrap::wire(&testing::process().await).unwrap();
    wiring.state.read = Arc::new(read);
    router(wiring.state)
}

fn read_models(halls: Vec<Stored<ConcertHallView>>, wallets: Vec<Stored<FanWalletView>>) -> ReadModels {
    ReadModels {
        halls: testing::read_model::<_, Concert>(halls),
        wallets: testing::read_model::<_, Fan>(wallets),
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
    assert_eq!(body["snapshot_every"], 10);
    assert_eq!(body["venue"]["seats_per_row"], 8);
    assert!(
        body.get("personas").is_none(),
        "the cast is a demo prop, served by the sidecar"
    );
    let (status, body) = send(&app.router, get("/api/projections/status")).await;
    assert_eq!((status, body["state"].as_str()), (StatusCode::OK, Some("idle")));
}

/// Demo operations and the external mail service's data are the sidecar's: the box office has no
/// such routes any more.
#[tokio::test]
async fn demo_only_routes_moved_to_the_sidecar_are_not_served() {
    let app = app().await;
    let (status, _) = send(&app.router, post("/api/projections/wipe")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send(&app.router, get("/api/fans/fox/inbox")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn malformed_ids_and_bodies_are_rejected_with_the_shared_error_shape() {
    let app = app().await;
    for uri in [
        "/api/concerts/Tbilisi%202026",
        "/api/concerts/Tbilisi%202026/timeline",
        "/api/concerts/Tbilisi%202026/state-at/1",
        "/api/fans/Fox!/wallet",
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
    let router = app_reading(read_models(vec![], vec![])).await;
    let (status, body) = send(&router, get("/api/concerts")).await;
    assert_eq!((status, body), (StatusCode::OK, json!([])));
    for uri in ["/api/concerts/c-1", "/api/fans/fox/wallet"] {
        let (status, body) = send(&router, get(uri)).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
        assert_eq!(body["error"]["code"], "projection_missing", "{uri}");
    }
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
    // One `fan_wallets` document per ledger (fan and concert, D-14): two of fox's, one of a fan whose
    // id merely starts with "fox".
    let ledger = |concert: &str, price: i64| FanWalletView {
        fan_id: "fox".into(),
        fan_name: "Fox".into(),
        tickets: BTreeMap::from([(
            format!("b-{concert}"),
            TicketEntry {
                concert_id: concert.into(),
                seat: "B4".into(),
                price,
                state: TicketEntryState::Active,
                refund: None,
                sequence: 1,
                changed_at: "2026-10-18T10:00:00.000000Z".into(),
            },
        )]),
        active_count: 1,
        total_spent: price,
        last_sequence: 1,
    };
    let stored = |id: &str, version: i64, view: FanWalletView| Stored {
        id: id.into(),
        version,
        view,
    };
    let router = app_reading(read_models(
        vec![Stored {
            id: "c-1".into(),
            version: 3,
            view: hall.clone(),
        }],
        vec![
            stored("fox-2:radiohead-2026-11-21", 1, ledger("radiohead-2026-11-21", 50)),
            stored("fox:muse-2027-01-16", 2, ledger("muse-2027-01-16", 120)),
            stored("fox:radiohead-2026-11-21", 1, ledger("radiohead-2026-11-21", 80)),
        ],
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
    let ticket = |concert: &str, price: i64| {
        json!({ "concert_id": concert, "seat": "B4", "price": price, "state": "active", "sequence": 1,
                "changed_at": "2026-10-18T10:00:00.000000Z" })
    };
    assert_eq!(
        body,
        json!({
            "fan_id": "fox",
            "ledgers": [
                { "concert_id": "muse-2027-01-16", "version": 2, "last_sequence": 1 },
                { "concert_id": "radiohead-2026-11-21", "version": 1, "last_sequence": 1 },
            ],
            "view": {
                "fan_id": "fox", "fan_name": "Fox",
                "tickets": {
                    "b-muse-2027-01-16": ticket("muse-2027-01-16", 120),
                    "b-radiohead-2026-11-21": ticket("radiohead-2026-11-21", 80),
                },
                "active_count": 2, "total_spent": 200, "last_sequence": 1,
            },
        })
    );
}
