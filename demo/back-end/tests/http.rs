//! The HTTP API of the box office on the engine (moved from `bootstrap/routes/tests.rs` with the routes): every branch
//! that answers before storage is touched (validation, the error shape), the endpoints that need no storage, and the
//! read side, on the engine in memory or over databases nobody listens on.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use chrono::NaiveDate;
use cqrsx::{
    projection::checkpoint::{CheckpointKey, CheckpointStore},
    view::repository::StoredView,
};
use cqrsx_engine::{
    Cqrs,
    build::Builder,
    memory::InMemory,
    mongodb::Mongo,
    postgres::{Postgres, PostgresPools},
};
use encore_backend::{
    DEMO_DEFAULTS, concert,
    concert::{
        application::views::concert_hall::{ConcertHallView, ConcertSummary},
        domain::{
            Concert, ConcertCommand, ConcertEvent,
            commands::{AnnounceConcert, BookSeat},
        },
    },
    fan,
    fan::{
        application::views::fan_wallet::{FanWalletView, TicketEntry, TicketEntryState},
        domain::Fan,
    },
    operations,
    shared::domain::{ConcertId, FanId},
};
use http_body_util::BodyExt;
use mongodb::{Client, options::ClientOptions};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tower::ServiceExt;
use uuid::Uuid;

/// The modules of the process over the variables of the router tests: a mail service nobody listens on, Encore! at
/// full speed only (no slow motion).
fn modules() -> Builder {
    Cqrs::from_lookup("ENCORE", |name| match name {
        "ENCORE_MAIL_SERVICE_URL" => Some("http://127.0.0.1:1".to_owned()),
        "ENCORE_REBUILD_SLOW_PACE_MS" => Some("0".to_owned()),
        "ENCORE_MONGODB_DATABASE" => Some("encore_read_test".to_owned()),
        _ => None,
    })
    .defaults(DEMO_DEFAULTS)
    .module(concert::module)
    .module(fan::module)
    .module(operations::module)
}

/// The modules of the process over stores in memory.
fn memory_app() -> Builder {
    modules().event_store(InMemory).projections(InMemory)
}

/// Nothing listens on port 1: the first acquire fails fast instead of retrying for the default 30 s.
fn lazy_pool() -> PgPool {
    PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(200))
        .connect_lazy_with("postgresql://encore:encore@127.0.0.1:1/encore".parse().unwrap())
}

async fn lazy_mongo() -> Client {
    let mut options = ClientOptions::parse("mongodb://127.0.0.1:1").await.unwrap();
    options.server_selection_timeout = Some(Duration::from_millis(50));
    Client::with_options(options).unwrap()
}

struct App {
    router: Router,
}

/// The process over databases nobody listens on: enough to exercise every branch that answers before storage is
/// touched, and every answer of a store that does not answer.
async fn app() -> App {
    let pools = PostgresPools {
        main: lazy_pool(),
        history: lazy_pool(),
        projection: lazy_pool(),
    };
    let cqrs = modules()
        .event_store(Postgres)
        .postgres_pools(pools)
        .projections(Mongo)
        .mongodb_client(lazy_mongo().await)
        .build()
        .await
        .unwrap();
    App {
        router: Arc::new(cqrs).router(),
    }
}

/// The process in memory, whose processors do not run: the rows of both read models registered and claimed by `owner`,
/// three events of the concert halls' journal unapplied.
async fn app_with_checkpoints(owner: &str) -> (Router, Arc<Cqrs>) {
    let cqrs = memory_app().build().await.unwrap();
    for key in [
        CheckpointKey::of::<ConcertHallView, Concert>(),
        CheckpointKey::of::<FanWalletView, Fan>(),
    ] {
        cqrs.checkpoints().unwrap().register(&key).await.unwrap();
        cqrs.checkpoints()
            .unwrap()
            .claim(&key, owner, Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
    }
    let concert_id = ConcertId::parse("c-1").unwrap();
    let announce = AnnounceConcert {
        artist: "A".into(),
        city: "C".into(),
        date: NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
    };
    let ui = cqrs.source("ui");
    ui.execute::<Concert>(&concert_id, ConcertCommand::AnnounceConcert(announce))
        .await
        .unwrap();
    for (seat, fan) in [("A1", "fox"), ("A2", "panda")] {
        let book = BookSeat {
            seat: seat.into(),
            fan_id: FanId::parse(fan).unwrap(),
            fan_name: fan.into(),
            booking_id: Uuid::now_v7(),
        };
        ui.execute::<Concert>(&concert_id, ConcertCommand::BookSeat(book))
            .await
            .unwrap();
    }
    let cqrs = Arc::new(cqrs);
    (cqrs.router(), cqrs)
}

/// The process in memory, the documents of both read models written by hand.
async fn app_reading(halls: Vec<StoredView<ConcertHallView>>, wallets: Vec<StoredView<FanWalletView>>) -> Router {
    let cqrs = memory_app().build().await.unwrap();
    let hall_documents = cqrs.documents::<ConcertHallView, Concert>().unwrap();
    for document in halls {
        hall_documents
            .put(&document.id, document.revision, document.last_sequence, &document.view)
            .unwrap();
    }
    let wallet_documents = cqrs.documents::<FanWalletView, Fan>().unwrap();
    for document in wallets {
        wallet_documents
            .put(&document.id, document.revision, document.last_sequence, &document.view)
            .unwrap();
    }
    Arc::new(cqrs).router()
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
    assert_eq!(
        body["rebuild_slow_pace_ms"], 0,
        "the slow-motion pause the UI asks Encore! for"
    );
    // MongoDB does not answer: the status says so instead of failing, and nothing is registered yet.
    let (status, body) = send(&app.router, get("/api/projections/status")).await;
    assert_eq!(
        (status, body["state"].as_str(), body["from_cache"].as_bool()),
        (StatusCode::OK, Some("starting"), Some(true))
    );
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
    for body in [r#"{ "speed": "slow" }"#, r#"{ "pace_ms": "slow" }"#, r#"{ "pace_ms": -1 }"#] {
        let (status, answer) = send(&app.router, post_json("/api/projections/rebuild", body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(
            answer["error"]["message"]
                .as_str()
                .unwrap()
                .starts_with("invalid rebuild request"),
            "{body}"
        );
    }
    let (status, _) = send(&app.router, post("/api/concerts/Tbilisi%202026/rebuild")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// The aggregate id of a command is typed: a malformed one is refused at the path, so it never reaches the executor,
/// the journal or a planner.
#[tokio::test]
async fn a_command_for_a_malformed_concert_id_is_refused_before_it_runs() {
    let app = app().await;
    let (status, body) = send(
        &app.router,
        post_json(
            "/api/concerts/Tbilisi%202026/commands",
            r#"{ "type": "AnnounceConcert", "artist": "Radiohead", "city": "Tbilisi", "date": "2026-11-21" }"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        (&body["outcome"], &body["error"]["code"]),
        (&json!("invalid"), &json!("invalid_request"))
    );
    assert!(
        body["error"]["message"].as_str().unwrap().starts_with("invalid concert id"),
        "{body}"
    );
}

#[tokio::test]
async fn the_read_side_asks_the_owners_and_reports_their_checkpoints() {
    let (router, cqrs) = app_with_checkpoints("node-a").await;
    let (status, body) = send(&router, get("/api/projections/status")).await;
    assert_eq!(
        (status, body["state"].as_str(), body["from_cache"].as_bool()),
        (StatusCode::OK, Some("live"), Some(false))
    );
    let names: Vec<&str> = body["projections"]
        .as_array()
        .unwrap()
        .iter()
        .map(|projection| projection["projection"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["concert_halls", "fan_wallets"]);
    assert_eq!(
        (
            body["projections"][0]["lag"].as_u64(),
            body["projections"][0]["owner"].as_str()
        ),
        (Some(3), Some("node-a"))
    );
    // Encore! at full speed (no body), refused while it runs and with a pace beyond the limit.
    let (status, body) = send(&router, post("/api/projections/rebuild")).await;
    assert_eq!(
        (status, body["outcome"].as_str(), body["pace_ms"].as_u64()),
        (StatusCode::ACCEPTED, Some("started"), Some(0))
    );
    assert!(body["rebuild_id"].is_string());
    let (status, body) = send(&router, post_json("/api/projections/rebuild", r#"{ "pace_ms": 80 }"#)).await;
    assert_eq!(
        (status, body["outcome"].as_str(), body["error"]["code"].as_str()),
        (StatusCode::CONFLICT, Some("conflict"), Some("rebuild_in_progress"))
    );
    let (status, body) = send(&router, post_json("/api/projections/rebuild", r#"{ "pace_ms": 5000 }"#)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["message"], "the pace must be at most 1000 ms");
    let (status, body) = send(&router, get("/api/projections/status")).await;
    assert_eq!((status, body["state"].as_str()), (StatusCode::OK, Some("rebuilding")));
    // One hall.
    let (status, body) = send(&router, post("/api/concerts/c-1/rebuild")).await;
    assert_eq!(
        (status, body),
        (StatusCode::ACCEPTED, json!({ "outcome": "requested", "concert_id": "c-1" }))
    );
    cqrs.checkpoints().unwrap().set_unavailable(true);
    let (status, body) = send(&router, post("/api/concerts/c-1/rebuild")).await;
    assert_eq!(
        (status, body["outcome"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("unavailable"))
    );
}

#[tokio::test]
async fn a_booking_and_a_cancellation_over_http_are_identified_by_the_command_id_of_the_answer() {
    let cqrs = Arc::new(memory_app().build().await.unwrap());
    let router = cqrs.router();
    let command = |body: Value| post_json("/api/concerts/c-1/commands", &body.to_string());
    let (status, _) = send(
        &router,
        command(json!({ "type": "AnnounceConcert", "artist": "A", "city": "C", "date": "2026-11-21" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, booked) = send(
        &router,
        command(json!({ "type": "BookSeat", "seat": "A1", "fan_id": "fox", "fan_name": "Fox" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, cancelled) = send(
        &router,
        command(json!({ "type": "CancelBooking", "seat": "A1", "fan_id": "fox" })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let events = cqrs.history::<Concert>(&ConcertId::parse("c-1").unwrap()).await.unwrap();
    let (ConcertEvent::SeatBooked(booking), ConcertEvent::BookingCancelled(cancellation)) =
        (&events[1].payload, &events[2].payload)
    else {
        panic!("a booking and a cancellation follow the announcement: {events:?}");
    };
    assert_eq!(json!(booking.booking_id), booked["command_id"]);
    assert_eq!(events[1].metadata["command_id"], booked["command_id"]);
    assert_eq!(json!(cancellation.cancellation_id), cancelled["command_id"]);
    assert_eq!(events[2].metadata["command_id"], cancelled["command_id"]);
    assert_eq!(json!(cancellation.booking_id), booked["command_id"], "the cancelled booking");
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
    for request in [
        get("/api/projections/verify"),
        post("/api/projections/rebuild"),
        post("/api/concerts/c-1/rebuild"),
    ] {
        let uri = request.uri().to_string();
        let (status, body) = send(&app.router, request).await;
        assert_eq!(
            (status, body["outcome"].as_str()),
            (StatusCode::SERVICE_UNAVAILABLE, Some("unavailable")),
            "{uri}"
        );
    }
}

#[tokio::test]
async fn empty_read_models_answer_an_empty_list_and_404s() {
    let router = app_reading(vec![], vec![]).await;
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
    let stored = |id: &str, revision: i64, view: FanWalletView| StoredView {
        id: id.into(),
        revision,
        last_sequence: 1,
        view,
    };
    let router = app_reading(
        vec![StoredView {
            id: "c-1".into(),
            revision: 3,
            last_sequence: 2,
            view: hall.clone(),
        }],
        vec![
            stored("fox-2:radiohead-2026-11-21", 1, ledger("radiohead-2026-11-21", 50)),
            stored("fox:muse-2027-01-16", 2, ledger("muse-2027-01-16", 120)),
            stored("fox:radiohead-2026-11-21", 1, ledger("radiohead-2026-11-21", 80)),
        ],
    )
    .await;
    let (_, body) = send(&router, get("/api/concerts")).await;
    assert_eq!(
        body,
        json!([{ "concert_id": "c-1", "artist": "Radiohead", "city": "Tbilisi", "date": "2026-11-21", "sold": 1,
                 "capacity": 40, "revenue": 80, "sold_out": false, "last_sequence": 2, "revision": 3 }])
    );
    let (_, body) = send(&router, get("/api/concerts/c-1")).await;
    assert_eq!(body, json!({ "concert_id": "c-1", "revision": 3, "view": hall }));
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
                { "concert_id": "muse-2027-01-16", "revision": 2, "last_sequence": 1 },
                { "concert_id": "radiohead-2026-11-21", "revision": 1, "last_sequence": 1 },
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

/// A JSON document with every leaf replaced by the name of its type (`"<string>"`, `"<number>"`, `"<bool>"`, `null`):
/// the keys and the kinds of the values of an answer, whatever its volatile values (ids, instants) are.
fn skeleton(value: &Value) -> Value {
    match value {
        Value::Null => Value::Null,
        Value::Bool(_) => json!("<bool>"),
        Value::Number(_) => json!("<number>"),
        Value::String(_) => json!("<string>"),
        Value::Array(items) => Value::Array(items.iter().map(skeleton).collect()),
        Value::Object(fields) => Value::Object(fields.iter().map(|(key, value)| (key.clone(), skeleton(value))).collect()),
    }
}

/// The first message of an SSE answer, as `(event, data)`.
async fn first_event(router: &Router, uri: &str) -> (String, Value) {
    let response = router.clone().oneshot(get(uri)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
    let text = String::from_utf8(frame.to_vec()).unwrap();
    let field = |name: &str| {
        text.lines()
            .find_map(|line| line.strip_prefix(name).and_then(|rest| rest.strip_prefix(':')))
            .unwrap_or_else(|| panic!("no `{name}` in {text:?}"))
            .trim()
            .to_owned()
    };
    (field("event"), serde_json::from_str(&field("data")).unwrap())
}

/// The ticket-limit, venue and pace settings the UI shows, whole.
fn config_document() -> Value {
    json!({
        "venue": {
            "rows": ["A", "B", "C", "D", "E"],
            "seats_per_row": 8,
            "zones": [
                { "id": "vip", "rows": ["A"], "price": 120 },
                { "id": "standard", "rows": ["B", "C"], "price": 80 },
                { "id": "balcony", "rows": ["D", "E"], "price": 50 },
            ],
        },
        "ticket_limit": 4,
        "snapshot_every": 10,
        "rebuild_slow_pace_ms": 0,
    })
}

#[tokio::test]
async fn health_and_config_answer_in_their_documented_shapes() {
    let app = app().await;
    let (status, body) = send(&app.router, get("/api/health")).await;
    assert_eq!((status, body), (StatusCode::OK, json!({ "status": "ok" })));
    let (status, body) = send(&app.router, get("/api/config")).await;
    assert_eq!((status, body), (StatusCode::OK, config_document()));
}

#[tokio::test]
async fn the_activity_feed_opens_with_hello_carrying_the_config_and_the_backlog() {
    let app = app().await;
    let (event, data) = first_event(&app.router, "/api/activity").await;
    assert_eq!(event, "hello");
    assert_eq!(data["config"], config_document());
    assert!(data["backlog"].is_array(), "{data}");
    assert_eq!(data.as_object().unwrap().len(), 2, "{data}");
}

#[tokio::test]
async fn the_time_machine_answers_in_the_documented_shapes() {
    let (router, _) = app_with_checkpoints("node-a").await;
    let (status, body) = send(&router, get("/api/concerts/c-1/timeline")).await;
    assert_eq!(status, StatusCode::OK);
    let rows = body.as_array().unwrap();
    assert_eq!(rows.len(), 3);
    let event_type = |row: &Value| row["event_type"].clone();
    assert_eq!(
        rows.iter()
            .map(|row| (row["sequence"].clone(), event_type(row), row["summary"].clone()))
            .collect::<Vec<_>>(),
        [
            (json!(1), json!("ConcertAnnounced"), json!("A · C")),
            (json!(2), json!("SeatBooked"), json!("A1 → fox")),
            (json!(3), json!("SeatBooked"), json!("A2 → panda")),
        ]
    );
    for row in rows {
        assert_eq!(
            skeleton(row),
            json!({ "sequence": "<number>", "event_type": "<string>", "recorded_at": "<string>", "summary": "<string>" })
        );
        let mut keys: Vec<&String> = row.as_object().unwrap().keys().collect();
        keys.sort();
        assert_eq!(keys, ["event_type", "recorded_at", "sequence", "summary"]);
    }

    let (status, body) = send(&router, get("/api/concerts/c-1/state-at/2")).await;
    assert_eq!(status, StatusCode::OK);
    let mut keys: Vec<&String> = body.as_object().unwrap().keys().collect();
    keys.sort();
    assert_eq!(keys, ["concert_id", "event", "last_sequence", "sequence", "state"]);
    assert_eq!(
        (&body["concert_id"], &body["sequence"], &body["last_sequence"]),
        (&json!("c-1"), &json!(2), &json!(3))
    );
    let state = &body["state"];
    assert_eq!(
        (&state["sold"], &state["free_seats"], &state["sold_out"], &state["revenue"]),
        (&json!(1), &json!(39), &json!(false), &json!(120))
    );
    assert_eq!(state["info"], json!({ "artist": "A", "city": "C", "date": "2026-11-21" }));
    assert_eq!(
        skeleton(&state["bookings"]),
        json!({ "A1": skeleton(&state["bookings"]["A1"]) }),
        "only the first booking existed after event 2"
    );
    assert_eq!(state["bookings"]["A1"]["fan_id"], "fox");
    let event = &body["event"];
    let mut keys: Vec<&String> = event.as_object().unwrap().keys().collect();
    keys.sort();
    assert_eq!(keys, ["event_type", "metadata", "payload", "sequence", "summary"]);
    assert_eq!(
        (&event["sequence"], &event["event_type"], &event["summary"]),
        (&json!(2), &json!("SeatBooked"), &json!("A1 → fox"))
    );
    let mut payload = event["payload"].clone();
    assert_eq!(skeleton(&payload["booking_id"]), json!("<string>"));
    payload["booking_id"] = json!("<string>");
    assert_eq!(
        payload,
        json!({ "type": "SeatBooked", "seat": "A1", "zone": "vip", "price": 120, "fan_id": "fox", "fan_name": "fox",
                "booking_id": "<string>" })
    );
    assert!(event["metadata"]["command_id"].is_string(), "{event}");

    for uri in [
        "/api/concerts/c-1/state-at/0",
        "/api/concerts/c-1/state-at/4",
        "/api/concerts/c-9/state-at/1",
    ] {
        let (status, body) = send(&router, get(uri)).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
        assert_eq!(
            body,
            json!({ "outcome": "not_found",
                    "error": { "code": "sequence_out_of_range", "params": {}, "message": "this concert has no such event" } }),
            "{uri}"
        );
    }
}

#[tokio::test]
async fn the_status_of_the_read_side_answers_in_its_documented_shapes() {
    // No checkpoint rows: only the name, the aggregate type and the state of each projection.
    let app = app().await;
    let (_, body) = send(&app.router, get("/api/projections/status")).await;
    assert_eq!(
        body,
        json!({
            "state": "starting",
            "from_cache": true,
            "projections": [
                { "projection": "concert_halls", "aggregate_type": "Concert", "state": "starting" },
                { "projection": "fan_wallets", "aggregate_type": "Fan", "state": "starting" },
            ],
            "running": null,
            "last_result": null,
        })
    );

    let (router, _) = app_with_checkpoints("node-a").await;
    let (_, body) = send(&router, get("/api/projections/status")).await;
    let mut halls = body["projections"][0].clone();
    for (key, kind) in [
        ("position", "<string>"),
        ("delivered", "<number>"),
        ("lease_until", "<string>"),
    ] {
        assert_eq!(skeleton(&halls[key]), json!(kind), "{key}");
        halls[key] = json!(kind);
    }
    assert_eq!(
        halls,
        json!({
            "projection": "concert_halls", "aggregate_type": "Concert", "state": "live", "position": "<string>",
            "delivered": "<number>", "lag": 3, "lag_capped": false, "owner": "node-a", "lease_until": "<string>",
            "failures": 0, "last_error": null,
            "rebuild": { "requested": false, "cursor": null, "rebuilt": 0, "pace_ms": 0 },
        })
    );
    assert_eq!(
        (&body["state"], &body["from_cache"], &body["running"], &body["last_result"]),
        (&json!("live"), &json!(false), &Value::Null, &Value::Null)
    );
    assert_eq!(body.as_object().unwrap().len(), 5);

    // A requested rebuild: this node's `running`, and the rows asked.
    let (status, started) = send(&router, post("/api/projections/rebuild")).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(
        skeleton(&started),
        json!({ "outcome": "<string>", "rebuild_id": "<string>", "pace_ms": "<number>" })
    );
    assert_eq!((&started["outcome"], &started["pace_ms"]), (&json!("started"), &json!(0)));
    let (_, body) = send(&router, get("/api/projections/status")).await;
    assert_eq!(body["state"], "rebuilding");
    assert_eq!(
        skeleton(&body["running"]),
        json!({ "rebuild_id": "<string>", "pace_ms": "<number>", "started_at": "<string>", "keys_total": "<number>" })
    );
    assert_eq!(
        (
            &body["running"]["rebuild_id"],
            &body["running"]["pace_ms"],
            &body["running"]["keys_total"]
        ),
        (&started["rebuild_id"], &json!(0), &json!(1))
    );
    assert_eq!(body["projections"][0]["rebuild"]["requested"], true);
}

#[tokio::test]
async fn the_verification_answers_in_its_documented_shape() {
    // One history of three events and no documents: the hall is behind, none is wrong.
    let (router, _) = app_with_checkpoints("node-a").await;
    let (status, body) = send(&router, get("/api/projections/verify")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({ "ok": true, "checked": { "concert_halls": 1, "fan_wallets": 0 },
                "behind": { "documents": 1, "events": 3 }, "differences": [] })
    );

    // A document of a concert without a history is wrong.
    let hall = StoredView {
        id: "c-9".to_owned(),
        revision: 1,
        last_sequence: 1,
        view: ConcertHallView::default(),
    };
    let router = app_reading(vec![hall], vec![]).await;
    let (status, body) = send(&router, get("/api/projections/verify")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], false);
    assert_eq!(
        body["differences"],
        json!([{ "view": "concert_halls", "id": "c-9", "kind": "without_history" }])
    );
    assert_eq!(body.as_object().unwrap().len(), 4);
}
