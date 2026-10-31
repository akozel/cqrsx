//! [P1] Integration test against a real PostgreSQL (testcontainers, same image as the root tests).
//! An accepted command writes one event and one task sharing the correlation id; a rejected
//! command writes nothing; the command's metadata is stored with the event. Bookings racing for
//! one concert wait for its lock instead of conflicting. The concert halls follow the journal through
//! their projection processor: woken by the commits, rebuilt in place, healed and restored after a wipe.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use chrono::NaiveDate;
use cqrsx::engine::{
    Cqrs,
    mongodb::MongoDb,
    postgres::{Postgres, PostgresPools},
    settings::{DEFAULT_SNAPSHOT_EVERY, Settings},
};
use cqrsx::postgres::pool::{ConnectionPingPolicy, StatementTimeout, connect_pool};
use cqrsx::task::{attempt::TaskAttempt, format::TaskFormat, result::TaskResult};
use cqrsx::{
    command::{
        CommandExecutor, CommandGatewayError, CommitReport, locks::LockLimits, metadata::CommandMetadata,
        retry::AggregateConflictRetryPolicy,
    },
    cqrs::{
        projection::{
            View, ViewProjection,
            checkpoint::{CheckpointKey, CheckpointStore},
            processor::{
                ProjectionProcessor,
                config::{DeliveryLimits, ProcessorConfig},
            },
            repository::ViewRepository,
        },
        testing::{mem_checkpoint_store::MemCheckpointStore, mem_view_repository::MemViewRepository},
    },
    domain::aggregate::Aggregate,
    journal::{EventStore, persisted::PersistedEventStore, repository::PersistedEventRepository, wakeup::CommitWakeup},
    postgres::{
        EVENT_STORE_SCHEMA, TRANSACTIONAL_OUTBOX_SCHEMA,
        command::{PostgresCommandConfig, PostgresCommandExecutor},
        event_repository::PostgresEventRepository,
    },
    reactions::ReactionsPlanner,
};
use encore_backend::{
    DEMO_DEFAULTS, concert,
    concert::{
        application::views::concert_hall::ConcertHallView,
        domain::{
            Concert, ConcertCommand, ConcertError, ConcertRules,
            commands::{AnnounceConcert, BookSeat, CancelBooking},
        },
    },
    fan, operations,
    shared::{
        application::activity::ActivityHub,
        domain::{ConcertId, FanId},
    },
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sqlx::{PgPool, Row};
use std::{sync::Arc, time::Duration};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use uuid::Uuid;

/// A free host port outside the ephemeral range, like the cqrsx fixtures: explicit mappings work on
/// every Docker-compatible engine, including rootless Podman behind an API proxy. Tests of this file
/// start their containers at the same time, so a port handed out once is never handed out again: the
/// bind check alone cannot see a port another test has picked but not mapped yet.
fn lease_port() -> u16 {
    static TAKEN: std::sync::Mutex<Vec<u16>> = std::sync::Mutex::new(Vec::new());
    let mut taken = TAKEN.lock().unwrap();
    for port in (20_000..30_000).step_by(37).map(|p| p + (std::process::id() % 37) as u16) {
        if !taken.contains(&port) && std::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, port)).is_ok() {
            taken.push(port);
            return port;
        }
    }
    panic!("no free port")
}

/// The formats of the tasks an accepted command committed, none for a refused one: what the HTTP answer lists.
fn committed(answer: &Result<CommitReport<Concert>, CommandGatewayError<ConcertError>>) -> Vec<String> {
    match answer {
        Ok(report) => report.tasks.iter().map(|task| task.format().to_string()).collect(),
        Err(_) => Vec::new(),
    }
}

/// The engine of the process over the variables of nobody: its modules as `main.rs` registers them.
fn process() -> cqrsx::engine::build::Builder {
    Cqrs::from_lookup("ENCORE", |_| None)
        .defaults(DEMO_DEFAULTS)
        .module(concert::module)
        .module(fan::module)
        .module(operations::module)
}

/// The box office's reactions with the policies of the configuration without environment, as the process plans.
fn concert_reactions() -> ReactionsPlanner<Concert> {
    // An engine in memory builds without waiting.
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let std::task::Poll::Ready(cqrs) = std::pin::pin!(process().build()).poll(&mut context) else {
        panic!("the engine in memory waits");
    };
    ReactionsPlanner::new(cqrs.expect("configuration").policies().clone())
}

async fn database() -> (PgPool, testcontainers::ContainerAsync<GenericImage>) {
    let container = GenericImage::new("docker.io/library/postgres", "18.6-alpine")
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::Nothing)
        .with_mapped_port(lease_port(), 5432.tcp())
        .with_env_var("POSTGRES_DB", "encore_test")
        .with_env_var("POSTGRES_USER", "encore_test")
        .with_env_var("POSTGRES_PASSWORD", "encore_test")
        .start()
        .await
        .expect("PostgreSQL container");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5432.tcp()).await.expect("port");
    let options = format!("postgresql://encore_test:encore_test@{host}:{port}/encore_test")
        .parse::<sqlx::postgres::PgConnectOptions>()
        .expect("options")
        .ssl_mode(sqlx::postgres::PgSslMode::Disable);
    let pool = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let Ok(pool) = connect_pool(
                options.clone(),
                ConnectionPingPolicy::Idle(cqrsx::postgres::pool::DEFAULT_IDLE_PING_AFTER),
                StatementTimeout::Default,
            )
            .await
                && sqlx::query("SELECT 1").execute(&pool).await.is_ok()
            {
                break pool;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("PostgreSQL readiness");
    sqlx::raw_sql(EVENT_STORE_SCHEMA)
        .execute(&pool)
        .await
        .expect("event store schema");
    sqlx::raw_sql(TRANSACTIONAL_OUTBOX_SCHEMA)
        .execute(&pool)
        .await
        .expect("outbox schema");
    (pool, container)
}

#[tokio::test]
async fn accepted_command_writes_event_and_task_atomically_and_rejected_writes_nothing() {
    let (pool, container) = database().await;
    let config = PostgresCommandConfig::new(
        DEFAULT_SNAPSHOT_EVERY,
        AggregateConflictRetryPolicy::default(),
        LockLimits::default(),
    );
    let concerts = PostgresCommandExecutor::new(pool.clone(), config, ConcertRules { ticket_limit: 4 })
        .with_task_planner(concert_reactions());
    let concert_id = ConcertId::parse("test-2026").expect("concert id");
    let announce = AnnounceConcert {
        artist: "A".into(),
        city: "C".into(),
        date: NaiveDate::from_ymd_opt(2026, 11, 21).expect("date"),
    };
    assert!(
        concerts
            .execute_with_metadata(
                &concert_id,
                ConcertCommand::AnnounceConcert(announce),
                CommandMetadata::root("seed").to_map(),
            )
            .await
            .is_ok()
    );

    let metadata = CommandMetadata::root("ui");
    let book = BookSeat {
        seat: "B4".into(),
        fan_id: FanId::parse("alice").expect("fan"),
        fan_name: "Alice".into(),
        booking_id: metadata.causation.command_id,
    };
    let executed = concerts
        .execute_with_metadata(&concert_id, ConcertCommand::BookSeat(book), metadata.to_map())
        .await;
    assert!(executed.is_ok());
    assert_eq!(committed(&executed), vec!["encore_record_ticket v1".to_owned()]);

    let rejected = concerts
        .execute_with_metadata(
            &concert_id,
            ConcertCommand::BookSeat(BookSeat {
                seat: "B4".into(),
                fan_id: FanId::parse("bob").expect("fan"),
                fan_name: "Bob".into(),
                booking_id: Uuid::now_v7(),
            }),
            CommandMetadata::root("ui").to_map(),
        )
        .await;
    assert!(matches!(
        rejected,
        Err(CommandGatewayError::Domain(ConcertError::SeatTaken { .. }))
    ));

    let events =
        sqlx::query("SELECT sequence, event_type, metadata FROM events WHERE aggregate_id = 'test-2026' ORDER BY sequence")
            .fetch_all(&pool)
            .await
            .expect("events");
    assert_eq!(events.len(), 2, "announce + booking; the rejected booking wrote nothing");
    let booked = &events[1];
    assert_eq!(booked.get::<String, _>("event_type"), "SeatBooked");
    let stored: Value = booked.get("metadata");
    assert_eq!(stored["command_id"], metadata.causation.command_id.to_string());
    assert_eq!(stored["correlation_id"], metadata.causation.correlation_id.to_string());
    assert_eq!(stored["recorded_at"], metadata.recorded_at);
    assert_eq!(stored["source"], "ui");
    assert_eq!(stored["causation_id"], metadata.causation.command_id.to_string());
    assert_eq!(
        stored.as_object().map(|m| m.len()),
        Some(5),
        "the events.metadata keys are unchanged"
    );

    let tasks = sqlx::query("SELECT id, task_type, aggregate_sequence, metadata, status FROM transactional_outbox")
        .fetch_all(&pool)
        .await
        .expect("tasks");
    assert_eq!(tasks.len(), 1, "one task for the accepted booking only");
    assert_eq!(tasks[0].get::<String, _>("task_type"), "encore_record_ticket");
    assert_eq!(
        tasks[0].get::<Uuid, _>("id"),
        metadata.causation.task_id(
            "Concert",
            "test-2026",
            &TaskFormat::try_new("encore_record_ticket", 1).expect("format"),
            0
        ),
        "the task id derives from the booking's command"
    );
    assert_eq!(tasks[0].get::<i64, _>("aggregate_sequence"), 2);
    assert_eq!(tasks[0].get::<String, _>("status"), "pending");
    let task_metadata: Value = tasks[0].get("metadata");
    assert_eq!(task_metadata["correlation_id"], metadata.causation.correlation_id.to_string());
    let mut keys: Vec<&str> = task_metadata
        .as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        ["causation_id", "command_id", "correlation_id"],
        "the transactional_outbox.metadata keys are the causation ids"
    );

    pool.close().await;
    container.stop().await.expect("stop");
}

/// `POST /api/concerts/{concert_id}/commands` on the real graph: its answer in its shape and status, and what the
/// activity feed says of every command, a step of the ledger issued by the composition's task handlers included.
async fn post_command(router: &axum::Router, concert_id: &str, command: Value) -> (StatusCode, Value) {
    let request = Request::post(format!("/api/concerts/{concert_id}/commands"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(command.to_string()))
        .expect("request");
    let response = router.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let body = response.into_body().collect().await.expect("body").to_bytes();
    (status, serde_json::from_slice(&body).expect("JSON body"))
}

#[tokio::test]
async fn the_command_endpoint_answers_in_its_shapes_and_the_feed_logs_every_command_with_its_values() {
    let (pool, container) = database().await;
    let mut mongo = mongodb::options::ClientOptions::parse("mongodb://127.0.0.1:1")
        .await
        .expect("options");
    mongo.server_selection_timeout = Some(Duration::from_millis(50));
    let settings = Settings::from_lookup("ENCORE", |_| None, &DEMO_DEFAULTS).expect("configuration");
    let pools = PostgresPools {
        main: pool.clone(),
        history: pool.clone(),
        projection: pool.clone(),
    };
    let cqrs = Cqrs::with_settings(settings)
        .event_store(Postgres)
        .postgres_pools(pools)
        .projections(MongoDb)
        .mongodb_client(mongodb::Client::with_options(mongo).expect("client"))
        .module(concert::module)
        .module(fan::module)
        .module(operations::module)
        .build()
        .await
        .expect("wiring");
    let cqrs = Arc::new(cqrs);
    let router = cqrs.router();

    let (status, announced) = post_command(
        &router,
        "http-2026",
        json!({ "type": "AnnounceConcert", "artist": "Radiohead", "city": "Tbilisi", "date": "2026-11-21" }),
    )
    .await;
    let c1 = announced["command_id"].clone();
    assert_eq!(
        (status, &announced),
        (
            StatusCode::OK,
            &json!({ "outcome": "accepted", "command_id": c1, "concert_id": "http-2026", "tasks": [] })
        )
    );
    let (status, booked) = post_command(
        &router,
        "http-2026",
        json!({ "type": "BookSeat", "seat": "A1", "fan_id": "fox", "fan_name": "Fox" }),
    )
    .await;
    let c2 = booked["command_id"].clone();
    assert_eq!(
        (status, &booked),
        (
            StatusCode::OK,
            &json!({ "outcome": "accepted", "command_id": c2, "concert_id": "http-2026",
                     "tasks": ["encore_record_ticket v1"] })
        )
    );
    let (status, taken) = post_command(
        &router,
        "http-2026",
        json!({ "type": "BookSeat", "seat": "A1", "fan_id": "panda", "fan_name": "Panda" }),
    )
    .await;
    let c3 = taken["command_id"].clone();
    assert_eq!(
        (status, &taken),
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            &json!({ "outcome": "rejected",
                     "error": { "code": "seat_taken", "params": { "seat": "A1", "by": "Fox" },
                                "message": "seat A1 is already taken by Fox" },
                     "command_id": c3, "concert_id": "http-2026" })
        )
    );
    let stored: Vec<Value> = sqlx::query_scalar("SELECT metadata FROM events WHERE aggregate_id = 'http-2026' ORDER BY sequence")
        .fetch_all(&pool)
        .await
        .expect("events");
    assert_eq!(
        stored
            .iter()
            .map(|metadata| metadata["command_id"].clone())
            .collect::<Vec<_>>(),
        [c1.clone(), c2.clone()],
        "the answer's command id is the event's; the rejected booking wrote nothing"
    );

    // The step of the ledger the booking published, run by the composition's handlers.
    let row = sqlx::query(
        "SELECT id, task_type, task_version, payload, metadata FROM transactional_outbox WHERE task_type = 'encore_record_ticket'",
    )
    .fetch_one(&pool)
    .await
    .expect("the booking's task");
    let attempt = TaskAttempt::try_new(
        row.get("id"),
        TaskFormat::try_new(row.get::<String, _>("task_type"), row.get("task_version")).expect("format"),
        row.get("payload"),
        row.get("metadata"),
        1,
    )
    .expect("attempt");
    assert_eq!(
        cqrs.dispatcher()
            .expect("task handlers")
            .dispatch(Arc::new(attempt), Duration::ZERO)
            .await,
        TaskResult::Done,
        "the step records the ticket"
    );
    let step: Value = sqlx::query_scalar("SELECT metadata FROM events WHERE aggregate_type = 'Fan'")
        .fetch_one(&pool)
        .await
        .expect("the ledger's event");

    let entries: Vec<(String, Value)> = cqrs
        .dependency::<ActivityHub>()
        .expect("the activity log")
        .subscribe()
        .0
        .iter()
        .filter(|entry| entry.kind.starts_with("command."))
        .map(|entry| (entry.kind.clone(), Value::Object(entry.fields.clone())))
        .collect();
    let entry = |kind: &str, fields: Value| (kind.to_owned(), fields);
    assert_eq!(
        entries,
        [
            entry(
                "command.accepted",
                json!({ "aggregate_type": "Concert", "aggregate_id": "http-2026", "command_id": c1, "correlation_id": c1,
                        "source": "ui", "command_type": "AnnounceConcert", "fan_id": null, "tasks": [] })
            ),
            entry(
                "command.accepted",
                json!({ "aggregate_type": "Concert", "aggregate_id": "http-2026", "command_id": c2, "correlation_id": c2,
                        "source": "ui", "command_type": "BookSeat", "fan_id": "fox",
                        "tasks": ["encore_record_ticket v1"] })
            ),
            entry(
                "command.rejected",
                json!({ "aggregate_type": "Concert", "aggregate_id": "http-2026", "command_id": c3, "correlation_id": c3,
                        "source": "ui", "command_type": "BookSeat", "fan_id": "panda", "code": "seat_taken",
                        "params": { "seat": "A1", "by": "Fox" } })
            ),
            entry(
                "command.accepted",
                json!({ "aggregate_type": "Fan", "aggregate_id": "fox:http-2026", "command_id": step["command_id"],
                        "correlation_id": c2, "source": "task:encore_record_ticket", "command_type": "RecordTicket",
                        "fan_id": "fox", "tasks": ["encore_send_e_ticket v1"] })
            ),
        ]
    );
    assert!(Uuid::parse_str(c1.as_str().expect("a command id is text")).is_ok(), "{c1}");

    pool.close().await;
    container.stop().await.expect("stop");
}

/// Bookings and cancellations of one concert racing on two nodes (two executors, each with its own
/// locks): whatever the optimistic check decided, every committed `SeatBooked` and `BookingCancelled` has
/// exactly one task, and the task carries the booking of its own event.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bookings_and_cancellations_racing_on_two_nodes_publish_one_matching_task_each() {
    let (pool, container) = database().await;
    let node = || {
        let config = PostgresCommandConfig::new(
            DEFAULT_SNAPSHOT_EVERY,
            AggregateConflictRetryPolicy::default(),
            LockLimits::default(),
        );
        let executor = PostgresCommandExecutor::new(pool.clone(), config, ConcertRules { ticket_limit: 40 })
            .with_task_planner(concert_reactions());
        Arc::new(executor)
    };
    let nodes = [node(), node()];
    let concert_id = ConcertId::parse("race-2026").expect("concert id");
    let announce = AnnounceConcert {
        artist: "A".into(),
        city: "C".into(),
        date: NaiveDate::from_ymd_opt(2026, 11, 21).expect("date"),
    };
    assert!(
        nodes[0]
            .execute_with_metadata(
                &concert_id,
                ConcertCommand::AnnounceConcert(announce),
                CommandMetadata::root("seed").to_map(),
            )
            .await
            .is_ok()
    );
    let mut commands = tokio::task::JoinSet::new();
    for index in 0..40 {
        let (concerts, concert_id) = (nodes[index % 2].clone(), concert_id.clone());
        commands.spawn(async move {
            // Four fans each book and cancel their seat, back and forth, on both nodes.
            let fan = index % 4;
            let seat = format!("A{}", fan + 1);
            let fan_id = FanId::parse(&format!("fan-{fan}")).expect("fan");
            let metadata = CommandMetadata::root("ui");
            if index % 8 < 4 {
                let book = BookSeat {
                    seat,
                    fan_id,
                    fan_name: "Fan".into(),
                    booking_id: metadata.causation.command_id,
                };
                concerts
                    .execute_with_metadata(&concert_id, ConcertCommand::BookSeat(book), metadata.to_map())
                    .await
            } else {
                let cancel = CancelBooking {
                    seat,
                    fan_id,
                    cancellation_id: metadata.causation.command_id,
                };
                concerts
                    .execute_with_metadata(&concert_id, ConcertCommand::CancelBooking(cancel), metadata.to_map())
                    .await
            }
        });
    }
    while let Some(outcome) = commands.join_next().await {
        match outcome.expect("command task") {
            Ok(_) | Err(CommandGatewayError::Domain(_) | CommandGatewayError::Conflict) => {}
            Err(CommandGatewayError::Unavailable) => panic!("the store stayed reachable"),
        }
    }
    let unmatched: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM events e FULL JOIN transactional_outbox t
           ON t.aggregate_type = e.aggregate_type AND t.aggregate_id = e.aggregate_id AND t.aggregate_sequence = e.sequence
         WHERE coalesce(e.aggregate_id, t.aggregate_id) = 'race-2026' AND (e.event_type IS NULL OR e.event_type <> 'ConcertAnnounced')
           AND (e.sequence IS NULL OR t.id IS NULL OR (e.payload::jsonb ->> 'booking_id') <> (t.payload ->> 'booking_id'))",
    )
    .fetch_one(&pool)
    .await
    .expect("join");
    assert_eq!(
        unmatched, 0,
        "every booking and cancellation has its task, every task its event"
    );
    let (events, tasks): (i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM events WHERE aggregate_id = 'race-2026' AND event_type <> 'ConcertAnnounced'),
                (SELECT count(*) FROM transactional_outbox WHERE aggregate_id = 'race-2026')",
    )
    .fetch_one(&pool)
    .await
    .expect("counts");
    assert!(events > 0);
    assert_eq!(events, tasks);
    pool.close().await;
    container.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_bookings_of_one_concert_wait_for_its_lock_and_need_no_retry() {
    let (pool, container) = database().await;
    // One attempt only: a booking that lost a version race would answer `Conflict`.
    let single_attempt = AggregateConflictRetryPolicy::try_new(1, Duration::ZERO, Duration::ZERO, 0.0).expect("policy");
    let config = PostgresCommandConfig::new(DEFAULT_SNAPSHOT_EVERY, single_attempt, LockLimits::default());
    let executor = PostgresCommandExecutor::new(pool.clone(), config, ConcertRules { ticket_limit: 4 })
        .with_task_planner(concert_reactions());
    let concerts = Arc::new(executor);
    let concert_id = ConcertId::parse("hot-2026").expect("concert id");
    let announce = AnnounceConcert {
        artist: "A".into(),
        city: "C".into(),
        date: NaiveDate::from_ymd_opt(2026, 11, 21).expect("date"),
    };
    assert!(
        concerts
            .execute_with_metadata(
                &concert_id,
                ConcertCommand::AnnounceConcert(announce),
                CommandMetadata::root("seed").to_map(),
            )
            .await
            .is_ok()
    );

    let mut bookings = tokio::task::JoinSet::new();
    for index in 0..20 {
        let (concerts, concert_id) = (concerts.clone(), concert_id.clone());
        bookings.spawn(async move {
            let metadata = CommandMetadata::root("ui");
            let book = BookSeat {
                seat: format!("{}{}", ["A", "B", "C"][index / 8], index % 8 + 1),
                fan_id: FanId::parse(&format!("fan-{index}")).expect("fan"),
                fan_name: "Fan".into(),
                booking_id: metadata.causation.command_id,
            };
            concerts
                .execute_with_metadata(&concert_id, ConcertCommand::BookSeat(book), metadata.to_map())
                .await
        });
    }
    while let Some(outcome) = bookings.join_next().await {
        let outcome = outcome.expect("booking task");
        assert!(outcome.is_ok(), "{outcome:?}");
    }
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events WHERE aggregate_id = 'hot-2026'")
        .fetch_one(&pool)
        .await
        .expect("events");
    assert_eq!(events, 21, "the announcement and every booking");

    pool.close().await;
    container.stop().await.expect("stop");
}

const HALLS: &str = "concert_halls";

type Halls = MemViewRepository<ConcertHallView, Concert>;

/// The poll is long, so a quick catch-up proves that the commit woke the processor.
const POLL: Duration = Duration::from_secs(2);

/// The concert halls fed from the journal: the processor's checkpoint and the documents in memory, the
/// journal in PostgreSQL.
struct ReadSide {
    pool: PgPool,
    checkpoints: Arc<MemCheckpointStore>,
    halls: Arc<Halls>,
    wakeup: CommitWakeup,
}

impl ReadSide {
    fn new(pool: &PgPool) -> Self {
        Self {
            pool: pool.clone(),
            checkpoints: Arc::new(MemCheckpointStore::default()),
            halls: Arc::new(Halls::default()),
            wakeup: CommitWakeup::new(),
        }
    }

    fn key() -> CheckpointKey {
        CheckpointKey::new(HALLS, Concert::TYPE)
    }

    /// Starts the processor; cancel the token to stop it.
    fn start(&self) -> (CancellationToken, tokio::task::JoinHandle<()>) {
        let config =
            ProcessorConfig::try_new(POLL, 100, Duration::from_secs(1), DeliveryLimits::default()).expect("processor config");
        let processor = ProjectionProcessor::new(
            HALLS,
            Arc::new(PostgresEventRepository::new(self.pool.clone())),
            self.checkpoints.clone(),
            Arc::new(ViewProjection::new(self.halls.clone())),
            config,
        )
        .with_wakeup(self.wakeup.clone());
        let shutdown = CancellationToken::new();
        (shutdown.clone(), tokio::spawn(processor.run(shutdown)))
    }

    /// The fold of the concert's whole history, `None` without one.
    async fn fold(&self, id: &str) -> Option<ConcertHallView> {
        let events = PersistedEventStore::<_, Concert>::new(PostgresEventRepository::new(self.pool.clone()))
            .load_events(id)
            .await
            .expect("journal");
        if events.is_empty() {
            return None;
        }
        let mut hall = ConcertHallView::default();
        for event in &events {
            hall.update(event);
        }
        Some(hall)
    }

    /// Whether the checkpoint stands at the head and every hall equals the fold of its history.
    async fn is_caught_up(&self, ids: &[&str]) -> bool {
        let head = PostgresEventRepository::new(self.pool.clone())
            .head::<Concert>()
            .await
            .expect("head");
        let at_head = self
            .checkpoints
            .status(Some(HALLS))
            .await
            .expect("status")
            .first()
            .is_some_and(|row| row.position == head);
        if !at_head {
            return false;
        }
        for id in ids {
            if self.halls.load(id).await.expect("hall") != self.fold(id).await {
                return false;
            }
        }
        true
    }

    /// Waits until caught up; panics after `within`.
    async fn caught_up(&self, ids: &[&str], within: Duration) {
        tokio::time::timeout(within, async {
            while !self.is_caught_up(ids).await {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("the halls {ids:?} did not catch up with the journal within {within:?}"));
    }

    /// Drops the document of `id`, as the X-ray's single delete does.
    async fn delete(&self, id: &str) {
        let kept: Vec<_> = self
            .halls
            .list()
            .await
            .expect("halls")
            .into_iter()
            .filter(|stored| stored.id != id)
            .collect();
        self.halls.clear();
        for stored in kept {
            self.halls
                .put(&stored.id, stored.version, stored.last_sequence, &stored.view)
                .expect("put");
        }
    }

    async fn version(&self, id: &str) -> i64 {
        self.halls
            .load_with_context(id)
            .await
            .expect("hall")
            .map(|stored| stored.version)
            .unwrap_or_default()
    }
}

/// The halls follow the journal through their processor, whatever happens to the read side: bookings made
/// while it is stopped, a burst woken by the commits, Encore! with the box office open, a document lost and
/// healed, one hall rebuilt on its own, and both the documents and the checkpoint wiped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_concert_halls_follow_the_journal_through_their_processor() {
    let (pool, container) = database().await;
    let read = ReadSide::new(&pool);
    let single_attempt = AggregateConflictRetryPolicy::try_new(1, Duration::ZERO, Duration::ZERO, 0.0).expect("policy");
    let config = PostgresCommandConfig::new(DEFAULT_SNAPSHOT_EVERY, single_attempt, LockLimits::default());
    let executor = PostgresCommandExecutor::new(pool.clone(), config, ConcertRules { ticket_limit: 4 })
        .with_task_planner(concert_reactions())
        .with_commit_wakeup(read.wakeup.clone());
    let concerts = Arc::new(executor);
    let announce = |id: &'static str| {
        let concerts = concerts.clone();
        async move {
            let announce = AnnounceConcert {
                artist: "A".into(),
                city: "C".into(),
                date: NaiveDate::from_ymd_opt(2026, 11, 21).expect("date"),
            };
            let concert_id = ConcertId::parse(id).expect("concert id");
            let outcome = concerts
                .execute_with_metadata(
                    &concert_id,
                    ConcertCommand::AnnounceConcert(announce),
                    CommandMetadata::root("seed").to_map(),
                )
                .await;
            assert!(outcome.is_ok(), "{outcome:?}");
        }
    };
    let book_all = |id: &'static str, seats: std::ops::Range<usize>| {
        let concerts = concerts.clone();
        async move {
            let mut bookings = tokio::task::JoinSet::new();
            for index in seats {
                let concerts = concerts.clone();
                bookings.spawn(async move {
                    let metadata = CommandMetadata::root("ui");
                    let book = BookSeat {
                        seat: format!("{}{}", ["A", "B", "C", "D", "E"][index / 8], index % 8 + 1),
                        fan_id: FanId::parse(&format!("fan-{index}")).expect("fan"),
                        fan_name: "Fan".into(),
                        booking_id: metadata.causation.command_id,
                    };
                    concerts
                        .execute_with_metadata(
                            &ConcertId::parse(id).expect("concert id"),
                            ConcertCommand::BookSeat(book),
                            metadata.to_map(),
                        )
                        .await
                });
            }
            while let Some(outcome) = bookings.join_next().await {
                let outcome = outcome.expect("booking task");
                assert!(outcome.is_ok(), "{outcome:?}");
            }
        }
    };

    // Bookings while the processor is stopped wait in the journal and are applied once it runs.
    announce("early-2026").await;
    book_all("early-2026", 0..2).await;
    assert!(read.halls.load("early-2026").await.expect("hall").is_none());
    let (shutdown, running) = read.start();
    read.caught_up(&["early-2026"], Duration::from_secs(10)).await;

    // A burst of parallel bookings: the commits wake the processor long before its poll.
    announce("hot-2026").await;
    book_all("hot-2026", 0..20).await;
    read.caught_up(&["early-2026", "hot-2026"], POLL / 2).await;
    let stored = read
        .halls
        .load_with_context("hot-2026")
        .await
        .expect("hall")
        .expect("a document");
    assert_eq!(
        (stored.view.sold, stored.view.last_sequence, stored.last_sequence),
        (20, 21, 21)
    );

    // Encore! with the box office open: the documents are replaced in place under new bookings.
    assert_eq!(
        read.checkpoints
            .request_rebuild(&ReadSide::key(), Duration::ZERO)
            .await
            .expect("request"),
        cqrsx::cqrs::projection::checkpoint::RebuildRequest::Accepted
    );
    book_all("hot-2026", 20..25).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row = read.checkpoints.status(Some(HALLS)).await.expect("status").remove(0);
            if !row.rebuild_requested && row.rebuild_cursor.is_none() && row.rebuilt == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Encore! ends");
    read.caught_up(&["early-2026", "hot-2026"], Duration::from_secs(10)).await;

    // A lost document is healed by the next booking of its concert, from the whole history.
    read.delete("early-2026").await;
    book_all("early-2026", 2..3).await;
    read.caught_up(&["early-2026", "hot-2026"], Duration::from_secs(10)).await;

    // One hall rebuilt on request; its neighbour is not touched.
    let (early, hot) = (read.version("early-2026").await, read.version("hot-2026").await);
    read.checkpoints
        .request_rebuild_key(&ReadSide::key(), "early-2026")
        .await
        .expect("request");
    book_all("hot-2026", 25..26).await; // wakes the processor, which takes the request on its next poll
    tokio::time::timeout(POLL * 3, async {
        while read.version("early-2026").await == early {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the hall is rebuilt");
    read.caught_up(&["early-2026", "hot-2026"], Duration::from_secs(10)).await;
    assert_eq!(
        read.version("hot-2026").await,
        hot + 1,
        "only its own new booking touched the neighbour"
    );

    // 🧨 wipe: the documents and the checkpoint go; the processor registers again and restores everything.
    read.halls.clear();
    read.checkpoints.clear();
    read.caught_up(&["early-2026", "hot-2026"], Duration::from_secs(10)).await;
    assert_eq!(read.halls.load("hot-2026").await.expect("hall").expect("restored").sold, 26);

    shutdown.cancel();
    running.await.expect("processor");
    pool.close().await;
    container.stop().await.expect("stop");
}
