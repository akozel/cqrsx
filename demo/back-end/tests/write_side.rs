//! [P1] Integration test against a real PostgreSQL (testcontainers, same image as the root tests).
//! An accepted command writes one event and one task sharing the correlation id; a rejected
//! command writes nothing; the command's metadata is stored with the event. With per-aggregate
//! ordering, bookings racing for one concert take turns instead of conflicting.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use chrono::NaiveDate;
use cqrsx::postgres::pool::{ConnectionPingPolicy, StatementTimeout, connect_pool};
use cqrsx::{
    cqrs::command::{
        retry::AggregateConflictRetryPolicy,
        turns::{CommandOrdering, TurnLimits},
    },
    postgres::{
        EVENT_STORE_SCHEMA, TRANSACTIONAL_OUTBOX_SCHEMA,
        command::{PostgresCommandConfig, PostgresCommandExecutor},
    },
};
use encore_backend::{
    application::{
        activity::ActivityHub,
        command_service::{CommandMetadata, CommandOutcome, CommandService},
        concert::ConcertApp,
        lock::BoxOfficeLock,
    },
    domain::{
        ConcertId, FanId,
        concert::{
            ConcertError, ConcertServices,
            commands::{AnnounceConcert, BookSeat},
        },
    },
    infrastructure::postgres::history_store,
};
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::{sync::Arc, time::Duration};
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
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
    let activity = Arc::new(ActivityHub::new(64, 64));
    let lock = Arc::new(BoxOfficeLock::new());
    let config = PostgresCommandConfig::new(
        10,
        AggregateConflictRetryPolicy::default(),
        CommandOrdering::PerAggregate(TurnLimits::default()),
    )
    .expect("config");
    let executor = PostgresCommandExecutor::new(pool.clone(), config, ConcertServices { ticket_limit: 4 });
    let concerts = ConcertApp::new(
        CommandService::new(executor, activity, lock),
        history_store(pool.clone(), 10),
        6,
    );
    let concert_id = ConcertId::parse("test-2026").expect("concert id");
    let announce = AnnounceConcert {
        artist: "A".into(),
        city: "C".into(),
        date: NaiveDate::from_ymd_opt(2026, 11, 21).expect("date"),
    };
    assert!(
        concerts
            .announce_concert(&concert_id, announce, CommandMetadata::with_source("seed"))
            .await
            .outcome
            .is_accepted()
    );

    let metadata = CommandMetadata::ui();
    let book = BookSeat {
        seat: "B4".into(),
        fan_id: FanId::parse("alice").expect("fan"),
        fan_name: "Alice".into(),
        booking_id: metadata.causation.command_id,
    };
    let executed = concerts.book_seat(&concert_id, book, metadata.clone()).await;
    assert!(executed.outcome.is_accepted());
    assert_eq!(executed.tasks, vec!["encore_record_ticket v1".to_owned()]);

    let rejected = concerts
        .book_seat(
            &concert_id,
            BookSeat {
                seat: "B4".into(),
                fan_id: FanId::parse("bob").expect("fan"),
                fan_name: "Bob".into(),
                booking_id: Uuid::now_v7(),
            },
            CommandMetadata::ui(),
        )
        .await;
    assert!(matches!(
        rejected.outcome,
        CommandOutcome::Rejected(ConcertError::SeatTaken { .. })
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

    let tasks = sqlx::query("SELECT task_type, aggregate_sequence, metadata, status FROM transactional_outbox")
        .fetch_all(&pool)
        .await
        .expect("tasks");
    assert_eq!(tasks.len(), 1, "one task for the accepted booking only");
    assert_eq!(tasks[0].get::<String, _>("task_type"), "encore_record_ticket");
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
        ["causation_id", "command_id", "concert_id", "correlation_id", "fan_id"],
        "the transactional_outbox.metadata keys are unchanged"
    );

    pool.close().await;
    container.stop().await.expect("stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_bookings_of_one_concert_take_turns_and_need_no_retry() {
    let (pool, container) = database().await;
    // One attempt only: a booking that lost a version race would answer `Conflict`.
    let single_attempt = AggregateConflictRetryPolicy::new(1, Duration::ZERO, Duration::ZERO, 0.0).expect("policy");
    let config =
        PostgresCommandConfig::new(10, single_attempt, CommandOrdering::PerAggregate(TurnLimits::default())).expect("config");
    let executor = PostgresCommandExecutor::new(pool.clone(), config, ConcertServices { ticket_limit: 4 });
    let concerts = Arc::new(ConcertApp::new(
        CommandService::new(executor, Arc::new(ActivityHub::new(64, 64)), Arc::new(BoxOfficeLock::new())),
        history_store(pool.clone(), 10),
        6,
    ));
    let concert_id = ConcertId::parse("hot-2026").expect("concert id");
    let announce = AnnounceConcert {
        artist: "A".into(),
        city: "C".into(),
        date: NaiveDate::from_ymd_opt(2026, 11, 21).expect("date"),
    };
    assert!(
        concerts
            .announce_concert(&concert_id, announce, CommandMetadata::with_source("seed"))
            .await
            .outcome
            .is_accepted()
    );

    let mut bookings = tokio::task::JoinSet::new();
    for index in 0..20 {
        let (concerts, concert_id) = (concerts.clone(), concert_id.clone());
        bookings.spawn(async move {
            let metadata = CommandMetadata::ui();
            let book = BookSeat {
                seat: format!("{}{}", ["A", "B", "C"][index / 8], index % 8 + 1),
                fan_id: FanId::parse(&format!("fan-{index}")).expect("fan"),
                fan_name: "Fan".into(),
                booking_id: metadata.causation.command_id,
            };
            concerts.book_seat(&concert_id, book, metadata).await.outcome
        });
    }
    while let Some(outcome) = bookings.join_next().await {
        let outcome = outcome.expect("booking task");
        assert!(outcome.is_accepted(), "{outcome:?}");
    }
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events WHERE aggregate_id = 'hot-2026'")
        .fetch_one(&pool)
        .await
        .expect("events");
    assert_eq!(events, 21, "the announcement and every booking");

    pool.close().await;
    container.stop().await.expect("stop");
}
