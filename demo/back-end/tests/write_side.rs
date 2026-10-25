//! [P1] Integration test against a real PostgreSQL (testcontainers, same image as the root tests).
//! An accepted command writes one event and one task sharing the correlation id; a rejected
//! command writes nothing; the command's metadata is stored with the event. Bookings racing for
//! one concert wait for its lock instead of conflicting. The concert halls follow the journal through
//! their projection processor: woken by the commits, rebuilt in place, healed and restored after a wipe.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use chrono::NaiveDate;
use cqrsx::postgres::pool::{ConnectionPingPolicy, StatementTimeout, connect_pool};
use cqrsx::{
    cqrs::{
        aggregate::Aggregate,
        command::{locks::LockLimits, retry::AggregateConflictRetryPolicy, wakeup::CommitWakeup},
        projection::{
            View, ViewProjection,
            checkpoint::{CheckpointKey, CheckpointStore},
            processor::{
                ProjectionProcessor,
                config::{DeliveryLimits, ProcessorConfig},
            },
            repository::ViewRepository,
        },
        store::{EventStore, persisted::PersistedEventStore, repository::PersistedEventRepository},
        testing::{mem_checkpoint_store::MemCheckpointStore, mem_view_repository::MemViewRepository},
    },
    postgres::{
        EVENT_STORE_SCHEMA, TRANSACTIONAL_OUTBOX_SCHEMA,
        command::{PostgresCommandConfig, PostgresCommandExecutor},
        event_repository::PostgresEventRepository,
    },
};
use encore_backend::{
    application::{
        activity::ActivityHub,
        command_service::{CommandMetadata, CommandOutcome, CommandService},
        concert::ConcertApp,
        concert::views::concert_hall::ConcertHallView,
    },
    domain::{
        ConcertId, FanId,
        concert::{
            Concert, ConcertError, ConcertServices,
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
use tokio_util::sync::CancellationToken;
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
    let config = PostgresCommandConfig::new(10, AggregateConflictRetryPolicy::default(), LockLimits::default()).expect("config");
    let executor = PostgresCommandExecutor::new(pool.clone(), config, ConcertServices { ticket_limit: 4 });
    let concerts = ConcertApp::new(CommandService::new(executor, activity), history_store(pool.clone(), 10), 6);
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
async fn parallel_bookings_of_one_concert_wait_for_its_lock_and_need_no_retry() {
    let (pool, container) = database().await;
    // One attempt only: a booking that lost a version race would answer `Conflict`.
    let single_attempt = AggregateConflictRetryPolicy::new(1, Duration::ZERO, Duration::ZERO, 0.0).expect("policy");
    let config = PostgresCommandConfig::new(10, single_attempt, LockLimits::default()).expect("config");
    let executor = PostgresCommandExecutor::new(pool.clone(), config, ConcertServices { ticket_limit: 4 });
    let concerts = Arc::new(ConcertApp::new(
        CommandService::new(executor, Arc::new(ActivityHub::new(64, 64))),
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
            ProcessorConfig::new(POLL, 100, Duration::from_secs(1), DeliveryLimits::default()).expect("processor config");
        let processor = ProjectionProcessor::new(
            HALLS,
            Arc::new(PostgresEventRepository::new(self.pool.clone())),
            self.checkpoints.clone(),
            Arc::new(ViewProjection::new(self.halls.clone()).with_name(HALLS)),
            config,
        )
        .with_wakeup(self.wakeup.clone());
        let shutdown = CancellationToken::new();
        (shutdown.clone(), tokio::spawn(processor.run(shutdown)))
    }

    /// The fold of the concert's whole history, `None` without one.
    async fn fold(&self, id: &str) -> Option<ConcertHallView> {
        let events = PersistedEventStore::<_, Concert>::new_event_store(PostgresEventRepository::new(self.pool.clone()))
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
    fn delete(&self, id: &str) {
        let kept: Vec<_> = self
            .halls
            .list_all()
            .expect("halls")
            .into_iter()
            .filter(|(context, _)| context.view_instance_id != id)
            .collect();
        self.halls.clear();
        for (context, view) in kept {
            self.halls
                .put(&context.view_instance_id, context.version, context.last_sequence, &view)
                .expect("put");
        }
    }

    async fn version(&self, id: &str) -> i64 {
        self.halls
            .load_with_context(id)
            .await
            .expect("hall")
            .map(|(_, context)| context.version)
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
    let single_attempt = AggregateConflictRetryPolicy::new(1, Duration::ZERO, Duration::ZERO, 0.0).expect("policy");
    let config = PostgresCommandConfig::new(10, single_attempt, LockLimits::default()).expect("config");
    let executor = PostgresCommandExecutor::new(pool.clone(), config, ConcertServices { ticket_limit: 4 })
        .with_commit_wakeup(read.wakeup.clone());
    let concerts = Arc::new(ConcertApp::new(
        CommandService::new(executor, Arc::new(ActivityHub::new(64, 64))),
        history_store(pool.clone(), 10),
        6,
    ));
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
                .announce_concert(&concert_id, announce, CommandMetadata::with_source("seed"))
                .await
                .outcome;
            assert!(outcome.is_accepted(), "{outcome:?}");
        }
    };
    let book_all = |id: &'static str, seats: std::ops::Range<usize>| {
        let concerts = concerts.clone();
        async move {
            let mut bookings = tokio::task::JoinSet::new();
            for index in seats {
                let concerts = concerts.clone();
                bookings.spawn(async move {
                    let metadata = CommandMetadata::ui();
                    let book = BookSeat {
                        seat: format!("{}{}", ["A", "B", "C", "D", "E"][index / 8], index % 8 + 1),
                        fan_id: FanId::parse(&format!("fan-{index}")).expect("fan"),
                        fan_name: "Fan".into(),
                        booking_id: metadata.causation.command_id,
                    };
                    concerts
                        .book_seat(&ConcertId::parse(id).expect("concert id"), book, metadata)
                        .await
                        .outcome
                });
            }
            while let Some(outcome) = bookings.join_next().await {
                let outcome = outcome.expect("booking task");
                assert!(outcome.is_accepted(), "{outcome:?}");
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
    let (hall, context) = read
        .halls
        .load_with_context("hot-2026")
        .await
        .expect("hall")
        .expect("a document");
    assert_eq!((hall.sold, hall.last_sequence, context.last_sequence), (20, 21, 21));

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
    read.delete("early-2026");
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
