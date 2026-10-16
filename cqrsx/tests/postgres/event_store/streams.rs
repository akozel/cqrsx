use super::*;
use cqrsx::cqrs::{
    event::EventEnvelope,
    event_handler::{EventHandler, replay::EventReplay},
    store::stream::ReplayStream,
};
use cqrsx::postgres::event_repository::PostgresEventRepository;
use std::sync::Mutex;
use tokio::sync::Notify;

async fn reader_pool(admin: &PgPool, name: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect_with(admin.connect_options().as_ref().clone().application_name(name))
        .await
        .unwrap()
}
async fn open<R: PersistedEventRepository>(repo: &R, all: bool) -> ReplayStream {
    if all {
        repo.stream_all_events::<TestAggregate>().await.unwrap()
    } else {
        repo.stream_events::<TestAggregate>("fault").await.unwrap()
    }
}
fn assert_sql_error(error: &PersistenceError, decode: bool) {
    match (error, decode) {
        (PersistenceError::UnknownError(source), false) | (PersistenceError::DeserializationError(source), true) => {
            assert!(source.is::<sqlx::Error>())
        }
        _ => panic!("incorrect stream error category: {error:?}"),
    }
}
struct CountQuery(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl EventHandler<TestAggregate> for CountQuery {
    async fn handle(&self, _: &str, events: &[EventEnvelope<TestAggregate>]) -> Result<(), PersistenceError> {
        self.0.fetch_add(events.len(), Ordering::SeqCst);
        Ok(())
    }
}
async fn terminal<R: PersistedEventRepository>(repo: R, all: bool, decode: bool) {
    assert_sql_error(&repo.get_events::<TestAggregate>("fault").await.unwrap_err(), decode);
    let mut stream = open(&repo, all).await;
    let Some(Err(error)) = tokio::time::timeout(Duration::from_secs(5), stream.next::<TestAggregate>(&[]))
        .await
        .unwrap()
    else {
        panic!("SQL/row failure must be an error item")
    };
    assert_sql_error(&error, decode);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), stream.next::<TestAggregate>(&[]))
            .await
            .unwrap()
            .is_none()
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let reports = Arc::new(AtomicUsize::new(0));
    let mut replay = EventReplay::new(repo, CountQuery(calls.clone()));
    replay.set_error_observer(Box::new({
        let reports = reports.clone();
        move |_| {
            reports.fetch_add(1, Ordering::SeqCst);
        }
    }));
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        if all {
            replay.replay_all().await
        } else {
            replay.replay("fault").await
        }
    })
    .await
    .unwrap();
    assert_sql_error(&result.unwrap_err(), decode);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(reports.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn both_adapters_report_missing_tables_and_row_decode_failures_then_eof() {
    let db = Database::new().await;
    for decode in [false, true] {
        if decode {
            sqlx::raw_sql(EVENT_STORE_SCHEMA).execute(&db.pool).await.unwrap();
            PostgresEventRepository::new(db.pool.clone())
                .persist::<TestAggregate>(&[event("fault", 1)], None)
                .await
                .unwrap();
            sqlx::raw_sql("ALTER TABLE events RENAME TO valid_events; CREATE VIEW events AS SELECT aggregate_type,aggregate_id,sequence,event_type,event_version,payload,NULL::json AS metadata FROM valid_events").execute(&db.pool).await.unwrap();
        } else {
            sqlx::query("DROP TABLE events").execute(&db.pool).await.unwrap();
        }
        for outbox in [false, true] {
            for all in [false, true] {
                let pool = reader_pool(&db.pool, "stream-terminal").await;
                if outbox {
                    terminal(
                        PostgresEventRepositoryWithOutbox::new(pool.clone()).with_streaming_channel_size(1),
                        all,
                        decode,
                    )
                    .await;
                } else {
                    terminal(
                        PostgresEventRepository::new(pool.clone()).with_streaming_channel_size(1),
                        all,
                        decode,
                    )
                    .await;
                }
                tokio::time::timeout(Duration::from_secs(5), pool.close()).await.unwrap();
            }
        }
    }
    db.close().await;
}

async fn blocked_pid(admin: &PgPool, name: &str, wait: &str) -> i32 {
    tokio::time::timeout(Duration::from_secs(10),async {loop {
        let pid:Option<i32>=sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND application_name=$1 AND state='active' AND (wait_event=$2 OR wait_event_type=$2) AND strpos(query,'FROM events')>0").bind(name).bind(wait).fetch_optional(admin).await.unwrap();
        if let Some(pid)=pid {break pid;}tokio::time::sleep(Duration::from_millis(10)).await;
    }}).await.expect("the PostgreSQL query must reach the controlled wait")
}
async fn consumer_close<R: PersistedEventRepository>(repo: R, admin: &PgPool, pool: &PgPool, name: &str, all: bool) {
    let mut lock = admin.begin().await.unwrap();
    sqlx::query("LOCK TABLE events IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let stream = open(&repo, all).await;
    blocked_pid(admin, name, "Lock").await;
    drop(stream);
    // This must work while the old SELECT still cannot pass the table lock.
    let reused = tokio::time::timeout(Duration::from_secs(5), sqlx::query("SELECT 1").execute(pool)).await;
    lock.commit().await.unwrap();
    reused.expect("closed consumer must release the only connection").unwrap();
}
#[tokio::test]
async fn closing_either_consumer_cancels_blocked_sql_and_releases_the_only_connection() {
    let db = Database::new().await;
    for outbox in [false, true] {
        for all in [false, true] {
            let name = format!("stream-close-{outbox}-{all}");
            let pool = reader_pool(&db.pool, &name).await;
            if outbox {
                consumer_close(
                    PostgresEventRepositoryWithOutbox::new(pool.clone()).with_streaming_channel_size(1),
                    &db.pool,
                    &pool,
                    &name,
                    all,
                )
                .await;
            } else {
                consumer_close(
                    PostgresEventRepository::new(pool.clone()).with_streaming_channel_size(1),
                    &db.pool,
                    &pool,
                    &name,
                    all,
                )
                .await;
            }
            tokio::time::timeout(Duration::from_secs(5), pool.close()).await.unwrap();
        }
    }
    db.close().await;
}

struct PauseQuery {
    seen: Arc<Mutex<Vec<usize>>>,
    first: Arc<Notify>,
    release: Arc<Notify>,
}
#[async_trait::async_trait]
impl EventHandler<TestAggregate> for PauseQuery {
    async fn handle(&self, _: &str, events: &[EventEnvelope<TestAggregate>]) -> Result<(), PersistenceError> {
        for event in events {
            self.seen.lock().unwrap().push(event.sequence);
            if event.sequence == 1 {
                self.first.notify_one();
                self.release.notified().await;
            }
        }
        Ok(())
    }
}
async fn terminate(admin: &PgPool, pid: i32) {
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .fetch_one(admin)
            .await
            .unwrap()
    );
}
fn assert_connection_failure(error: &PersistenceError) {
    match error {
        PersistenceError::ConnectionError(source) | PersistenceError::UnknownError(source) => assert!(source.is::<sqlx::Error>()),
        _ => panic!("SQL interruption must remain technical: {error:?}"),
    }
}
async fn interrupted<R: PersistedEventRepository>(repo: R, admin: &PgPool, pool: &PgPool, name: &str, all: bool) {
    let mut stream = open(&repo, all).await;
    let first = tokio::time::timeout(Duration::from_secs(10), stream.next::<TestAggregate>(&[]))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.sequence, 1);
    assert_eq!(first.aggregate_id, "fault");
    assert_eq!(first.metadata["padding"].len(), 1024 * 1024);
    // Backpressure leaves PostgreSQL in an observable ClientWrite SQL wait.
    // Kill only after a complete first event has been received.
    let pid = blocked_pid(admin, name, "ClientWrite").await;
    terminate(admin, pid).await;
    let mut delivered = vec![1];
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match stream.next::<TestAggregate>(&[]).await {
                Some(Ok(event)) => {
                    assert_eq!(event.sequence, delivered.len() + 1);
                    delivered.push(event.sequence);
                }
                Some(Err(error)) => {
                    assert_connection_failure(&error);
                    break;
                }
                None => panic!("a terminated SQL backend must not become clean EOF"),
            }
        }
    })
    .await
    .unwrap();
    assert!(stream.next::<TestAggregate>(&[]).await.is_none());
    let mut abandoned = open(&repo, all).await;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), abandoned.next::<TestAggregate>(&[]))
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .sequence,
        1
    );
    let abandoned_pid = blocked_pid(admin, name, "ClientWrite").await;
    drop(abandoned);
    tokio::time::timeout(Duration::from_secs(5), sqlx::query("SELECT 1").execute(pool))
        .await
        .unwrap()
        .unwrap();
    // A closed socket may already have terminated this backend.
    let _: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(abandoned_pid)
        .fetch_one(admin)
        .await
        .unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let first = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let reports = Arc::new(AtomicUsize::new(0));
    let mut replay = EventReplay::new(
        repo,
        PauseQuery {
            seen: seen.clone(),
            first: first.clone(),
            release: release.clone(),
        },
    );
    replay.set_error_observer(Box::new({
        let reports = reports.clone();
        move |error| {
            assert_connection_failure(error);
            reports.fetch_add(1, Ordering::SeqCst);
        }
    }));
    let run = async {
        if all {
            replay.replay_all().await
        } else {
            replay.replay("fault").await
        }
    };
    let control = async {
        first.notified().await;
        let pid = blocked_pid(admin, name, "ClientWrite").await;
        terminate(admin, pid).await;
        release.notify_one();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(20), async { tokio::join!(run, control) })
        .await
        .unwrap();
    assert_connection_failure(&result.unwrap_err());
    assert_eq!(reports.load(Ordering::SeqCst), 1);
    let seen = seen.lock().unwrap().clone();
    assert!(!seen.is_empty());
    assert_eq!(seen, (1..=seen.len()).collect::<Vec<_>>());
    tokio::time::timeout(Duration::from_secs(5), sqlx::query("SELECT 1").execute(pool))
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn backend_failure_after_delivered_events_is_terminal_for_streams_and_replay() {
    let db = Database::new().await;
    let padding = "x".repeat(1024 * 1024);
    let events: Vec<_> = (1..=64)
        .map(|sequence| {
            let mut event = event("fault", sequence);
            event.metadata = serde_json::json!({"padding":padding});
            event
        })
        .collect();
    PostgresEventRepository::new(db.pool.clone())
        .persist::<TestAggregate>(&events, None)
        .await
        .unwrap();
    drop(events);
    for outbox in [false, true] {
        for all in [false, true] {
            let name = format!("stream-interrupt-{outbox}-{all}");
            let pool = reader_pool(&db.pool, &name).await;
            if outbox {
                interrupted(
                    PostgresEventRepositoryWithOutbox::new(pool.clone()).with_streaming_channel_size(1),
                    &db.pool,
                    &pool,
                    &name,
                    all,
                )
                .await;
            } else {
                interrupted(
                    PostgresEventRepository::new(pool.clone()).with_streaming_channel_size(1),
                    &db.pool,
                    &pool,
                    &name,
                    all,
                )
                .await;
            }
            tokio::time::timeout(Duration::from_secs(5), pool.close()).await.unwrap();
        }
    }
    db.close().await;
}
#[tokio::test]
async fn pool_acquisition_errors_are_also_terminal_stream_items() {
    let pool = PgPoolOptions::new().connect_lazy("postgres://localhost/unused").unwrap();
    pool.close().await;
    for all in [false, true] {
        terminal(
            PostgresEventRepository::new(pool.clone()).with_streaming_channel_size(1),
            all,
            false,
        )
        .await;
        terminal(
            PostgresEventRepositoryWithOutbox::new(pool.clone()).with_streaming_channel_size(1),
            all,
            false,
        )
        .await;
    }
}

mod contracts {
    struct DeltaUpcaster(&'static str, &'static str, fn(Value) -> Value);
    impl cqrsx::cqrs::store::upcaster::EventUpcaster for DeltaUpcaster {
        fn can_upcast(&self, kind: &str, version: &str) -> bool {
            kind == "delta" && version == self.0
        }
        fn upcast(&self, mut event: SerializedEvent) -> SerializedEvent {
            event.event_version = self.1.into();
            event.payload = (self.2)(event.payload);
            event
        }
    }

    struct CaptureEnvelopes(Arc<std::sync::Mutex<Vec<SerializedEvent>>>);
    #[async_trait::async_trait]
    impl cqrsx::cqrs::event_handler::EventHandler<Ledger> for CaptureEnvelopes {
        async fn handle(&self, id: &str, events: &[EventEnvelope<Ledger>]) -> Result<(), cqrsx::cqrs::store::PersistenceError> {
            for event in events {
                assert_eq!(id, event.aggregate_id);
                self.0.lock().unwrap().push(SerializedEvent::try_from(event).unwrap());
            }
            Ok(())
        }
    }

    use super::*;
    #[tokio::test]
    async fn replay_isolates_ids_and_types_checks_full_envelopes_and_decoding_errors() {
        let db = Database::new().await;
        let repo = PostgresEventRepository::new(db.pool.clone()).with_streaming_channel_size(1);
        let mut empty = repo.stream_events::<Ledger>("empty").await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), empty.next::<Ledger>(&[]))
                .await
                .unwrap()
                .is_none()
        );
        let mut expected = HashMap::new();
        for id in ["one", "two"] {
            let events: Vec<_> = (1..=5).map(|i| event_for(id, i, i as i32)).collect();
            repo.persist::<Ledger>(&events, None).await.unwrap();
            expected.insert(id.to_owned(), events);
        }
        repo.persist::<OtherLedger>(&[event_for("one", 1, 999)], None).await.unwrap();
        for id in ["one", "two"] {
            assert_eq!(repo.get_events::<Ledger>(id).await.unwrap(), expected[id]);
            assert_eq!(repo.get_events_after::<Ledger>(id, 3).await.unwrap(), expected[id][3..]);
        }
        assert_eq!(
            repo.get_events::<OtherLedger>("one").await.unwrap()[0].aggregate_type,
            OtherLedger::TYPE
        );
        let mut stream = repo.stream_all_events::<Ledger>().await.unwrap();
        let mut observed: HashMap<String, Vec<SerializedEvent>> = HashMap::new();
        while let Some(item) = tokio::time::timeout(Duration::from_secs(5), stream.next::<Ledger>(&[]))
            .await
            .unwrap()
        {
            let event = SerializedEvent::try_from(&item.unwrap()).unwrap();
            observed.entry(event.aggregate_id.clone()).or_default().push(event);
            tokio::task::yield_now().await;
        }
        assert_eq!(observed, expected);
        // The producer must release its connection when the capacity-one consumer closes early.
        drop(repo.stream_all_events::<Ledger>().await.unwrap());
        sqlx::query(
            "UPDATE events SET metadata = '{\"bad\":42}'::json WHERE aggregate_type=$1 AND aggregate_id='one' AND sequence=3",
        )
        .bind(Ledger::TYPE)
        .execute(&db.pool)
        .await
        .unwrap();
        let mut stream = repo.stream_events::<Ledger>("one").await.unwrap();
        for sequence in 1..=5 {
            let item = tokio::time::timeout(Duration::from_secs(5), stream.next::<Ledger>(&[]))
                .await
                .unwrap()
                .unwrap();
            if sequence == 3 {
                assert!(matches!(item, Err(PersistenceError::DeserializationError(_))));
            } else {
                assert_eq!(
                    SerializedEvent::try_from(&item.unwrap()).unwrap(),
                    expected["one"][sequence - 1]
                );
            }
        }
        assert!(stream.next::<Ledger>(&[]).await.is_none());
        sqlx::query("UPDATE events SET payload = '{}'::json WHERE aggregate_type=$1 AND aggregate_id='one' AND sequence=2")
            .bind(Ledger::TYPE)
            .execute(&db.pool)
            .await
            .unwrap();
        assert!(matches!(
            PersistedEventStore::<_, Ledger>::new_event_store(repo)
                .load_aggregate("one")
                .await,
            Err(cqrsx::cqrs::AggregateError::DeserializationError(_))
        ));
        db.close().await;
    }

    #[tokio::test]
    async fn stream_query_failure_is_reported_before_eof() {
        let db = Database::new().await;
        let repo = PostgresEventRepository::new(db.pool.clone()).with_tables("missing_events", "snapshots");
        let mut stream = repo.stream_events::<Ledger>("a").await.unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), stream.next::<Ledger>(&[]))
                .await
                .unwrap(),
            Some(Err(PersistenceError::UnknownError(_)))
        ));
        assert!(stream.next::<Ledger>(&[]).await.is_none());
        db.close().await;
    }

    #[tokio::test]
    async fn supported_upcaster_chain_preserves_full_envelopes_in_load_and_replay() {
        use cqrsx::cqrs::event_handler::replay::EventReplay;
        let db = Database::new().await;
        let mut original = event_for("upcast", 37, 3);
        original.event_version = "0".into();
        original.payload = json!({"old":3});
        original.metadata = json!({"trace":"данные"});
        let mut expected = original.clone();
        expected.event_version = "1".into();
        expected.payload = json!(4);
        let repo = PostgresEventRepository::new(db.pool.clone());
        repo.persist::<Ledger>(&[original], None).await.unwrap();
        let store = PersistedEventStore::<_, Ledger>::new_event_store(PostgresEventRepository::new(db.pool.clone()))
            .with_upcasters(delta_upcasters());
        let loaded = store.load_events("upcast").await.unwrap();
        assert_eq!(SerializedEvent::try_from(&loaded[0]).unwrap(), expected);
        assert_eq!(store.load_aggregate("upcast").await.unwrap().aggregate.0, [4]);
        for all in [false, true] {
            let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
            let replay = EventReplay::new(PostgresEventRepository::new(db.pool.clone()), CaptureEnvelopes(seen.clone()))
                .with_upcasters(delta_upcasters());
            if all {
                replay.replay_all().await.unwrap();
            } else {
                replay.replay("upcast").await.unwrap();
            }
            assert_eq!(*seen.lock().unwrap(), [expected.clone()]);
        }
        sqlx::query("UPDATE events SET event_version='unsupported'")
            .execute(&db.pool)
            .await
            .unwrap();
        assert!(matches!(
            store.load_aggregate("upcast").await,
            Err(cqrsx::cqrs::AggregateError::DeserializationError(_))
        ));
        let mut stream = repo.stream_events::<Ledger>("upcast").await.unwrap();
        assert!(matches!(
            stream.next::<Ledger>(&delta_upcasters()).await.unwrap(),
            Err(PersistenceError::DeserializationError(_))
        ));
        assert!(stream.next::<Ledger>(&[]).await.is_none());
        db.close().await;
    }

    fn delta_upcasters() -> Vec<Box<dyn cqrsx::cqrs::store::upcaster::EventUpcaster>> {
        vec![
            Box::new(DeltaUpcaster("0", "0.5", |v| v["old"].clone())),
            Box::new(DeltaUpcaster("0.5", "1", |v| json!(v.as_i64().unwrap() + 1))),
        ]
    }
}
