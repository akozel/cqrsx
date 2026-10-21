use super::*;
use cqrsx::task::attempt::TaskAttempt;

#[path = "support/port_lease.rs"]
mod port_lease;
use port_lease::PortLease;

pub(super) struct Database {
    pub(super) pool: PgPool,
    container: ContainerAsync<GenericImage>,
    _port_lease: PortLease,
}
impl Database {
    pub(super) async fn new() -> Self {
        let port_lease = PortLease::acquire();
        let container = tokio::time::timeout(
            Duration::from_secs(60),
            GenericImage::new("docker.io/library/postgres", "18.6-alpine")
                .with_exposed_port(5432.tcp())
                .with_wait_for(WaitFor::Nothing)
                .with_mapped_port(port_lease.port(), 5432.tcp())
                .with_env_var("POSTGRES_DB", "outbox_test")
                .with_env_var("POSTGRES_USER", "outbox_test")
                .with_env_var("POSTGRES_PASSWORD", "outbox_test")
                .start(),
        )
        .await
        .expect("bounded PostgreSQL container startup")
        .unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432.tcp()).await.unwrap();
        let options = format!("postgresql://outbox_test:outbox_test@{host}:{port}/outbox_test")
            .parse::<sqlx::postgres::PgConnectOptions>()
            .unwrap()
            // This local fixture has no TLS. Keep early connection closes on the
            // retryable I/O path instead of SQLx's SSLRequest protocol error path.
            .ssl_mode(sqlx::postgres::PgSslMode::Disable)
            .options([("lock_timeout", "15000")]);
        // Poll the actual protocol endpoint: log delivery may lag under Podman.
        // This is startup readiness, not a retry of a failed test or command.
        let pool = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                match tokio::time::timeout(
                    Duration::from_secs(2),
                    connect_pool(
                        options.clone(),
                        ConnectionPingPolicy::Idle(DEFAULT_IDLE_PING_AFTER),
                        StatementTimeout::Custom(Duration::from_secs(30)),
                    ),
                )
                .await
                {
                    Ok(Ok(pool)) => break pool,
                    Ok(Err(sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut)) | Err(_) => {}
                    Ok(Err(sqlx::Error::Database(error))) if matches!(error.code().as_deref(), Some("57P03" | "3D000")) => {}
                    Ok(Err(error)) => panic!("PostgreSQL fixture startup connection failed: {error}"),
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("bounded PostgreSQL SQL readiness");
        sqlx::raw_sql(EVENT_STORE_SCHEMA).execute(&pool).await.unwrap();
        sqlx::raw_sql(TRANSACTIONAL_OUTBOX_SCHEMA).execute(&pool).await.unwrap();
        Self {
            pool,
            container,
            _port_lease: port_lease,
        }
    }
    pub(super) async fn close(self) {
        self.pool.close().await;
        self.container.stop().await.unwrap();
    }
}

/// A pool on this database whose sessions carry `timeout`, like the one a host shares between the
/// command executors and the task queue.
pub(super) async fn pool_with(db: &Database, pool: PgPoolOptions, timeout: StatementTimeout) -> PgPool {
    connect_pool_with(
        db.pool.connect_options().as_ref().clone(),
        pool,
        ConnectionPingPolicy::Default,
        timeout,
    )
    .await
    .unwrap()
}

pub(super) fn new_task() -> NewTask {
    NewTask::new(DummyTaskV1 {
        value: "runtime".to_owned(),
    })
    .unwrap()
}
pub(super) fn format() -> TaskFormat {
    TaskFormat::of::<DummyTaskV1>().unwrap()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct DummyTaskV1 {
    pub(super) value: String,
}

impl TaskPayload for DummyTaskV1 {
    const TASK_TYPE: &'static str = "dummy_task";
    const TASK_VERSION: i32 = 1;
}

pub(super) fn event(id: &str, sequence: usize) -> SerializedEvent {
    let payload = TestEvent::Recorded {
        value: format!("event-{sequence}"),
    };
    SerializedEvent::new(
        id.to_owned(),
        sequence,
        TestAggregate::TYPE.to_owned(),
        payload.event_type(),
        "1".to_owned(),
        serde_json::to_value(payload).unwrap(),
        json!({"correlation_id": "command"}),
    )
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(super) struct TestAggregate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) enum TestEvent {
    Recorded { value: String },
}

impl DomainEvent for TestEvent {
    fn event_type(&self) -> String {
        "recorded".into()
    }

    fn event_version(&self) -> String {
        "1".into()
    }
}

impl Aggregate for TestAggregate {
    const TYPE: &'static str = "runtime_test";
    type Command = ();
    type Event = TestEvent;
    type Error = std::convert::Infallible;
    type Services = ();

    async fn handle(&mut self, _: (), _: &(), _: &EventSink<Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn apply(&mut self, _: Self::Event) {}
}

pub(super) fn worker_event(id: &str) -> SerializedEvent {
    event(id, 1)
}

pub(super) async fn enqueue(pool: &PgPool, tasks: Vec<NewTask>) {
    let id = Uuid::new_v4().to_string();
    PostgresEventRepositoryWithOutbox::new(pool.clone())
        .with_outbox(tasks)
        .persist::<TestAggregate>(&[worker_event(&id)], None)
        .await
        .unwrap();
}

pub(super) struct TestAdapter {
    pub(super) outcome: Result<(), TaskHandlerError>,
}

impl TaskAdapter for TestAdapter {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
        self.outcome.clone()
    }
}

pub(super) fn adapter(outcome: Result<(), TaskHandlerError>) -> TestAdapter {
    TestAdapter { outcome }
}

pub(super) struct HangingAdapter;

impl TaskAdapter for HangingAdapter {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
        std::future::pending().await
    }
}

pub(super) struct CountingAdapter(pub(super) Arc<AtomicUsize>);

impl TaskAdapter for CountingAdapter {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn invoke(&mut self, _: &TaskAttempt) -> Result<(), TaskHandlerError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
pub(super) async fn row(pool: &PgPool, id: Uuid) -> sqlx::postgres::PgRow {
    sqlx::query("SELECT * FROM transactional_outbox WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}
pub(super) async fn make_ready(pool: &PgPool, id: Uuid) {
    sqlx::query("UPDATE transactional_outbox SET available_at = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}
pub(super) async fn expire(pool: &PgPool, id: Uuid) {
    sqlx::query("UPDATE transactional_outbox SET locked_until = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}
pub(super) async fn claim(queue: &PostgresTaskQueue) -> Vec<LeasedTask> {
    queue.claim(&format(), "test", 100, Duration::from_secs(60)).await.unwrap()
}

// Observe server-side contention before releasing the test's lock. A barrier
// alone cannot ensure that both SQL statements have reached PostgreSQL.
pub(super) async fn wait_for_blocked_queries(pool: &PgPool, query_fragment: &str, count: i64) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let blocked: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() \
                 AND pid <> pg_backend_pid() AND state = 'active' AND wait_event_type = 'Lock' \
                 AND strpos(query, $1) > 0",
            )
            .bind(query_fragment)
            .fetch_one(pool)
            .await
            .unwrap();
            if blocked >= count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("competing SQL statements must reach the held lock");
}

pub(super) async fn assert_worker_state(pool: &PgPool, id: Uuid, status: &str, attempts: i32, error: Option<&str>) {
    let saved = row(pool, id).await;
    assert_eq!(saved.get::<String, _>("status"), status);
    assert_eq!(saved.get::<i32, _>("attempts"), attempts);
    assert_eq!(saved.get::<Option<String>, _>("last_error").as_deref(), error);
    let processing = status == "processing";
    let finished = status == "completed" || status == "failed";
    assert_eq!(saved.get::<Option<String>, _>("locked_by").is_some(), processing);
    assert_eq!(saved.get::<Option<Uuid>, _>("lock_token").is_some(), processing);
    assert_eq!(saved.get::<Option<DateTime<Utc>>, _>("locked_until").is_some(), processing);
    assert_eq!(saved.get::<Option<DateTime<Utc>>, _>("finished_at").is_some(), finished);
}

pub(super) fn dispatcher<A: TaskAdapter>(adapter: A) -> TaskDispatcher<A> {
    TaskDispatcher::try_new(vec![adapter]).unwrap()
}

#[path = "support/ledger.rs"]
pub(crate) mod ledger;
#[path = "support/repository.rs"]
pub(crate) mod repository;

pub(crate) async fn database_state(pool: &PgPool) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object('events', (SELECT coalesce(jsonb_agg(to_jsonb(e) ORDER BY aggregate_type, aggregate_id, sequence), '[]') FROM events e), 'snapshots', (SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY aggregate_type, aggregate_id, last_sequence), '[]') FROM snapshots s), 'tasks', (SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY id), '[]') FROM transactional_outbox t))").fetch_one(pool).await.unwrap()
}
