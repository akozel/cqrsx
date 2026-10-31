use super::*;

/// The database of one test: a PostgreSQL container with the cqrsx schemas.
pub(super) type Database = crate::postgres_container::Postgres;

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
    NewTask::try_new(DummyTaskV1 {
        value: "runtime".to_owned(),
    })
    .unwrap()
}
pub(super) fn format() -> TaskFormat {
    TaskFormat::of::<DummyTaskV1>()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct DummyTaskV1 {
    pub(super) value: String,
}

impl Task for DummyTaskV1 {
    const TASK_TYPE: &'static str = "dummy_task";
    const TASK_VERSION: i32 = 1;
}

pub(super) fn event(id: &str, sequence: usize) -> SerializedEvent {
    let payload = TestEvent::Recorded {
        value: format!("event-{sequence}"),
    };
    SerializedEvent {
        aggregate_id: id.to_owned(),
        sequence,
        aggregate_type: TestAggregate::TYPE.to_owned(),
        event_type: payload.event_type().to_owned(),
        event_version: "1".to_owned(),
        payload: serde_json::to_value(payload).unwrap(),
        metadata: json!({"correlation_id": "command"}),
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(super) struct TestAggregate;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) enum TestEvent {
    Recorded { value: String },
}

impl Events for TestEvent {
    fn event_type(&self) -> &'static str {
        "recorded"
    }

    fn event_version(&self) -> &'static str {
        "1"
    }
}

impl Aggregate for TestAggregate {
    const TYPE: &'static str = "runtime_test";
    type Id = String;
    type Command = ();
    type Event = TestEvent;
    type Error = std::convert::Infallible;
    type Rules = ();

    fn apply(&mut self, _: Self::Event) {}
}

impl Decide<TestAggregate> for () {
    fn decide(self, _: &TestAggregate, _: &()) -> Result<TestEvent, std::convert::Infallible> {
        Ok(TestEvent::Recorded { value: String::new() })
    }
}

pub(super) fn worker_event(id: &str) -> SerializedEvent {
    event(id, 1)
}

pub(super) async fn enqueue(pool: &PgPool, tasks: Vec<NewTask>) {
    let id = Uuid::new_v4().to_string();
    PostgresEventRepositoryWithOutbox::new(pool.clone())
        .persist_with_tasks::<TestAggregate>(&[worker_event(&id)], None, &tasks)
        .await
        .unwrap();
}

pub(super) struct TestAdapter {
    pub(super) outcome: TaskResult,
}

impl TaskAdapter for TestAdapter {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn invoke(&self, _: &TaskContext) -> TaskResult {
        self.outcome.clone()
    }
}

pub(super) fn adapter(outcome: TaskResult) -> TestAdapter {
    TestAdapter { outcome }
}

pub(super) struct HangingAdapter;

impl TaskAdapter for HangingAdapter {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn invoke(&self, _: &TaskContext) -> TaskResult {
        std::future::pending().await
    }
}

pub(super) struct CountingAdapter(pub(super) Arc<AtomicUsize>);

impl TaskAdapter for CountingAdapter {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn invoke(&self, _: &TaskContext) -> TaskResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        TaskResult::Done
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
