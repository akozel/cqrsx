//! Tasks planned from decisions on PostgreSQL: every published task matches the event it was committed
//! with, whatever races the commands ran into, and every refused plan writes nothing.
use super::*;
use cqrsx::{
    command::{
        CommandExecutor, CommandGatewayError,
        causation::Causation,
        executor::CommandExecutorCore,
        locks::LockLimits,
        planner::{Decision, TaskPlanError, TaskPlanner},
        retry::AggregateConflictRetryPolicy,
    },
    postgres::command::{PostgresCommandConfig, PostgresCommandExecutor},
};
use thiserror::Error;

/// Holds up to its capacity of items, which the rules give.
#[derive(Clone, Default, Serialize, Deserialize)]
struct Shelf {
    items: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Put {
    item: String,
}

impl Events for Put {
    fn event_type(&self) -> &'static str {
        "put"
    }

    fn event_version(&self) -> &'static str {
        "1"
    }
}

#[derive(Clone)]
enum ShelfCommand {
    Put(String),
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("full")]
struct Full;

impl Aggregate for Shelf {
    const TYPE: &'static str = "planning_shelf";
    type Id = String;
    type Command = ShelfCommand;
    type Event = Put;
    type Error = Full;
    type Rules = usize;

    fn apply(&mut self, Put { item }: Put) {
        self.items.push(item);
    }
}

impl Decide<Shelf> for ShelfCommand {
    fn decide(self, shelf: &Shelf, capacity: &usize) -> Result<Put, Full> {
        let ShelfCommand::Put(item) = self;
        if shelf.items.len() >= *capacity {
            return Err(Full);
        }
        Ok(Put { item })
    }
}

/// Plans `per_item` tasks carrying the item of every put; refuses the item `refuse`.
#[derive(Clone)]
struct ShelfPlanner {
    per_item: usize,
    refuse: Option<&'static str>,
}

impl ShelfPlanner {
    fn one_per_item() -> Self {
        Self {
            per_item: 1,
            refuse: None,
        }
    }
}

impl TaskPlanner<Shelf> for ShelfPlanner {
    fn plan(&self, decision: &Decision<'_, Shelf>) -> Result<Vec<NewTask>, TaskPlanError> {
        let mut tasks = Vec::new();
        for Put { item } in decision.events {
            if self.refuse == Some(item.as_str()) {
                return Err(TaskPlanError::application("refused item"));
            }
            for _ in 0..self.per_item {
                tasks.push(NewTask::try_new(DummyTaskV1 { value: item.clone() })?);
            }
        }
        Ok(tasks)
    }
}

fn shelf_config(snapshot_interval: usize, attempts: usize) -> PostgresCommandConfig {
    let retry = AggregateConflictRetryPolicy::try_new(attempts, Duration::ZERO, Duration::ZERO, 0.0).unwrap();
    PostgresCommandConfig::new(
        std::num::NonZeroUsize::new(snapshot_interval).unwrap(),
        retry,
        LockLimits::default(),
    )
}

fn shelves(pool: &PgPool, snapshot_interval: usize, attempts: usize, planner: ShelfPlanner) -> PostgresCommandExecutor<Shelf> {
    PostgresCommandExecutor::new(pool.clone(), shelf_config(snapshot_interval, attempts), 1_000).with_task_planner(planner)
}

fn put(item: impl Into<String>) -> ShelfCommand {
    ShelfCommand::Put(item.into())
}

/// Checks that every outbox row of shelf `id` joins the event it was committed with, carries that event's
/// item and has the id derived from that event's command; returns the numbers of events and of tasks.
async fn tasks_match_their_events(pool: &PgPool, id: &str) -> (usize, usize) {
    let events: Vec<(i64, Value, Value)> = sqlx::query_as(
        "SELECT sequence, payload::jsonb, metadata::jsonb FROM events WHERE aggregate_type = $1 AND aggregate_id = $2 ORDER BY sequence",
    )
    .bind(Shelf::TYPE)
    .bind(id)
    .fetch_all(pool)
    .await
    .unwrap();
    let tasks: Vec<(Uuid, i64, Value)> = sqlx::query_as(
        "SELECT t.id, t.aggregate_sequence, t.payload FROM transactional_outbox t \
         WHERE t.aggregate_type = $1 AND t.aggregate_id = $2 ORDER BY t.aggregate_sequence",
    )
    .bind(Shelf::TYPE)
    .bind(id)
    .fetch_all(pool)
    .await
    .unwrap();
    for (task_id, sequence, payload) in &tasks {
        let (_, event, metadata) = events
            .iter()
            .find(|(event_sequence, _, _)| event_sequence == sequence)
            .unwrap_or_else(|| panic!("task {task_id} has no event {sequence}"));
        assert_eq!(payload["value"], event["item"], "the task carries its own event's item");
        let metadata: HashMap<String, String> = serde_json::from_value(metadata.clone()).unwrap();
        let causation = Causation::from_event_metadata(&metadata).unwrap().unwrap();
        assert_eq!(
            *task_id,
            causation.task_id(Shelf::TYPE, id, &format(), 0),
            "the id is the event's command's"
        );
    }
    (events.len(), tasks.len())
}

async fn wait_until_finished(pool: &PgPool, query_fragment: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let running: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() \
                 AND pid <> pg_backend_pid() AND state = 'active' AND strpos(query, $1) > 0",
            )
            .bind(query_fragment)
            .fetch_one(pool)
            .await
            .unwrap();
            if running == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the statement finishes once the gate opens");
}

async fn lock_events(pool: &PgPool) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut gate = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE events IN SHARE MODE")
        .execute(&mut *gate)
        .await
        .unwrap();
    gate
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_two_nodes_publish_only_the_tasks_of_their_committed_decisions() {
    let db = Database::new().await;
    // Like two nodes: each executor has its own locks, and only the optimistic check orders them.
    let nodes = [(); 2].map(|_| Arc::new(shelves(&db.pool, 10, 3, ShelfPlanner::one_per_item())));
    let gate = lock_events(&db.pool).await;
    let mut commands = JoinSet::new();
    for index in 0..40 {
        let node = nodes[index % 2].clone();
        commands.spawn(async move {
            let metadata = Causation::root("node").event_metadata();
            node.execute_with_metadata(&"shared".into(), put(format!("v{index}")), metadata)
                .await
        });
    }
    // Both nodes' first commands meet at the gate: one wins, the other conflicts and plans again.
    wait_for_blocked_queries(&db.pool, "written_events", 2).await;
    gate.commit().await.unwrap();
    let (mut accepted, mut conflicts) = (0, 0);
    while let Some(result) = commands.join_next().await {
        match result.unwrap() {
            Ok(report) => {
                assert_eq!(report.tasks.len(), 1);
                accepted += 1;
            }
            Err(CommandGatewayError::Conflict) => conflicts += 1,
            Err(other) => panic!("unexpected result {other:?}"),
        }
    }
    assert_eq!(accepted + conflicts, 40);
    let (events, tasks) = tasks_match_their_events(&db.pool, "shared").await;
    assert_eq!(
        (events, tasks),
        (accepted, accepted),
        "one task per committed event and no orphan"
    );
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forty_parallel_commands_through_one_executor_each_publish_their_own_task() {
    let db = Database::new().await;
    // One attempt: through one executor the commands of a key wait for its lock and never conflict.
    let executor = Arc::new(shelves(&db.pool, 10, 1, ShelfPlanner::one_per_item()));
    let mut commands = JoinSet::new();
    for index in 0..40 {
        let executor = executor.clone();
        commands.spawn(async move {
            let metadata = Causation::root("ui").event_metadata();
            executor
                .execute_with_metadata(&"hot".into(), put(format!("v{index}")), metadata)
                .await
        });
    }
    while let Some(result) = commands.join_next().await {
        assert_eq!(result.unwrap().unwrap().tasks.len(), 1);
    }
    assert_eq!(tasks_match_their_events(&db.pool, "hot").await, (40, 40));
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_a_dropped_command_commits_its_event_and_its_tasks_together_or_not_at_all() {
    let db = Database::new().await;
    let executor = Arc::new(shelves(&db.pool, 2, 1, ShelfPlanner::one_per_item()));
    for round in 0..4 {
        let gate = lock_events(&db.pool).await;
        let command = tokio::spawn({
            let executor = executor.clone();
            async move {
                let metadata = Causation::root("ui").event_metadata();
                executor
                    .execute_with_metadata(&"dropped".into(), put(format!("dropped-{round}")), metadata)
                    .await
            }
        });
        wait_for_blocked_queries(&db.pool, "written_events", 1).await;
        // The caller gives up while the statement waits: its outcome is unknown, but one statement.
        command.abort();
        assert!(command.await.unwrap_err().is_cancelled());
        gate.commit().await.unwrap();
        wait_until_finished(&db.pool, "written_events").await;
        let (events, tasks) = tasks_match_their_events(&db.pool, "dropped").await;
        assert_eq!(events, tasks, "round {round}: the event and its task, or neither");
        let metadata = Causation::root("ui").event_metadata();
        executor
            .execute_with_metadata(&"dropped".into(), put(format!("next-{round}")), metadata)
            .await
            .expect("the lock was released and the next command commits");
        let (events, tasks) = tasks_match_their_events(&db.pool, "dropped").await;
        assert_eq!(events, tasks);
    }
    db.close().await;
}

#[tokio::test]
async fn refused_plans_write_nothing_and_a_plan_without_tasks_needs_no_causation() {
    let db = Database::new().await;
    let planner = ShelfPlanner {
        per_item: 1,
        refuse: Some("bad"),
    };
    let executor = shelves(&db.pool, 1, 3, planner);
    let before = database_state(&db.pool).await;
    let metadata = Causation::root("ui").event_metadata();
    assert_eq!(
        executor
            .execute_with_metadata(&"s".into(), put("bad"), metadata.clone())
            .await,
        Err(CommandGatewayError::Unavailable),
        "a planner error refuses the command"
    );
    let malformed = HashMap::from([("command_id".to_owned(), "cmd-1".to_owned())]);
    assert_eq!(
        executor
            .execute_with_metadata(&"s".into(), put("good"), malformed.clone())
            .await,
        Err(CommandGatewayError::Unavailable),
        "a causation that does not parse refuses a decision with tasks"
    );
    assert_eq!(database_state(&db.pool).await, before);
    // A decision its planner plans no task for: the same causation does not stand in its way.
    let untasked = shelves(
        &db.pool,
        1,
        3,
        ShelfPlanner {
            per_item: 0,
            refuse: None,
        },
    );
    let report = untasked
        .execute_with_metadata(&"s".into(), put("quiet"), malformed)
        .await
        .unwrap();
    assert!(report.tasks.is_empty(), "and lets a decision without tasks through");
    assert_eq!(tasks_match_their_events(&db.pool, "s").await, (1, 0));
    db.close().await;
}

#[tokio::test]
async fn without_causation_a_task_keeps_its_random_id_and_carries_no_stamp() {
    let db = Database::new().await;
    let executor = shelves(&db.pool, 10, 1, ShelfPlanner::one_per_item());
    let report = executor.execute(&"s".into(), put("plain")).await.unwrap();
    let id = report.tasks[0].id();
    assert_eq!(id.get_version_num(), 4);
    let saved = row(&db.pool, id).await;
    assert_eq!(saved.get::<Value, _>("metadata"), json!({}));
    assert_eq!(saved.get::<Value, _>("payload"), json!({ "value": "plain" }));
    db.close().await;
}

#[tokio::test]
async fn the_same_command_id_twice_on_one_aggregate_publishes_once_and_writes_the_repeat_nowhere() {
    let db = Database::new().await;
    let executor = shelves(&db.pool, 10, 3, ShelfPlanner::one_per_item());
    let metadata = Causation::root("ui").event_metadata();
    executor
        .execute_with_metadata(&"s".into(), put("a"), metadata.clone())
        .await
        .unwrap();
    let before = database_state(&db.pool).await;
    // The aggregate accepts the second item, but its task has the first one's id: the outbox key refuses
    // the whole statement, the event included.
    assert_eq!(
        executor.execute_with_metadata(&"s".into(), put("b"), metadata.clone()).await,
        Err(CommandGatewayError::Unavailable)
    );
    assert_eq!(database_state(&db.pool).await, before);
    // The same command id on another aggregate is another task.
    executor.execute_with_metadata(&"t".into(), put("b"), metadata).await.unwrap();
    assert_eq!(tasks_match_their_events(&db.pool, "s").await, (1, 1));
    assert_eq!(tasks_match_their_events(&db.pool, "t").await, (1, 1));
    db.close().await;
}

#[tokio::test]
async fn a_plan_at_the_bind_limit_commits_and_one_more_task_is_refused_before_any_write() {
    let db = Database::new().await;
    // Seven binds of the event, eleven per task, and five or six of a snapshot below 65 535.
    for (interval, largest) in [(1_000, 5_957), (1, 5_956)] {
        let fits = shelves(
            &db.pool,
            interval,
            1,
            ShelfPlanner {
                per_item: largest,
                refuse: None,
            },
        );
        let id = format!("fits-{interval}");
        let report = fits
            .execute_with_metadata(&id, put("bulk"), Causation::root("ui").event_metadata())
            .await
            .unwrap();
        let ids: std::collections::HashSet<Uuid> = report.tasks.iter().map(NewTask::id).collect();
        assert_eq!(ids.len(), largest, "every task of one format has an id of its own");
        let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox WHERE aggregate_id = $1")
            .bind(&id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(stored, largest as i64);
        let too_many = shelves(
            &db.pool,
            interval,
            1,
            ShelfPlanner {
                per_item: largest + 1,
                refuse: None,
            },
        );
        let before = database_state(&db.pool).await;
        assert_eq!(
            too_many
                .execute_with_metadata(
                    &format!("over-{interval}"),
                    put("bulk"),
                    Causation::root("ui").event_metadata()
                )
                .await,
            Err(CommandGatewayError::Unavailable)
        );
        assert_eq!(database_state(&db.pool).await, before);
    }
    db.close().await;
}

#[tokio::test]
async fn a_store_without_an_outbox_refuses_planned_tasks_and_writes_nothing() {
    let db = Database::new().await;
    let store = || PersistedEventStore::<_, Shelf>::new(PostgresEventRepository::new(db.pool.clone()));
    let planned = CommandExecutorCore::new(
        CqrsFramework::new(store(), 10).with_task_planner(ShelfPlanner::one_per_item()),
        AggregateConflictRetryPolicy::default(),
        LockLimits::default(),
    );
    let before = database_state(&db.pool).await;
    assert_eq!(
        planned
            .execute_with_metadata(&"s".into(), put("a"), Causation::root("ui").event_metadata())
            .await,
        Err(CommandGatewayError::Unavailable)
    );
    assert_eq!(database_state(&db.pool).await, before);
    let unplanned = CommandExecutorCore::new(
        CqrsFramework::new(store(), 10),
        AggregateConflictRetryPolicy::default(),
        LockLimits::default(),
    );
    let report = CommandExecutor::execute(&unplanned, &"s".into(), put("a")).await.unwrap();
    assert_eq!((report.events.len(), report.tasks.len()), (1, 0));
    db.close().await;
}
