use super::*;
use cqrsx::cqrs::store::EventStore;
use cqrsx::cqrs::store::persisted::PersistedEventStore;
use cqrsx::{
    cqrs::command::{
        CommandExecutor, CommandGatewayError,
        causation::Causation,
        locks::LockLimits,
        observer::{CommandObserver, CommandReport, Outcome},
        planner::{Decision, TaskPlanError, TaskPlanner},
        retry::AggregateConflictRetryPolicy,
    },
    postgres::command::{PostgresCommandConfig, PostgresCommandExecutor},
};
use thiserror::Error;

#[derive(Clone, Default, Serialize, Deserialize)]
struct CommandAggregate {
    values: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum CommandEvent {
    Recorded { values: Vec<String> },
}

impl DomainEvent for CommandEvent {
    fn event_type(&self) -> String {
        "recorded".into()
    }

    fn event_version(&self) -> String {
        "1".into()
    }
}

#[derive(Clone)]
enum Command {
    Record { value: String, expected_count: usize },
    Reject,
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("rejected")]
struct Rejected;

impl Aggregate for CommandAggregate {
    const TYPE: &'static str = "command_factory_test";
    type Id = String;
    type Command = Command;
    type Event = CommandEvent;
    type Error = Rejected;
    type Rules = ();

    fn apply(&mut self, event: CommandEvent) {
        let CommandEvent::Recorded { values } = event;
        self.values = values;
    }
}

impl Decide<CommandAggregate> for Command {
    fn decide(self, aggregate: &CommandAggregate, _: &()) -> Result<CommandEvent, Rejected> {
        match self {
            Command::Record { value, expected_count } if aggregate.values.len() == expected_count => {
                let mut values = aggregate.values.clone();
                values.push(value);
                Ok(CommandEvent::Recorded { values })
            }
            _ => Err(Rejected),
        }
    }
}

/// The default retry policy and lock limits.
fn config(interval: usize) -> PostgresCommandConfig {
    PostgresCommandConfig::new(
        std::num::NonZeroUsize::new(interval).unwrap(),
        AggregateConflictRetryPolicy::default(),
        LockLimits::default(),
    )
}

fn record(value: &str, expected_count: usize) -> Command {
    Command::Record {
        value: value.to_owned(),
        expected_count,
    }
}

/// One task per decision: the value the command recorded.
struct LastValue;

impl TaskPlanner<CommandAggregate> for LastValue {
    fn plan(&self, decision: &Decision<'_, CommandAggregate>) -> Result<Vec<NewTask>, TaskPlanError> {
        let Some(value) = decision.after.values.last() else {
            return Ok(Vec::new());
        };
        Ok(vec![NewTask::new(DummyTaskV1 { value: value.clone() })?])
    }
}

fn planned_executor(pool: &PgPool, interval: usize) -> PostgresCommandExecutor<CommandAggregate> {
    PostgresCommandExecutor::new(pool.clone(), config(interval), ()).with_task_planner(LastValue)
}

/// The id the core derives for the first task of `causation`'s command on `aggregate_id`.
fn derived_id(causation: &Causation, aggregate_id: &str) -> Uuid {
    causation.task_id(CommandAggregate::TYPE, aggregate_id, &format(), 0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflicting_command_reloads_the_winner_before_deciding_and_publishes_only_its_tasks() {
    let db = Database::new().await;
    // Both commands must reach the database at once, so each goes through an executor of its own, like
    // two nodes: through one executor the second would wait for the first's lock.
    let nodes = [(); 2].map(|_| Arc::new(planned_executor(&db.pool, 1)));
    let mut gate = db.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE events IN SHARE MODE")
        .execute(&mut *gate)
        .await
        .unwrap();
    let mut calls = tokio::task::JoinSet::new();
    let mut causations = HashMap::new();
    for (value, executor) in ["left", "right"].into_iter().zip(nodes.clone()) {
        let causation = Causation::root(value);
        causations.insert(value, causation.clone());
        calls.spawn(async move {
            let result = executor
                .execute_with_metadata(&"contended".into(), record(value, 0), causation.event_metadata())
                .await;
            (value, result)
        });
    }
    wait_for_blocked_queries(&db.pool, "written_events", 2).await;
    gate.commit().await.unwrap();
    let mut winner = None;
    let mut rejected = 0;
    while let Some(result) = calls.join_next().await {
        let (value, result) = result.unwrap();
        match result {
            Ok(report) => {
                assert_eq!(report.tasks.len(), 1);
                assert!(winner.replace(value).is_none());
            }
            Err(CommandGatewayError::Domain(Rejected)) => rejected += 1,
            other => panic!("unexpected command result: {other:?}"),
        }
    }
    assert_eq!(rejected, 1, "the retry must decide against the committed aggregate state");
    let winner = winner.unwrap();
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let events = repository.events::<CommandAggregate>("contended").await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload, json!({"Recorded": {"values": [winner]}}));
    let snapshot = repository.snapshot::<CommandAggregate>("contended").await.unwrap().unwrap();
    assert_eq!(snapshot.last_sequence, 1);
    assert_eq!(snapshot.aggregate, json!({"values": [winner]}));
    let published: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM transactional_outbox")
        .fetch_all(&db.pool)
        .await
        .unwrap();
    let winning_id = derived_id(&causations[winner], "contended");
    assert_eq!(
        published,
        vec![winning_id],
        "the loser's re-planned attempt was rejected and published nothing"
    );
    assert_eq!(
        row(&db.pool, winning_id).await.get::<Value, _>("payload"),
        json!({ "value": winner })
    );
    db.close().await;
}

#[tokio::test]
async fn planned_work_publishes_atomically_and_rejections_write_nothing() {
    let db = Database::new().await;
    let executor = planned_executor(&db.pool, 1);
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let causation = Causation::root("integration");
    let report = executor
        .execute_with_metadata(&"accepted".into(), record("first", 0), causation.event_metadata())
        .await
        .unwrap();
    assert_eq!(report.events.iter().map(|event| event.sequence).collect::<Vec<_>>(), [1]);
    assert_eq!(
        report.tasks.iter().map(NewTask::id).collect::<Vec<_>>(),
        [derived_id(&causation, "accepted")]
    );
    let events = repository.events::<CommandAggregate>("accepted").await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload, json!({"Recorded": {"values": ["first"]}}));
    assert!(repository.snapshot::<CommandAggregate>("accepted").await.unwrap().is_some());
    let saved = row(&db.pool, derived_id(&causation, "accepted")).await;
    assert_eq!(saved.get::<String, _>("aggregate_id"), "accepted");
    assert_eq!(saved.get::<String, _>("aggregate_type"), CommandAggregate::TYPE);
    assert_eq!(saved.get::<i64, _>("aggregate_sequence"), 1);
    assert_eq!(saved.get::<Value, _>("payload"), json!({ "value": "first" }));
    assert_eq!(
        saved.get::<Value, _>("metadata"),
        json!({
            "command_id": causation.command_id.to_string(),
            "correlation_id": causation.correlation_id.to_string(),
            "causation_id": causation.causation_id.to_string(),
        }),
        "the core stamps the causation on every planned task"
    );

    assert_eq!(
        executor.execute(&"rejected".into(), Command::Reject).await,
        Err(CommandGatewayError::Domain(Rejected))
    );
    assert!(repository.events::<CommandAggregate>("rejected").await.unwrap().is_empty());
    assert!(repository.snapshot::<CommandAggregate>("rejected").await.unwrap().is_none());
    executor.execute(&"accepted".into(), record("second", 1)).await.unwrap();
    let rows: Vec<(String, i64, Value)> =
        sqlx::query_as("SELECT aggregate_id, aggregate_sequence, payload FROM transactional_outbox ORDER BY aggregate_sequence")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    assert_eq!(
        rows,
        [
            ("accepted".to_owned(), 1, json!({ "value": "first" })),
            ("accepted".to_owned(), 2, json!({ "value": "second" })),
        ],
        "one task per committed decision, none of the rejected one"
    );
    db.close().await;
}

/// What a report says: the id, the metadata and the ids and bodies of the tasks, or what became of the command.
type Heard = (String, HashMap<String, String>, Result<Vec<(Uuid, Value)>, String>);

/// Notes every report.
#[derive(Default)]
struct Reports(std::sync::Mutex<Vec<Heard>>);

impl CommandObserver<CommandAggregate> for Reports {
    fn command(&self, report: &CommandReport<'_, CommandAggregate>) {
        let outcome = match report.outcome {
            Outcome::Accepted { tasks } => Ok(tasks.iter().map(|task| (task.id(), task.payload().clone())).collect()),
            Outcome::Rejected(error) => Err(format!("rejected: {error}")),
            Outcome::Conflict => Err("conflict".to_owned()),
            Outcome::Unavailable => Err("unavailable".to_owned()),
        };
        self.0
            .lock()
            .unwrap()
            .push((report.aggregate_id.clone(), report.metadata.clone(), outcome));
    }
}

#[tokio::test]
async fn the_observer_reports_the_metadata_and_the_tasks_the_commit_stored() {
    let db = Database::new().await;
    let reports = Arc::new(Reports::default());
    let executor = planned_executor(&db.pool, 1).with_observer(reports.clone());
    let causation = Causation::root("integration");
    executor
        .execute_with_metadata(&"observed".into(), record("first", 0), causation.event_metadata())
        .await
        .unwrap();
    assert_eq!(
        executor.execute(&"observed".into(), Command::Reject).await,
        Err(CommandGatewayError::Domain(Rejected))
    );
    let events = PostgresEventRepositoryWithOutbox::new(db.pool.clone())
        .events::<CommandAggregate>("observed")
        .await
        .unwrap();
    let outbox: Vec<(Uuid, Value)> =
        sqlx::query_as("SELECT id, payload FROM transactional_outbox WHERE aggregate_id = 'observed'")
            .fetch_all(&db.pool)
            .await
            .unwrap();
    let reports = std::mem::take(&mut *reports.0.lock().unwrap());
    let [(id, metadata, accepted), rejected] = &reports[..] else {
        panic!("one report per command: {reports:?}");
    };
    assert_eq!((id.as_str(), json!(metadata)), ("observed", events[0].metadata.clone()));
    assert_eq!(accepted, &Ok(outbox), "the report's tasks are the outbox rows of the commit");
    assert_eq!(
        rejected,
        &("observed".to_owned(), HashMap::new(), Err("rejected: rejected".to_owned()))
    );
    db.close().await;
}

#[tokio::test]
async fn command_metadata_is_stored_with_the_event_and_defaults_to_an_empty_object() {
    let db = Database::new().await;
    let executor = PostgresCommandExecutor::<CommandAggregate>::new(db.pool.clone(), config(1), ());
    let metadata = HashMap::from([
        ("command_id".to_owned(), "cmd-1".to_owned()),
        ("source".to_owned(), "integration".to_owned()),
    ]);
    CommandExecutor::execute_with_metadata(&executor, &"metadata".into(), record("first", 0), metadata.clone())
        .await
        .unwrap();
    executor.execute(&"metadata".into(), record("second", 1)).await.unwrap();
    assert_eq!(
        executor
            .execute_with_metadata(&"metadata".into(), Command::Reject, metadata.clone())
            .await,
        Err(CommandGatewayError::Domain(Rejected))
    );
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let events = repository.events::<CommandAggregate>("metadata").await.unwrap();
    assert_eq!(events.len(), 2, "a rejected command commits neither event nor metadata");
    assert_eq!(events[0].metadata, json!({"command_id": "cmd-1", "source": "integration"}));
    assert_eq!(events[1].metadata, json!({}));
    db.close().await;
}

#[tokio::test]
async fn parallel_commands_publish_the_tasks_of_their_own_decisions() {
    let db = Database::new().await;
    let executor = Arc::new(planned_executor(&db.pool, 25));
    let mut commands = tokio::task::JoinSet::new();
    for index in 0..16 {
        let executor = executor.clone();
        commands.spawn(async move {
            let id = format!("parallel-{index}");
            let causation = Causation::root("parallel");
            let report = executor
                .execute_with_metadata(&id, record(&id, 0), causation.event_metadata())
                .await
                .unwrap();
            (id, causation, report)
        });
    }
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    while let Some(result) = commands.join_next().await {
        let (id, causation, report) = result.unwrap();
        assert_eq!(report.tasks[0].id(), derived_id(&causation, &id));
        let saved = row(&db.pool, derived_id(&causation, &id)).await;
        assert_eq!(saved.get::<String, _>("aggregate_id"), id);
        assert_eq!(saved.get::<String, _>("aggregate_type"), CommandAggregate::TYPE);
        assert_eq!(saved.get::<i64, _>("aggregate_sequence"), 1);
        assert_eq!(saved.get::<Value, _>("payload"), json!({ "value": id }));
        let events = repository.events::<CommandAggregate>(&id).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].payload, json!({"Recorded": {"values": [id]}}));
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 16);
    db.close().await;
}

#[tokio::test]
async fn configured_snapshot_interval_replays_snapshot_and_later_events() {
    let db = Database::new().await;
    let executor = PostgresCommandExecutor::<CommandAggregate>::new(db.pool.clone(), config(2), ());
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    executor.execute(&"snapshot".into(), record("first", 0)).await.unwrap();
    assert!(repository.snapshot::<CommandAggregate>("snapshot").await.unwrap().is_none());
    executor.execute(&"snapshot".into(), record("second", 1)).await.unwrap();
    let snapshot = repository.snapshot::<CommandAggregate>("snapshot").await.unwrap().unwrap();
    assert_eq!(snapshot.last_sequence, 2);
    assert_eq!(snapshot.aggregate, json!({"values": ["first", "second"]}));
    // The third and fourth calls require state from the snapshot, then its tail.
    executor.execute(&"snapshot".into(), record("third", 2)).await.unwrap();
    let store =
        PersistedEventStore::<_, CommandAggregate>::new(repository).with_snapshots(std::num::NonZeroUsize::new(2).unwrap());
    let context = store.load_aggregate("snapshot").await.unwrap();
    assert_eq!(context.current_sequence, 3);
    assert_eq!(context.aggregate.values, vec!["first", "second", "third"]);
    executor.execute(&"snapshot".into(), record("fourth", 3)).await.unwrap();
    let snapshot = PostgresEventRepositoryWithOutbox::new(db.pool.clone())
        .snapshot::<CommandAggregate>("snapshot")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.last_sequence, 4);
    assert_eq!(snapshot.aggregate, json!({"values": ["first", "second", "third", "fourth"]}));
    db.close().await;
}

#[tokio::test]
async fn the_constructor_needs_no_connection_and_closed_pool_errors_remain_unavailable() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://localhost/unused")
        .unwrap();
    let executor = PostgresCommandExecutor::<CommandAggregate>::new(pool.clone(), config(1), ());
    assert_eq!(pool.size(), 0);
    pool.close().await;
    assert_eq!(
        executor.execute(&"unused".into(), record("value", 0)).await,
        Err(CommandGatewayError::Unavailable)
    );
}

/// The cap lives in the rules: a value is accepted only while the stream is shorter.
#[derive(Clone)]
struct Cap(usize);

#[derive(Clone, Default, Serialize, Deserialize)]
struct CappedAggregate {
    values: Vec<String>,
}

impl Aggregate for CappedAggregate {
    const TYPE: &'static str = "capped_command_test";
    type Id = String;
    type Command = String;
    type Event = CommandEvent;
    type Error = Rejected;
    type Rules = Cap;

    fn apply(&mut self, event: CommandEvent) {
        let CommandEvent::Recorded { values } = event;
        self.values = values;
    }
}

impl Decide<CappedAggregate> for String {
    fn decide(self, aggregate: &CappedAggregate, cap: &Cap) -> Result<CommandEvent, Rejected> {
        if aggregate.values.len() >= cap.0 {
            return Err(Rejected);
        }
        let mut values = aggregate.values.clone();
        values.push(self);
        Ok(CommandEvent::Recorded { values })
    }
}

#[tokio::test]
async fn rules_decide_and_the_metadata_is_stored_with_every_event() {
    let db = Database::new().await;
    let executor = PostgresCommandExecutor::<CappedAggregate>::new(db.pool.clone(), config(10), Cap(2));
    let metadata = HashMap::from([("source".to_owned(), "test".to_owned())]);
    for value in ["first", "second"] {
        executor
            .execute_with_metadata(&"capped".into(), value.into(), metadata.clone())
            .await
            .unwrap();
    }
    assert_eq!(
        CommandExecutor::execute(&executor, &"capped".into(), "third".into()).await,
        Err(CommandGatewayError::Domain(Rejected)),
        "the cap from the rules rejects the third value"
    );
    CommandExecutor::execute(&executor, &"other".into(), "first".into())
        .await
        .unwrap();
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let capped = repository.events::<CappedAggregate>("capped").await.unwrap();
    assert_eq!(
        capped
            .iter()
            .map(|event| event.metadata["source"].as_str())
            .collect::<Vec<_>>(),
        [Some("test"), Some("test")]
    );
    let other = repository.events::<CappedAggregate>("other").await.unwrap();
    assert_eq!(other[0].metadata, json!({}));
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregate_locks_accept_forty_parallel_commands_without_conflicts() {
    let db = Database::new().await;
    let single_attempt = AggregateConflictRetryPolicy::try_new(1, Duration::ZERO, Duration::ZERO, 0.0).unwrap();
    let config = PostgresCommandConfig::new(
        std::num::NonZeroUsize::new(10).unwrap(),
        single_attempt,
        LockLimits::default(),
    );
    let executor = Arc::new(PostgresCommandExecutor::<CappedAggregate>::new(
        db.pool.clone(),
        config,
        Cap(40),
    ));
    let mut commands = tokio::task::JoinSet::new();
    for index in 0..40 {
        let executor = executor.clone();
        commands.spawn(async move { executor.execute(&"hot".into(), format!("v{index}")).await });
    }
    while let Some(result) = commands.join_next().await {
        assert!(result.unwrap().is_ok(), "without a conflict there is nothing to retry");
    }
    let events = PostgresEventRepositoryWithOutbox::new(db.pool.clone())
        .events::<CappedAggregate>("hot")
        .await
        .unwrap();
    assert_eq!(
        events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
        (1..=40).collect::<Vec<_>>()
    );
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_executors_with_their_own_locks_meet_only_in_the_database() {
    let db = Database::new().await;
    // Like two nodes: each executor has its own locks, the optimistic check decides between them.
    let nodes: Vec<_> = (0..2)
        .map(|_| {
            Arc::new(PostgresCommandExecutor::<CappedAggregate>::new(
                db.pool.clone(),
                config(10),
                Cap(40),
            ))
        })
        .collect();
    let mut commands = tokio::task::JoinSet::new();
    for index in 0..40 {
        let node = nodes[index % 2].clone();
        commands.spawn(async move { node.execute(&"shared".into(), format!("v{index}")).await });
    }
    let mut accepted = 0;
    while let Some(result) = commands.join_next().await {
        match result.unwrap() {
            Ok(_) => accepted += 1,
            Err(CommandGatewayError::Conflict) => {}
            other => panic!("unexpected result {other:?}"),
        }
    }
    let events = PostgresEventRepositoryWithOutbox::new(db.pool.clone())
        .events::<CappedAggregate>("shared")
        .await
        .unwrap();
    assert_eq!(events.len(), accepted);
    assert_eq!(
        events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
        (1..=accepted).collect::<Vec<_>>()
    );
    let Some(last) = events.last() else {
        panic!("at least one command wins")
    };
    let values = last.payload["Recorded"]["values"].as_array().unwrap();
    let unique: std::collections::BTreeSet<_> = values.iter().map(|value| value.as_str().unwrap()).collect();
    assert_eq!(
        (values.len(), unique.len()),
        (accepted, accepted),
        "every accepted value once"
    );
    db.close().await;
}

/// An aggregate whose rules cannot be cloned: the executor owns them, it never clones them per command.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Counted(Vec<String>);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Counted1(String);

impl DomainEvent for Counted1 {
    fn event_type(&self) -> String {
        "counted".into()
    }

    fn event_version(&self) -> String {
        "1".into()
    }
}

/// Counts the commands it decided; deliberately not `Clone`.
struct Counter(Arc<AtomicUsize>);

impl Aggregate for Counted {
    const TYPE: &'static str = "core_command_test";
    type Id = String;
    type Command = String;
    type Event = Counted1;
    type Error = std::convert::Infallible;
    type Rules = Counter;

    fn apply(&mut self, event: Counted1) {
        self.0.push(event.0);
    }
}

impl Decide<Counted> for String {
    fn decide(self, _: &Counted, counter: &Counter) -> Result<Counted1, std::convert::Infallible> {
        counter.0.fetch_add(1, Ordering::SeqCst);
        Ok(Counted1(self))
    }
}

/// One task per counted value.
struct CountedTasks;

impl TaskPlanner<Counted> for CountedTasks {
    fn plan(&self, decision: &Decision<'_, Counted>) -> Result<Vec<NewTask>, TaskPlanError> {
        let mut tasks = Vec::new();
        for Counted1(value) in decision.events {
            tasks.push(NewTask::new(DummyTaskV1 { value: value.clone() })?);
        }
        Ok(tasks)
    }
}

#[tokio::test]
async fn rules_that_are_not_clone_serve_the_executor_which_commits_event_snapshot_and_task_at_once() {
    let db = Database::new().await;
    let decided = Arc::new(AtomicUsize::new(0));
    let executor = PostgresCommandExecutor::<Counted>::new(db.pool.clone(), config(1), Counter(decided.clone()))
        .with_task_planner(CountedTasks);
    let causation = Causation::root("integration");
    let report = executor
        .execute_with_metadata(&"core".into(), "accepted".into(), causation.event_metadata())
        .await
        .unwrap();
    assert_eq!(decided.load(Ordering::SeqCst), 1);
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let events = repository.events::<Counted>("core").await.unwrap();
    assert_eq!(events.iter().map(|event| event.sequence).collect::<Vec<_>>(), [1]);
    let snapshot = repository.snapshot::<Counted>("core").await.unwrap().unwrap();
    assert_eq!((snapshot.aggregate, snapshot.last_sequence), (json!(["accepted"]), 1));
    let id = causation.task_id(Counted::TYPE, "core", &format(), 0);
    assert_eq!(report.tasks[0].id(), id);
    let saved = row(&db.pool, id).await;
    assert_eq!(saved.get::<String, _>("aggregate_id"), "core");
    assert_eq!(saved.get::<i64, _>("aggregate_sequence"), 1);
    db.close().await;
}

mod contracts {
    use crate::support::ledger::{Delta, Ledger, Rejected, event_for};
    struct CommitBoundary {
        inner: PersistedEventStore<PostgresEventRepository, Ledger>,
        pool: PgPool,
        mode: u8,
        loads: Arc<AtomicUsize>,
        commits: Arc<AtomicUsize>,
        durable: Arc<tokio::sync::Notify>,
    }
    impl EventStore<Ledger> for CommitBoundary {
        type Context = cqrsx::cqrs::store::persisted::EventStoreAggregateContext<Ledger>;
        async fn load_events(&self, id: &str) -> Result<Vec<EventEnvelope<Ledger>>, cqrsx::cqrs::AggregateError<Rejected>> {
            self.inner.load_events(id).await
        }
        async fn load_aggregate(&self, id: &str) -> Result<Self::Context, cqrsx::cqrs::AggregateError<Rejected>> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            self.inner.load_aggregate(id).await
        }
        async fn commit(
            &self,
            events: Vec<Delta>,
            context: Self::Context,
            metadata: HashMap<String, String>,
            tasks: &[NewTask],
        ) -> Result<Vec<EventEnvelope<Ledger>>, cqrsx::cqrs::AggregateError<Rejected>> {
            let attempt = self.commits.fetch_add(1, Ordering::SeqCst);
            if self.mode == 0 && attempt == 0 {
                PostgresEventRepository::new(self.pool.clone())
                    .persist::<Ledger>(&[event_for(&context.aggregate_id, context.current_sequence + 1, -7)], None)
                    .await
                    .unwrap();
            }
            let result = self.inner.commit(events, context, metadata, tasks).await?;
            if self.mode == 1 {
                return Err(cqrsx::cqrs::AggregateError::DatabaseConnectionError(
                    "response lost after real commit".into(),
                ));
            }
            if self.mode == 2 {
                self.durable.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(result)
        }
    }

    use super::*;
    #[tokio::test]
    async fn unrelated_unique_violations_are_technical_and_never_retry_commands() {
        let db = Database::new().await;
        sqlx::query("CREATE UNIQUE INDEX unrelated_unique ON events (event_type)")
            .execute(&db.pool)
            .await
            .unwrap();
        let repo = PostgresEventRepository::new(db.pool.clone());
        repo.persist::<Ledger>(&[event_for("a", 1, 1)], None).await.unwrap();
        let before = database_state(&db.pool).await;
        assert!(matches!(
            repo.persist::<Ledger>(&[event_for("b", 1, 2)], None).await,
            Err(PersistenceError::UnknownError(_))
        ));
        assert_eq!(database_state(&db.pool).await, before);
        use cqrsx::cqrs::command::{CommandGatewayError, executor::CommandExecutorCore};
        let loads = Arc::new(AtomicUsize::new(0));
        let commits = Arc::new(AtomicUsize::new(0));
        let executor = CommandExecutorCore::new(
            CqrsFramework::new(
                CommitBoundary {
                    inner: PersistedEventStore::new(PostgresEventRepository::new(db.pool.clone())),
                    pool: db.pool.clone(),
                    mode: 3,
                    loads: loads.clone(),
                    commits: commits.clone(),
                    durable: Arc::default(),
                },
                (),
            ),
            Default::default(),
            cqrsx::cqrs::command::locks::LockLimits::default(),
        );
        assert!(matches!(
            executor.execute(&"command".into(), 2).await,
            Err(CommandGatewayError::Unavailable)
        ));
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(commits.load(Ordering::SeqCst), 1);
        assert_eq!(database_state(&db.pool).await, before);
        db.close().await;
    }

    #[tokio::test]
    async fn command_retry_rechecks_balance_and_unknown_or_cancelled_response_keeps_durable_commit() {
        use cqrsx::cqrs::command::{CommandGatewayError, executor::CommandExecutorCore, retry::AggregateConflictRetryPolicy};
        let db = Database::new().await;
        for mode in 0..3 {
            let id = format!("boundary-{mode}");
            let repo = PostgresEventRepository::new(db.pool.clone());
            if mode == 0 {
                repo.persist::<Ledger>(&[event_for(&id, 1, 10)], None).await.unwrap();
            }
            let pool = db.pool.clone();
            let loads = Arc::new(AtomicUsize::new(0));
            let commits = Arc::new(AtomicUsize::new(0));
            let durable = Arc::new(tokio::sync::Notify::new());
            let executor = CommandExecutorCore::new(
                CqrsFramework::new(
                    CommitBoundary {
                        inner: PersistedEventStore::new(PostgresEventRepository::new(pool.clone())),
                        pool: pool.clone(),
                        mode,
                        loads: loads.clone(),
                        commits: commits.clone(),
                        durable: durable.clone(),
                    },
                    (),
                ),
                AggregateConflictRetryPolicy::try_new(3, Duration::ZERO, Duration::ZERO, 0.0).unwrap(),
                cqrsx::cqrs::command::locks::LockLimits::default(),
            );
            if mode == 2 {
                let id = id.clone();
                let task = tokio::spawn(async move { executor.execute(&id, 10).await });
                tokio::time::timeout(Duration::from_secs(10), durable.notified())
                    .await
                    .unwrap();
                task.abort();
                assert!(
                    tokio::time::timeout(Duration::from_secs(10), task)
                        .await
                        .unwrap()
                        .unwrap_err()
                        .is_cancelled()
                );
            } else {
                let result = executor.execute(&id, if mode == 0 { -7 } else { 10 }).await;
                if mode == 0 {
                    assert!(matches!(result, Err(CommandGatewayError::Domain(Rejected))));
                } else {
                    assert!(matches!(result, Err(CommandGatewayError::Unavailable)));
                }
            }
            assert_eq!(loads.load(Ordering::SeqCst), if mode == 0 { 2 } else { 1 });
            assert_eq!(commits.load(Ordering::SeqCst), 1);
            let context = PersistedEventStore::<_, Ledger>::new(repo).load_aggregate(&id).await.unwrap();
            assert_eq!(context.aggregate.0, if mode == 0 { vec![10, -7] } else { vec![10] });
            assert_eq!(context.current_sequence, if mode == 0 { 2 } else { 1 });
        }
        db.close().await;
    }
}
