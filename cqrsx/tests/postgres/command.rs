use super::*;
use cqrsx::cqrs::store::EventStore;
use cqrsx::cqrs::store::persisted::PersistedEventStore;
use cqrsx::{
    cqrs::{
        command::{
            CommandExecutor, CommandGatewayError, FailedHandler,
            retry::AggregateConflictRetryPolicy,
            turns::{CommandOrdering, TurnLimits},
        },
        event_handler::EventHandler,
    },
    postgres::command::{PostgresCommandConfig, PostgresCommandExecutor, command_executor},
};
use std::sync::Mutex;
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
    type Command = Command;
    type Event = CommandEvent;
    type Error = Rejected;
    type Services = ();

    async fn handle(&mut self, command: Command, _: &(), sink: &EventSink<Self>) -> Result<(), Rejected> {
        match command {
            Command::Record { value, expected_count } if self.values.len() == expected_count => {
                let mut values = self.values.clone();
                values.push(value);
                sink.write(CommandEvent::Recorded { values }, self).await;
                Ok(())
            }
            _ => Err(Rejected),
        }
    }

    fn apply(&mut self, event: CommandEvent) {
        let CommandEvent::Recorded { values } = event;
        self.values = values;
    }
}

/// Commands to one aggregate in turns, the default retry policy.
fn config(interval: usize) -> PostgresCommandConfig {
    PostgresCommandConfig::new(
        interval,
        AggregateConflictRetryPolicy::default(),
        CommandOrdering::PerAggregate(TurnLimits::default()),
    )
    .unwrap()
}

fn record(value: &str, expected_count: usize) -> Command {
    Command::Record {
        value: value.to_owned(),
        expected_count,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflicting_command_reloads_the_winner_before_deciding_and_publishes_no_losing_tasks() {
    let db = Database::new().await;
    // Both commands must reach the database at once: with turns the second would wait for the first.
    let concurrent = PostgresCommandConfig::new(1, AggregateConflictRetryPolicy::default(), CommandOrdering::Concurrent).unwrap();
    let executor = Arc::new(command_executor::<CommandAggregate>(db.pool.clone(), concurrent));
    let mut gate = db.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE events IN SHARE MODE")
        .execute(&mut *gate)
        .await
        .unwrap();
    let mut calls = tokio::task::JoinSet::new();
    let mut tasks = Vec::new();
    for value in ["left", "right"] {
        let executor = executor.clone();
        let task = NewTask::new(DummyTaskV1 { value: value.into() }).unwrap();
        tasks.push((value, task.clone()));
        calls.spawn(async move { (value, executor.execute("contended", record(value, 0), vec![task]).await) });
    }
    wait_for_blocked_queries(&db.pool, "written_events", 2).await;
    gate.commit().await.unwrap();
    let mut winner = None;
    let mut rejected = 0;
    while let Some(result) = calls.join_next().await {
        let (value, result) = result.unwrap();
        match result {
            Ok(()) => assert!(winner.replace(value).is_none()),
            Err(CommandGatewayError::Domain(Rejected)) => rejected += 1,
            other => panic!("unexpected command result: {other:?}"),
        }
    }
    assert_eq!(rejected, 1, "the retry must decide against the committed aggregate state");
    let winner = winner.unwrap();
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let events = repository.get_events::<CommandAggregate>("contended").await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload, json!({"Recorded": {"values": [winner]}}));
    let snapshot = repository
        .get_snapshot::<CommandAggregate>("contended")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.last_sequence, 1);
    assert_eq!(snapshot.aggregate, json!({"values": [winner]}));
    let published: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM transactional_outbox")
        .fetch_all(&db.pool)
        .await
        .unwrap();
    let (_, winning_task) = tasks.iter().find(|(value, _)| *value == winner).unwrap();
    assert_eq!(published, vec![winning_task.id()]);
    assert_eq!(
        &row(&db.pool, winning_task.id()).await.get::<Value, _>("payload"),
        winning_task.payload()
    );
    db.close().await;
}

#[tokio::test]
async fn factory_publishes_atomic_work_and_preserves_rejections_and_rollbacks() {
    let db = Database::new().await;
    let executor = command_executor::<CommandAggregate>(db.pool.clone(), config(1));
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let task = new_task();
    executor
        .execute("accepted", record("first", 0), vec![task.clone()])
        .await
        .unwrap();
    let events = repository.get_events::<CommandAggregate>("accepted").await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload, json!({"Recorded": {"values": ["first"]}}));
    assert!(
        repository
            .get_snapshot::<CommandAggregate>("accepted")
            .await
            .unwrap()
            .is_some()
    );
    let saved = row(&db.pool, task.id()).await;
    assert_eq!(saved.get::<String, _>("aggregate_id"), "accepted");
    assert_eq!(saved.get::<String, _>("aggregate_type"), CommandAggregate::TYPE);
    assert_eq!(saved.get::<i64, _>("aggregate_sequence"), 1);

    let rejected_task = new_task();
    assert_eq!(
        executor
            .execute("rejected", Command::Reject, vec![rejected_task.clone()])
            .await,
        Err(CommandGatewayError::Domain(Rejected))
    );
    assert!(
        repository
            .get_events::<CommandAggregate>("rejected")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        repository
            .get_snapshot::<CommandAggregate>("rejected")
            .await
            .unwrap()
            .is_none()
    );
    let rejected_count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox WHERE id = $1")
        .bind(rejected_task.id())
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(rejected_count, 0);

    // Reusing an existing task ID fails publication and rolls back event/snapshot.
    assert_eq!(
        executor.execute("rollback", record("uncommitted", 0), vec![task]).await,
        Err(CommandGatewayError::Unavailable)
    );
    assert!(
        repository
            .get_events::<CommandAggregate>("rollback")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        repository
            .get_snapshot::<CommandAggregate>("rollback")
            .await
            .unwrap()
            .is_none()
    );

    executor.execute("accepted", record("second", 1), vec![]).await.unwrap();
    assert_eq!(repository.get_events::<CommandAggregate>("accepted").await.unwrap().len(), 2);
    let task_count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(task_count, 1, "events-only execution must not reuse the previous task list");
    db.close().await;
}

#[tokio::test]
async fn command_metadata_is_stored_with_the_event_and_defaults_to_an_empty_object() {
    let db = Database::new().await;
    let executor = command_executor::<CommandAggregate>(db.pool.clone(), config(1));
    let metadata = HashMap::from([
        ("command_id".to_owned(), "cmd-1".to_owned()),
        ("source".to_owned(), "integration".to_owned()),
    ]);
    let task = new_task();
    CommandExecutor::execute_with_metadata(
        &executor,
        "metadata",
        record("first", 0),
        metadata.clone(),
        vec![task.clone()],
    )
    .await
    .unwrap();
    executor.execute("metadata", record("second", 1), vec![]).await.unwrap();
    assert_eq!(
        executor
            .execute_with_metadata("metadata", Command::Reject, metadata.clone(), vec![new_task()])
            .await,
        Err(CommandGatewayError::Domain(Rejected))
    );
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let events = repository.get_events::<CommandAggregate>("metadata").await.unwrap();
    assert_eq!(events.len(), 2, "a rejected command commits neither event nor metadata");
    assert_eq!(events[0].metadata, json!({"command_id": "cmd-1", "source": "integration"}));
    assert_eq!(events[1].metadata, json!({}));
    assert_eq!(row(&db.pool, task.id()).await.get::<i64, _>("aggregate_sequence"), 1);
    db.close().await;
}

#[tokio::test]
async fn shared_executor_isolates_parallel_command_task_lists() {
    let db = Database::new().await;
    let executor = Arc::new(command_executor::<CommandAggregate>(db.pool.clone(), config(25)));
    let mut commands = tokio::task::JoinSet::new();
    for index in 0..16 {
        let executor = executor.clone();
        commands.spawn(async move {
            let id = format!("parallel-{index}");
            let task = NewTask::new(DummyTaskV1 { value: id.clone() }).unwrap();
            executor.execute(&id, record(&id, 0), vec![task.clone()]).await.unwrap();
            (id, task)
        });
    }
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    while let Some(result) = commands.join_next().await {
        let (id, task) = result.unwrap();
        let saved = row(&db.pool, task.id()).await;
        assert_eq!(saved.get::<String, _>("aggregate_id"), id);
        assert_eq!(saved.get::<String, _>("aggregate_type"), CommandAggregate::TYPE);
        assert_eq!(saved.get::<i64, _>("aggregate_sequence"), 1);
        assert_eq!(&saved.get::<Value, _>("payload"), task.payload());
        let events = repository.get_events::<CommandAggregate>(&id).await.unwrap();
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
    let executor = command_executor::<CommandAggregate>(db.pool.clone(), config(2));
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    executor.execute("snapshot", record("first", 0), vec![]).await.unwrap();
    assert!(
        repository
            .get_snapshot::<CommandAggregate>("snapshot")
            .await
            .unwrap()
            .is_none()
    );
    executor.execute("snapshot", record("second", 1), vec![]).await.unwrap();
    let snapshot = repository
        .get_snapshot::<CommandAggregate>("snapshot")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.last_sequence, 2);
    assert_eq!(snapshot.aggregate, json!({"values": ["first", "second"]}));
    // The third and fourth calls require state from the snapshot, then its tail.
    executor.execute("snapshot", record("third", 2), vec![]).await.unwrap();
    let store = PersistedEventStore::<_, CommandAggregate>::new_snapshot_store(repository, 2);
    let context = store.load_aggregate("snapshot").await.unwrap();
    assert_eq!(context.current_sequence, 3);
    assert_eq!(context.aggregate.values, vec!["first", "second", "third"]);
    executor.execute("snapshot", record("fourth", 3), vec![]).await.unwrap();
    let snapshot = PostgresEventRepositoryWithOutbox::new(db.pool.clone())
        .get_snapshot::<CommandAggregate>("snapshot")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.last_sequence, 4);
    assert_eq!(snapshot.aggregate, json!({"values": ["first", "second", "third", "fourth"]}));
    db.close().await;
}

#[tokio::test]
async fn constructors_need_no_connection_and_closed_pool_errors_remain_unavailable() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://localhost/unused")
        .unwrap();
    let named = PostgresCommandExecutor::<CommandAggregate>::new(pool.clone(), config(1), ());
    let factory = command_executor::<CommandAggregate>(pool.clone(), config(2));
    assert_eq!(pool.size(), 0);
    pool.close().await;
    assert_eq!(
        CommandExecutor::execute(&named, "unused", record("value", 0), vec![]).await,
        Err(CommandGatewayError::Unavailable)
    );
    assert_eq!(
        factory.execute("unused", record("value", 0), vec![]).await,
        Err(CommandGatewayError::Unavailable)
    );
}

/// The cap lives in the services: a value is accepted only while the stream is shorter.
#[derive(Clone)]
struct Cap(usize);

#[derive(Clone, Default, Serialize, Deserialize)]
struct CappedAggregate {
    values: Vec<String>,
}

impl Aggregate for CappedAggregate {
    const TYPE: &'static str = "capped_command_test";
    type Command = String;
    type Event = CommandEvent;
    type Error = Rejected;
    type Services = Cap;

    async fn handle(&mut self, value: String, cap: &Cap, sink: &EventSink<Self>) -> Result<(), Rejected> {
        if self.values.len() >= cap.0 {
            return Err(Rejected);
        }
        let mut values = self.values.clone();
        values.push(value);
        sink.write(CommandEvent::Recorded { values }, self).await;
        Ok(())
    }

    fn apply(&mut self, event: CommandEvent) {
        let CommandEvent::Recorded { values } = event;
        self.values = values;
    }
}

/// Records every committed batch it sees; fails after recording when `fail` is set.
#[derive(Default)]
struct Recorder {
    name: &'static str,
    fail: bool,
    seen: Mutex<Vec<(String, usize, Option<String>)>>,
}

#[async_trait::async_trait]
impl EventHandler<CappedAggregate> for Recorder {
    fn name(&self) -> String {
        self.name.to_owned()
    }

    async fn handle(&self, id: &str, events: &[EventEnvelope<CappedAggregate>]) -> Result<(), PersistenceError> {
        for event in events {
            let source = event.metadata.get("source").cloned();
            self.seen.lock().unwrap().push((id.to_owned(), event.sequence, source));
        }
        if self.fail {
            return Err(PersistenceError::OptimisticLockError);
        }
        Ok(())
    }
}

#[tokio::test]
async fn services_decide_and_one_shared_handler_sees_every_committed_event() {
    let db = Database::new().await;
    let recorder = Arc::new(Recorder {
        name: "recorder",
        ..Recorder::default()
    });
    let executor =
        PostgresCommandExecutor::<CappedAggregate>::new(db.pool.clone(), config(10), Cap(2)).with_event_handler(recorder.clone());
    let metadata = HashMap::from([("source".to_owned(), "test".to_owned())]);
    for value in ["first", "second"] {
        executor
            .execute_with_metadata("capped", value.into(), metadata.clone(), vec![])
            .await
            .unwrap();
    }
    assert_eq!(
        CommandExecutor::execute(&executor, "capped", "third".into(), vec![]).await,
        Err(CommandGatewayError::Domain(Rejected)),
        "the cap from the services rejects the third value"
    );
    CommandExecutor::execute(&executor, "other", "first".into(), vec![])
        .await
        .unwrap();
    let source = Some("test".to_owned());
    assert_eq!(
        *recorder.seen.lock().unwrap(),
        [
            ("capped".to_owned(), 1, source.clone()),
            ("capped".to_owned(), 2, source),
            ("other".to_owned(), 1, None),
        ]
    );
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    assert_eq!(repository.get_events::<CappedAggregate>("capped").await.unwrap().len(), 2);
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_aggregate_turns_accept_forty_parallel_commands_without_conflicts() {
    let db = Database::new().await;
    let recorder = Arc::new(Recorder {
        name: "recorder",
        ..Recorder::default()
    });
    let single_attempt = AggregateConflictRetryPolicy::new(1, Duration::ZERO, Duration::ZERO, 0.0).unwrap();
    let config = PostgresCommandConfig::new(10, single_attempt, CommandOrdering::PerAggregate(TurnLimits::default())).unwrap();
    let executor = Arc::new(
        PostgresCommandExecutor::<CappedAggregate>::new(db.pool.clone(), config, Cap(40)).with_event_handler(recorder.clone()),
    );
    let mut commands = tokio::task::JoinSet::new();
    for index in 0..40 {
        let executor = executor.clone();
        commands.spawn(async move { executor.execute("hot", format!("v{index}"), vec![]).await });
    }
    while let Some(result) = commands.join_next().await {
        assert_eq!(result.unwrap(), Ok(()), "without a conflict there is nothing to retry");
    }
    let events = PostgresEventRepositoryWithOutbox::new(db.pool.clone())
        .get_events::<CappedAggregate>("hot")
        .await
        .unwrap();
    assert_eq!(
        events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
        (1..=40).collect::<Vec<_>>()
    );
    // Handlers run inside the turn, so they see one aggregate's events in commit order.
    let seen: Vec<usize> = recorder
        .seen
        .lock()
        .unwrap()
        .iter()
        .map(|(_, sequence, _)| *sequence)
        .collect();
    assert_eq!(seen, (1..=40).collect::<Vec<_>>());
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_executors_with_their_own_turns_meet_only_in_the_database() {
    let db = Database::new().await;
    // Like two nodes: each executor has its own turns, the optimistic check decides between them.
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
        commands.spawn(async move { node.execute("shared", format!("v{index}"), vec![]).await });
    }
    let mut accepted = 0;
    while let Some(result) = commands.join_next().await {
        match result.unwrap() {
            Ok(()) => accepted += 1,
            Err(CommandGatewayError::Conflict) => {}
            other => panic!("unexpected result {other:?}"),
        }
    }
    let events = PostgresEventRepositoryWithOutbox::new(db.pool.clone())
        .get_events::<CappedAggregate>("shared")
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

#[tokio::test]
async fn a_failed_handler_is_reported_by_name_after_the_event_and_its_tasks_are_committed() {
    let db = Database::new().await;
    let audit = Arc::new(Recorder {
        name: "audit",
        fail: true,
        ..Recorder::default()
    });
    let executor = PostgresCommandExecutor::<CappedAggregate>::new(db.pool.clone(), config(10), Cap(5))
        .with_event_handler(Arc::new(Recorder {
            name: "first",
            ..Recorder::default()
        }))
        .with_event_handler(audit.clone());
    let task = new_task();
    assert_eq!(
        executor.execute("audited", "value".into(), vec![task.clone()]).await,
        Err(CommandGatewayError::CommittedWithEventHandlerFailure {
            last_sequence: 1,
            handlers: vec![FailedHandler {
                index: 1,
                name: "audit".into()
            }]
        })
    );
    assert_eq!(*audit.seen.lock().unwrap(), [("audited".to_owned(), 1, None)]);
    let events = PostgresEventRepositoryWithOutbox::new(db.pool.clone())
        .get_events::<CappedAggregate>("audited")
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    let saved = row(&db.pool, task.id()).await;
    assert_eq!(saved.get::<String, _>("aggregate_id"), "audited");
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
        type AC = cqrsx::cqrs::store::persisted::EventStoreAggregateContext<Ledger>;
        async fn load_events(&self, id: &str) -> Result<Vec<EventEnvelope<Ledger>>, cqrsx::cqrs::AggregateError<Rejected>> {
            self.inner.load_events(id).await
        }
        async fn load_aggregate(&self, id: &str) -> Result<Self::AC, cqrsx::cqrs::AggregateError<Rejected>> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            self.inner.load_aggregate(id).await
        }
        async fn commit(
            &self,
            events: Vec<Delta>,
            context: Self::AC,
            metadata: HashMap<String, String>,
        ) -> Result<Vec<EventEnvelope<Ledger>>, cqrsx::cqrs::AggregateError<Rejected>> {
            let attempt = self.commits.fetch_add(1, Ordering::SeqCst);
            if self.mode == 0 && attempt == 0 {
                PostgresEventRepository::new(self.pool.clone())
                    .persist::<Ledger>(&[event_for(&context.aggregate_id, context.current_sequence + 1, -7)], None)
                    .await
                    .unwrap();
            }
            let result = self.inner.commit(events, context, metadata).await?;
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
        use cqrsx::cqrs::command::{CommandGatewayError, executor::CqrsCommandExecutor};
        let loads = Arc::new(AtomicUsize::new(0));
        let commits = Arc::new(AtomicUsize::new(0));
        let executor = CqrsCommandExecutor::new(
            {
                let pool = db.pool.clone();
                let loads = loads.clone();
                let commits = commits.clone();
                move |_| {
                    CqrsFramework::new(
                        CommitBoundary {
                            inner: PersistedEventStore::new_event_store(PostgresEventRepository::new(pool.clone())),
                            pool: pool.clone(),
                            mode: 3,
                            loads: loads.clone(),
                            commits: commits.clone(),
                            durable: Arc::default(),
                        },
                        vec![],
                        (),
                    )
                }
            },
            Default::default(),
        );
        assert!(matches!(
            executor.execute("command", vec![2], vec![]).await,
            Err(CommandGatewayError::Unavailable)
        ));
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert_eq!(commits.load(Ordering::SeqCst), 1);
        assert_eq!(database_state(&db.pool).await, before);
        db.close().await;
    }

    #[tokio::test]
    async fn command_retry_rechecks_balance_and_unknown_or_cancelled_response_keeps_durable_commit() {
        use cqrsx::cqrs::command::{CommandGatewayError, executor::CqrsCommandExecutor, retry::AggregateConflictRetryPolicy};
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
            let executor = CqrsCommandExecutor::new(
                {
                    let loads = loads.clone();
                    let commits = commits.clone();
                    let durable = durable.clone();
                    move |_| {
                        CqrsFramework::new(
                            CommitBoundary {
                                inner: PersistedEventStore::new_event_store(PostgresEventRepository::new(pool.clone())),
                                pool: pool.clone(),
                                mode,
                                loads: loads.clone(),
                                commits: commits.clone(),
                                durable: durable.clone(),
                            },
                            vec![],
                            (),
                        )
                    }
                },
                AggregateConflictRetryPolicy::new(3, Duration::ZERO, Duration::ZERO, 0.0).unwrap(),
            );
            if mode == 2 {
                let id = id.clone();
                let task = tokio::spawn(async move { executor.execute(&id, vec![10], vec![]).await });
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
                let result = executor
                    .execute(&id, if mode == 0 { vec![-7] } else { vec![10] }, vec![])
                    .await;
                if mode == 0 {
                    assert!(matches!(result, Err(CommandGatewayError::Domain(Rejected))));
                } else {
                    assert!(matches!(result, Err(CommandGatewayError::Unavailable)));
                }
            }
            assert_eq!(loads.load(Ordering::SeqCst), if mode == 0 { 2 } else { 1 });
            assert_eq!(commits.load(Ordering::SeqCst), 1);
            let context = PersistedEventStore::<_, Ledger>::new_event_store(repo)
                .load_aggregate(&id)
                .await
                .unwrap();
            assert_eq!(context.aggregate.0, if mode == 0 { vec![10, -7] } else { vec![10] });
            assert_eq!(context.current_sequence, if mode == 0 { 2 } else { 1 });
        }
        db.close().await;
    }
}
