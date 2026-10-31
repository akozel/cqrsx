use cqrsx::journal::snapshot::SnapshotUpdate;
// Derived from postgres-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use super::support::Database;
use super::support::repository::*;
use crate::event;
use cqrsx::journal::PersistenceError;
use sqlx::postgres::PgPoolOptions;

// Characterization of the open snapshot issue, not the desired future contract.
// Change this expectation when repairing doc/known_issues/cqrs_snapshot_event_reapplication.md.
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
struct History {
    entries: Vec<i32>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Appended(i32);

impl cqrsx::domain::event::Events for Appended {
    fn event_type(&self) -> &'static str {
        "Appended"
    }
    fn event_version(&self) -> &'static str {
        "1"
    }
}

impl cqrsx::domain::aggregate::Aggregate for History {
    const TYPE: &'static str = "snapshot_reapplication_characterization";
    type Id = String;
    type Command = i32;
    type Event = Appended;
    type Error = std::convert::Infallible;
    type Rules = ();

    fn apply(&mut self, event: Appended) {
        self.entries.push(event.0);
    }
}

impl cqrsx::domain::command::Decide<History> for i32 {
    fn decide(self, _: &History, _: &()) -> Result<Appended, std::convert::Infallible> {
        Ok(Appended(self))
    }
}

#[tokio::test]
async fn snapshot_command_applies_one_event_once() {
    let db = Database::new().await;
    let executor = crate::support::executor::<History>(&db.pool, 1, ());
    executor.execute(&"history".into(), 7).await.unwrap();
    let store = PersistedEventStore::<_, History>::new(PostgresEventRepository::new(db.pool.clone()))
        .with_snapshots(std::num::NonZeroUsize::MIN);
    assert_eq!(store.load_events("history").await.unwrap().len(), 1);
    let context = store.load_aggregate("history").await.unwrap();
    assert_eq!(context.current_sequence, 1);
    assert_eq!(context.snapshot_version, Some(1));
    assert_eq!(context.aggregate.entries, vec![7]);
    db.close().await;
}
use cqrsx::journal::repository::PersistedEventRepository;
use cqrsx::postgres::event_repository::PostgresEventRepository;

#[tokio::test]
async fn custom_tables_keep_the_event_and_snapshot_contracts() {
    let db = Database::new().await;
    sqlx::raw_sql(
        "CREATE TABLE custom_events (LIKE events INCLUDING ALL); CREATE TABLE custom_snapshots (LIKE snapshots INCLUDING ALL)",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let repository = PostgresEventRepository::new(db.pool.clone()).with_tables("custom_events", "custom_snapshots");
    let mut event = test_serialized_event("custom", 1, &TestEvent::Created(Created { id: "custom".into() }));
    event.metadata = serde_json::json!({});
    let aggregate = serde_json::json!({"id": "custom", "description": "", "tests": []});
    repository
        .persist::<TestAggregate>(
            std::slice::from_ref(&event),
            Some(SnapshotUpdate {
                aggregate_id: "custom".into(),
                payload: aggregate.clone(),
                last_sequence: 1,
                next_version: 1,
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        repository.events::<TestAggregate>("custom").await.unwrap(),
        vec![event.clone()]
    );
    assert!(
        repository
            .events_after::<TestAggregate>("custom", 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        repository.snapshot::<TestAggregate>("custom").await.unwrap(),
        Some(serialized_snapshot("custom".into(), 1, 1, aggregate))
    );
    let default = PostgresEventRepository::new(db.pool.clone());
    assert!(default.events::<TestAggregate>("custom").await.unwrap().is_empty());
    db.close().await;
}

#[tokio::test]
async fn event_repositories() {
    let db = Database::new().await;
    let pool = db.pool.clone();
    let id = uuid::Uuid::new_v4().to_string();
    let event_repo: PostgresEventRepository = PostgresEventRepository::new(pool.clone());
    let events = event_repo.events::<TestAggregate>(&id).await.unwrap();
    assert!(events.is_empty());

    event_repo
        .persist::<TestAggregate>(
            &[
                test_serialized_event(&id, 1, &TestEvent::Created(Created { id: id.clone() })),
                test_serialized_event(
                    &id,
                    2,
                    &TestEvent::Tested(Tested {
                        test_name: "a test was run".to_string(),
                    }),
                ),
            ],
            None,
        )
        .await
        .unwrap();
    let events = event_repo.events::<TestAggregate>(&id).await.unwrap();
    assert_eq!(2, events.len());
    events.iter().for_each(|e| assert_eq!(&id, &e.aggregate_id));

    // Optimistic lock error
    let result = event_repo
        .persist::<TestAggregate>(
            &[
                test_serialized_event(
                    &id,
                    3,
                    &TestEvent::SomethingElse(SomethingElse {
                        description: "this should not persist".to_string(),
                    }),
                ),
                test_serialized_event(
                    &id,
                    2,
                    &TestEvent::SomethingElse(SomethingElse {
                        description: "bad sequence number".to_string(),
                    }),
                ),
            ],
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(result, PersistenceError::OptimisticLockError),
        "invalid error result found during insert: {result}"
    );

    let events = event_repo.events::<TestAggregate>(&id).await.unwrap();
    assert_eq!(2, events.len());

    db.close().await;
}

#[tokio::test]
async fn events_after_every_watermark_share_one_prepared_statement() {
    let db = Database::new().await;
    // One connection: every read below runs in the session whose prepared statements are listed.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(db.pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let repository = PostgresEventRepository::new(pool.clone());
    let id = uuid::Uuid::new_v4().to_string();
    let events: Vec<_> = (1..=12).map(|sequence| event(&id, sequence)).collect();
    repository.persist::<TestAggregate>(&events, None).await.unwrap();
    for after in 0..10 {
        let found = repository.events_after::<TestAggregate>(&id, after).await.unwrap();
        let sequences: Vec<_> = found.iter().map(|event| event.sequence).collect();
        assert_eq!(
            sequences,
            ((after + 1)..=12).collect::<Vec<_>>(),
            "exclusive lower bound {after}"
        );
    }
    // A watermark spliced into the text would leave one statement per watermark here.
    let prepared: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_prepared_statements
          WHERE statement LIKE '%sequence > %' AND statement NOT LIKE '%pg_prepared_statements%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(prepared, 1);
    let bound: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_prepared_statements
          WHERE statement LIKE '%AND sequence > $3%' AND statement NOT LIKE '%pg_prepared_statements%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(bound, 1);
    pool.close().await;
    db.close().await;
}

#[tokio::test]
async fn snapshot_repositories() {
    let db = Database::new().await;
    let pool = db.pool.clone();
    let id = uuid::Uuid::new_v4().to_string();
    let event_repo: PostgresEventRepository = PostgresEventRepository::new(pool.clone());
    let snapshot = event_repo.snapshot::<TestAggregate>(&id).await.unwrap();
    assert_eq!(None, snapshot);

    let test_description = "some test snapshot here".to_string();
    let test_tests = vec!["testA".to_string(), "testB".to_string()];
    event_repo
        .persist::<TestAggregate>(
            &[],
            Some(SnapshotUpdate {
                aggregate_id: id.clone(),
                payload: serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: test_description.clone(),
                    tests: test_tests.clone(),
                })
                .unwrap(),
                last_sequence: 0,
                next_version: 1,
            }),
        )
        .await
        .unwrap();

    let snapshot = event_repo.snapshot::<TestAggregate>(&id).await.unwrap();
    assert_eq!(
        Some(serialized_snapshot(
            id.clone(),
            0,
            1,
            serde_json::to_value(TestAggregate {
                id: id.clone(),
                description: test_description.clone(),
                tests: test_tests.clone(),
            })
            .unwrap()
        )),
        snapshot
    );

    // sequence iterated, does update
    event_repo
        .persist::<TestAggregate>(
            &[],
            Some(SnapshotUpdate {
                aggregate_id: id.clone(),
                payload: serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: "a test description that should be saved".to_string(),
                    tests: test_tests.clone(),
                })
                .unwrap(),
                last_sequence: 0,
                next_version: 2,
            }),
        )
        .await
        .unwrap();

    let snapshot = event_repo.snapshot::<TestAggregate>(&id).await.unwrap();
    assert_eq!(
        Some(serialized_snapshot(
            id.clone(),
            0,
            2,
            serde_json::to_value(TestAggregate {
                id: id.clone(),
                description: "a test description that should be saved".to_string(),
                tests: test_tests.clone(),
            })
            .unwrap()
        )),
        snapshot
    );

    // sequence out of order or not iterated, does not update
    let result = event_repo
        .persist::<TestAggregate>(
            &[],
            Some(SnapshotUpdate {
                aggregate_id: id.clone(),
                payload: serde_json::to_value(TestAggregate {
                    id: id.clone(),
                    description: "a test description that should not be saved".to_string(),
                    tests: test_tests.clone(),
                })
                .unwrap(),
                last_sequence: 0,
                next_version: 2,
            }),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(result, PersistenceError::OptimisticLockError),
        "invalid error result found during insert: {result}"
    );

    let snapshot = event_repo.snapshot::<TestAggregate>(&id).await.unwrap();
    assert_eq!(
        Some(serialized_snapshot(
            id.clone(),
            0,
            2,
            serde_json::to_value(TestAggregate {
                id: id.clone(),
                description: "a test description that should be saved".to_string(),
                tests: test_tests.clone(),
            })
            .unwrap()
        )),
        snapshot
    );

    db.close().await;
}
use super::support::repository::customer::Customer;
use super::support::repository::customer::CustomerEvent;
use cqrsx::journal::EventStore;
use cqrsx::journal::persisted::PersistedEventStore;
use cqrsx::journal::upcaster::NumericVersionUpcaster;
use serde_json::Value;
use std::collections::HashMap;
async fn simple_es_commit_and_load_test(event_store: PersistedEventStore<PostgresEventRepository, Customer>) {
    let id = uuid::Uuid::new_v4().to_string();
    assert_eq!(0, event_store.load_events(id.as_str()).await.unwrap().len());
    let context = event_store.load_aggregate(id.as_str()).await.unwrap();

    event_store
        .commit(
            vec![
                CustomerEvent::NameAdded {
                    name: "test_event_A".to_string(),
                },
                CustomerEvent::EmailUpdated {
                    new_email: "email A".to_string(),
                },
            ],
            context,
            HashMap::default(),
            &[],
        )
        .await
        .unwrap();

    assert_eq!(2, event_store.load_events(id.as_str()).await.unwrap().len());
    let context = event_store.load_aggregate(id.as_str()).await.unwrap();

    event_store
        .commit(
            vec![CustomerEvent::EmailUpdated {
                new_email: "email B".to_string(),
            }],
            context,
            HashMap::default(),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(3, event_store.load_events(id.as_str()).await.unwrap().len());
}
fn new_test_event_store(pool: sqlx::PgPool) -> PersistedEventStore<PostgresEventRepository, Customer> {
    let repo = PostgresEventRepository::new(pool);
    PersistedEventStore::<PostgresEventRepository, Customer>::new(repo)
}

#[tokio::test]
async fn commit_and_load_events() {
    let db = Database::new().await;
    let pool = db.pool.clone();
    let repo = PostgresEventRepository::new(pool);
    let event_store = PersistedEventStore::<PostgresEventRepository, Customer>::new(repo);

    simple_es_commit_and_load_test(event_store).await;

    db.close().await;
}

#[tokio::test]
async fn upcasted_event() {
    let db = Database::new().await;
    let pool = db.pool.clone();
    sqlx::query("INSERT INTO events VALUES ($1, $2, 1, 'NameAdded', '0.1.0', $3, '{}')")
        .bind("Customer")
        .bind("previous_event_in_need_of_upcast")
        .bind(serde_json::json!({"NameAdded": {"legacy_name": "original"}}))
        .execute(&pool)
        .await
        .unwrap();
    let upcaster = NumericVersionUpcaster::try_new(
        "NameAdded",
        "1.0.1",
        Box::new(|mut event| match event.get_mut("NameAdded").unwrap() {
            Value::Object(object) => {
                object.insert("name".to_string(), Value::String("UNKNOWN".to_string()));
                event
            }
            _ => panic!("not the expected object"),
        }),
    )
    .unwrap();
    let event_store = new_test_event_store(pool).with_upcasters(vec![Box::new(upcaster)]);

    let id = "previous_event_in_need_of_upcast".to_string();
    let result = event_store.load_aggregate(id.as_str()).await.unwrap();
    assert_eq!(1, result.current_sequence);
    assert_eq!(None, result.snapshot_version);
    assert_eq!(result.aggregate.name, "UNKNOWN");

    db.close().await;
}
