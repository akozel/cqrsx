// Derived from postgres-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use super::support::Database;
use super::support::repository::*;
use cqrsx::cqrs::projection::repository::ViewContext;
use cqrsx::cqrs::projection::repository::ViewRepository;
use cqrsx::postgres::view_repository::PostgresViewRepository;
use std::sync::Arc;
async fn create_view(pool: &sqlx::PgPool) {
    sqlx::raw_sql("CREATE TABLE test_view (view_id text PRIMARY KEY, version bigint NOT NULL, payload json NOT NULL)")
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_valid_view_repository() {
    let db = Database::new().await;
    let pool = db.pool.clone();
    create_view(&pool).await;
    let repo = PostgresViewRepository::<TestView, TestAggregate>::new("test_view", pool.clone());
    let test_view_id = uuid::Uuid::new_v4().to_string();

    let view = TestView {
        events: vec![TestEvent::Created(Created {
            id: "just a test event for this view".to_string(),
        })],
    };
    repo.update_view(view.clone(), ViewContext::new(test_view_id.to_string(), 0))
        .await
        .unwrap();
    let (found, context) = repo.load_with_context(&test_view_id).await.unwrap().unwrap();
    assert_eq!(found, view);
    let found = repo.load(&test_view_id).await.unwrap().unwrap();
    assert_eq!(found, view);

    let updated_view = TestView {
        events: vec![TestEvent::Created(Created {
            id: "a totally different view".to_string(),
        })],
    };
    repo.update_view(updated_view.clone(), context).await.unwrap();
    let found_option = repo.load(&test_view_id).await.unwrap();
    let found = found_option.unwrap();

    assert_eq!(found, updated_view);

    db.close().await;
}

#[tokio::test]
async fn test_valid_cqrs_framework() {
    let db = Database::new().await;
    let pool = db.pool.clone();
    create_view(&pool).await;
    let repo = PostgresViewRepository::<TestView, TestAggregate>::new("test_view", pool.clone());
    let projection = TestProjection::new(Arc::new(repo));
    let repository = cqrsx::postgres::event_repository::PostgresEventRepository::new(pool);
    let store = cqrsx::cqrs::store::persisted::PersistedEventStore::new_event_store(repository);
    let _framework = cqrsx::cqrs::CqrsFramework::new(store, vec![Box::new(projection)], TestServices);

    db.close().await;
}

#[tokio::test]
async fn stress_optimistic_view_create_and_update_has_one_winner() {
    use super::support::repository::{finish_writers, wait_for_writers};
    use cqrsx::cqrs::store::PersistenceError;
    use tokio::task::JoinSet;
    let db = Database::new().await;
    create_view(&db.pool).await;
    for version in [0, 1] {
        for writers in [2, 4, 8] {
            for round in 0..32 {
                let id = format!("view-{version}-{writers}-{round}");
                let repo = PostgresViewRepository::<TestView, TestAggregate>::new("test_view", db.pool.clone());
                if version == 1 {
                    repo.update_view(TestView::default(), ViewContext::new(id.clone(), 0))
                        .await
                        .unwrap();
                }
                let mut lock = db.pool.begin().await.unwrap();
                sqlx::query("LOCK TABLE test_view IN SHARE MODE")
                    .execute(&mut *lock)
                    .await
                    .unwrap();
                let mut tasks = JoinSet::new();
                for writer in 0..writers {
                    let pool = db.pool.clone();
                    let id = id.clone();
                    tasks.spawn(async move {
                        let view = TestView {
                            events: vec![TestEvent::Created(Created {
                                id: format!("writer-{writer}"),
                            })],
                        };
                        let result = PostgresViewRepository::<TestView, TestAggregate>::new("test_view", pool)
                            .update_view(view.clone(), ViewContext::new(id, version))
                            .await;
                        (view, result)
                    });
                }
                wait_for_writers(
                    &db.pool,
                    if version == 0 {
                        "INSERT INTO test_view"
                    } else {
                        "UPDATE test_view"
                    },
                    writers,
                    &mut tasks,
                )
                .await;
                lock.commit().await.unwrap();
                let mut winners = Vec::new();
                let mut conflicts = 0;
                for (view, result) in finish_writers(&mut tasks).await {
                    match result {
                        Ok(()) => winners.push(view),
                        Err(PersistenceError::OptimisticLockError) => conflicts += 1,
                        other => panic!("unexpected view result: {other:?}"),
                    }
                }
                assert_eq!(winners.len(), 1, "{id}");
                assert_eq!(conflicts, writers - 1);
                let (view, context) = repo.load_with_context(&id).await.unwrap().unwrap();
                assert_eq!(view, winners[0]);
                assert_eq!(context.version, version + 1);
                assert_eq!(context.view_instance_id, id);
            }
        }
    }
    db.close().await;
}

#[tokio::test]
async fn stress_view_projection_reloads_after_conflict_and_preserves_all_batches() {
    use super::support::repository::{finish_writers, wait_for_writers};
    use cqrsx::cqrs::{
        event::EventEnvelope,
        event_handler::EventHandler,
        projection::ViewProjection,
        store::{repository::PersistedEventRepository, serialized_event::SerializedEvent},
    };
    use cqrsx::postgres::event_repository::PostgresEventRepository;
    let db = Database::new().await;
    create_view(&db.pool).await;
    for initial in [0, 1] {
        for writers in [2, 4, 8] {
            for round in 0..32 {
                let id = format!("query-{initial}-{writers}-{round}");
                let repository = Arc::new(PostgresViewRepository::<TestView, TestAggregate>::new(
                    "test_view",
                    db.pool.clone(),
                ));
                let seed = TestEvent::Created(Created { id: "seed".into() });
                let mut expected = if initial == 1 { vec![seed] } else { vec![] };
                if initial == 1 {
                    repository
                        .update_view(
                            TestView {
                                events: expected.clone(),
                            },
                            ViewContext::new(id.clone(), 0),
                        )
                        .await
                        .unwrap();
                }
                let mut source = Vec::new();
                let mut batches = Vec::new();
                if initial == 1 {
                    source.push(EventEnvelope::<TestAggregate> {
                        aggregate_id: id.clone(),
                        sequence: 1,
                        payload: expected[0].clone(),
                        metadata: Default::default(),
                    });
                }
                for writer in 0..writers {
                    let batch: Vec<_> = (0..2)
                        .map(|offset| EventEnvelope::<TestAggregate> {
                            aggregate_id: id.clone(),
                            sequence: source.len() + offset + 1,
                            payload: TestEvent::Created(Created {
                                id: format!("{writer}-{offset}"),
                            }),
                            metadata: Default::default(),
                        })
                        .collect();
                    expected.extend(batch.iter().map(|e| e.payload.clone()));
                    source.extend_from_slice(&batch);
                    batches.push(batch);
                }
                PostgresEventRepository::new(db.pool.clone())
                    .persist::<TestAggregate>(
                        &source
                            .iter()
                            .map(|e| SerializedEvent::try_from(e).unwrap())
                            .collect::<Vec<_>>(),
                        None,
                    )
                    .await
                    .unwrap();
                let mut lock = db.pool.begin().await.unwrap();
                sqlx::query("LOCK TABLE test_view IN SHARE MODE")
                    .execute(&mut *lock)
                    .await
                    .unwrap();
                let mut tasks = tokio::task::JoinSet::new();
                for batch in batches.clone() {
                    let projection = ViewProjection::new(repository.clone());
                    let id = id.clone();
                    tasks.spawn(async move { projection.handle(&id, &batch).await });
                }
                wait_for_writers(
                    &db.pool,
                    if initial == 0 {
                        "INSERT INTO test_view"
                    } else {
                        "UPDATE test_view"
                    },
                    writers as i64,
                    &mut tasks,
                )
                .await;
                lock.commit().await.unwrap();
                for result in finish_writers(&mut tasks).await {
                    result.unwrap();
                }
                let (view, context) = repository.load_with_context(&id).await.unwrap().unwrap();
                assert_eq!(context.version, initial + writers as i64);
                assert_eq!(view.events.len(), expected.len());
                for event in &expected {
                    assert_eq!(view.events.iter().filter(|e| *e == event).count(), 1);
                }
                for batch in batches {
                    let first = view.events.iter().position(|e| e == &batch[0].payload).unwrap();
                    assert_eq!(view.events[first + 1], batch[1].payload);
                }
                assert_eq!(
                    PostgresEventRepository::new(db.pool.clone())
                        .get_events::<TestAggregate>(&id)
                        .await
                        .unwrap()
                        .len(),
                    expected.len()
                );
            }
        }
    }
    db.close().await;
}

#[tokio::test]
async fn missing_and_corrupt_views_and_sql_failures_are_explicit() {
    use cqrsx::cqrs::store::PersistenceError;
    let db = Database::new().await;
    create_view(&db.pool).await;
    let repo = PostgresViewRepository::<TestView, TestAggregate>::new("test_view", db.pool.clone());
    assert_eq!(repo.load("missing").await.unwrap(), None);
    assert!(repo.load_with_context("missing").await.unwrap().is_none());
    sqlx::query("INSERT INTO test_view VALUES ('broken',1,'{}')")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(matches!(
        repo.load("broken").await,
        Err(PersistenceError::DeserializationError(_))
    ));
    assert!(matches!(
        repo.load_with_context("broken").await,
        Err(PersistenceError::DeserializationError(_))
    ));
    let missing = PostgresViewRepository::<TestView, TestAggregate>::new("missing", db.pool.clone());
    assert!(matches!(missing.load("a").await, Err(PersistenceError::UnknownError(_))));
    assert!(matches!(
        missing.load_with_context("a").await,
        Err(PersistenceError::UnknownError(_))
    ));
    assert!(matches!(
        missing
            .update_view(TestView::default(), ViewContext::new("a".into(), 0))
            .await,
        Err(PersistenceError::UnknownError(_))
    ));
    db.close().await;
}

#[tokio::test]
async fn unrelated_view_uniqueness_is_technical_and_stale_versions_are_conflicts() {
    use cqrsx::cqrs::store::PersistenceError;
    let db = Database::new().await;
    create_view(&db.pool).await;
    sqlx::query("CREATE UNIQUE INDEX unrelated_view ON test_view ((payload::text))")
        .execute(&db.pool)
        .await
        .unwrap();
    let repo = PostgresViewRepository::<TestView, TestAggregate>::new("test_view", db.pool.clone());
    repo.update_view(TestView::default(), ViewContext::new("one".into(), 0))
        .await
        .unwrap();
    assert!(matches!(
        repo.update_view(TestView::default(), ViewContext::new("two".into(), 0)).await,
        Err(PersistenceError::UnknownError(_))
    ));
    assert!(matches!(
        repo.update_view(TestView::default(), ViewContext::new("one".into(), 0)).await,
        Err(PersistenceError::OptimisticLockError)
    ));
    let other = TestView {
        events: vec![TestEvent::Created(Created { id: "other".into() })],
    };
    repo.update_view(other.clone(), ViewContext::new("two".into(), 0))
        .await
        .unwrap();
    assert!(matches!(
        repo.update_view(TestView::default(), ViewContext::new("two".into(), 1)).await,
        Err(PersistenceError::UnknownError(_))
    ));
    assert!(matches!(
        repo.update_view(other.clone(), ViewContext::new("two".into(), 9)).await,
        Err(PersistenceError::OptimisticLockError)
    ));
    assert!(matches!(
        repo.update_view(other.clone(), ViewContext::new("two".into(), i64::MAX))
            .await,
        Err(PersistenceError::UnknownError(_))
    ));
    let (saved, context) = repo.load_with_context("two").await.unwrap().unwrap();
    assert_eq!(saved, other);
    assert_eq!(context.version, 1);
    db.close().await;
}

#[path = "projection/committed.rs"]
mod committed;
