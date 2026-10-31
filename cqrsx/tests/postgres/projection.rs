// Derived from postgres-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use super::support::Database;
use super::support::repository::*;
use cqrsx::domain::event::EventEnvelope;
use cqrsx::postgres::view_repository::PostgresViewRepository;
use cqrsx::view::View;
use cqrsx::view::repository::StoredView;
use cqrsx::view::repository::ViewContext;
use cqrsx::view::repository::ViewRepository;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A view whose table nobody created.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Unprovisioned;

impl View<TestAggregate> for Unprovisioned {
    const NAME: &'static str = "missing";
    fn update(&mut self, _: &EventEnvelope<TestAggregate>) {}
}

/// A view whose table holds `version` as `integer` and `payload` as `text`: other types than the repository reads.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Mistyped;

impl View<TestAggregate> for Mistyped {
    const NAME: &'static str = "mistyped_view";
    fn update(&mut self, _: &EventEnvelope<TestAggregate>) {}
}

async fn create_view(pool: &sqlx::PgPool) {
    sqlx::raw_sql("CREATE TABLE test_view (view_id text PRIMARY KEY, version bigint NOT NULL, last_sequence bigint NOT NULL CHECK (last_sequence >= 0), payload json NOT NULL)")
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_valid_view_repository() {
    let db = Database::new().await;
    let pool = db.pool.clone();
    create_view(&pool).await;
    let repo = PostgresViewRepository::<TestView, TestAggregate>::new(pool.clone());
    let printed = format!("{repo:?}");
    assert!(
        printed.starts_with(r#"PostgresViewRepository { insert_sql: "INSERT INTO "#),
        "{printed}"
    );
    assert!(
        printed.contains(r#"", pool: Pool { size: "#),
        "the pool is printed last: {printed}"
    );
    let test_view_id = uuid::Uuid::new_v4().to_string();

    let view = TestView {
        events: vec![TestEvent::Created(Created {
            id: "just a test event for this view".to_string(),
        })],
    };
    repo.update_view(view.clone(), ViewContext::new(test_view_id.to_string(), 0, 1))
        .await
        .unwrap();
    let stored = repo.load_with_context(&test_view_id).await.unwrap().unwrap();
    assert_eq!(
        stored,
        StoredView {
            id: test_view_id.to_string(),
            version: 1,
            last_sequence: 1,
            view: view.clone(),
        }
    );
    let found = repo.load(&test_view_id).await.unwrap().unwrap();
    assert_eq!(found, view);

    let updated_view = TestView {
        events: vec![TestEvent::Created(Created {
            id: "a totally different view".to_string(),
        })],
    };
    repo.update_view(updated_view.clone(), ViewContext::new(stored.id, stored.version, 2))
        .await
        .unwrap();
    let stored = repo.load_with_context(&test_view_id).await.unwrap().unwrap();
    assert_eq!(
        stored,
        StoredView {
            id: test_view_id.to_string(),
            version: 2,
            last_sequence: 2,
            view: updated_view,
        }
    );

    db.close().await;
}

#[tokio::test]
async fn stress_optimistic_view_create_and_update_has_one_winner() {
    use super::support::repository::{finish_writers, wait_for_writers};
    use cqrsx::journal::PersistenceError;
    use tokio::task::JoinSet;
    let db = Database::new().await;
    create_view(&db.pool).await;
    for version in [0, 1] {
        for writers in [2, 4, 8] {
            for round in 0..32 {
                let id = format!("view-{version}-{writers}-{round}");
                let repo = PostgresViewRepository::<TestView, TestAggregate>::new(db.pool.clone());
                if version == 1 {
                    repo.update_view(TestView::default(), ViewContext::new(id.clone(), 0, 0))
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
                        let result = PostgresViewRepository::<TestView, TestAggregate>::new(pool)
                            .update_view(view.clone(), ViewContext::new(id, version, 1))
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
                let stored = repo.load_with_context(&id).await.unwrap().unwrap();
                assert_eq!(stored.view, winners[0]);
                assert_eq!((stored.version, stored.last_sequence), (version + 1, 1));
                assert_eq!(stored.id, id);
            }
        }
    }
    db.close().await;
}

/// Case 8 of the contiguity rule on a real database: writers of nested prefixes of one aggregate's history
/// race for one document; whoever loses the optimistic check rereads and applies only what the winner left.
#[tokio::test]
async fn stress_two_writers_of_one_document_converge() {
    use super::support::repository::{finish_writers, wait_for_writers};
    use cqrsx::postgres::event_repository::PostgresEventRepository;
    use cqrsx::{
        domain::event::EventEnvelope,
        journal::{repository::PersistedEventRepository, serialized_event::SerializedEvent},
        projection::EventHandler,
        view::projection::ViewProjection,
    };
    let db = Database::new().await;
    create_view(&db.pool).await;
    for initial in [0usize, 1] {
        for writers in [2usize, 4] {
            for round in 0..16 {
                let id = format!("converge-{initial}-{writers}-{round}");
                let repository = Arc::new(PostgresViewRepository::<TestView, TestAggregate>::new(db.pool.clone()));
                // One history of `total` events; writer `w` delivers the prefix `initial+1 ..= initial+2+w`.
                let total = initial + writers + 1;
                let source: Vec<EventEnvelope<TestAggregate>> = (1..=total)
                    .map(|sequence| EventEnvelope {
                        aggregate_id: id.clone(),
                        sequence,
                        payload: TestEvent::Created(Created {
                            id: format!("event-{sequence}"),
                        }),
                        metadata: Default::default(),
                    })
                    .collect();
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
                if initial == 1 {
                    ViewProjection::new(repository.clone())
                        .handle(&id, &source[..1])
                        .await
                        .unwrap();
                }
                let mut lock = db.pool.begin().await.unwrap();
                sqlx::query("LOCK TABLE test_view IN SHARE MODE")
                    .execute(&mut *lock)
                    .await
                    .unwrap();
                let mut tasks = tokio::task::JoinSet::new();
                for writer in 0..writers {
                    let projection = ViewProjection::new(repository.clone());
                    let batch = source[initial..initial + 2 + writer].to_vec();
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
                let stored = repository.load_with_context(&id).await.unwrap().unwrap();
                let expected: Vec<_> = source.iter().map(|e| e.payload.clone()).collect();
                assert_eq!(
                    stored.view.events, expected,
                    "{id}: the fold of the whole history, every event once"
                );
                assert_eq!(stored.last_sequence, total, "{id}");
                let versions = (initial as i64 + 1)..=((initial + writers) as i64);
                assert!(
                    versions.contains(&stored.version),
                    "{id}: version {} outside {versions:?}",
                    stored.version
                );
                assert_eq!(
                    PostgresEventRepository::new(db.pool.clone())
                        .events::<TestAggregate>(&id)
                        .await
                        .unwrap()
                        .len(),
                    total
                );
            }
        }
    }
    db.close().await;
}

#[tokio::test]
async fn missing_and_corrupt_views_and_sql_failures_are_explicit() {
    use cqrsx::journal::PersistenceError;
    let db = Database::new().await;
    create_view(&db.pool).await;
    let repo = PostgresViewRepository::<TestView, TestAggregate>::new(db.pool.clone());
    assert_eq!(repo.load("missing").await.unwrap(), None);
    assert!(repo.load_with_context("missing").await.unwrap().is_none());
    sqlx::query("INSERT INTO test_view VALUES ('broken', 1, 0, '{}')")
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
    let missing = PostgresViewRepository::<Unprovisioned, TestAggregate>::new(db.pool.clone());
    assert!(matches!(missing.load("a").await, Err(PersistenceError::UnknownError(_))));
    assert!(matches!(
        missing.load_with_context("a").await,
        Err(PersistenceError::UnknownError(_))
    ));
    assert!(matches!(
        missing.update_view(Unprovisioned, ViewContext::new("a".into(), 0, 0)).await,
        Err(PersistenceError::UnknownError(_))
    ));
    #[cfg(target_pointer_width = "64")]
    {
        // A `last_sequence` beyond `bigint` is refused before the statement.
        assert!(matches!(
            repo.update_view(TestView::default(), ViewContext::new("too-far".into(), 0, usize::MAX))
                .await,
            Err(PersistenceError::UnknownError(_))
        ));
        assert!(repo.load("too-far").await.unwrap().is_none());
    }
    db.close().await;
}

/// A table whose columns have other types than the repository reads fails every read with a `DeserializationError`,
/// as a payload that is no view does, instead of panicking in the handler of the projection or in a query.
#[tokio::test]
async fn columns_of_other_types_are_deserialization_errors_not_panics() {
    use cqrsx::journal::PersistenceError;
    let db = Database::new().await;
    sqlx::raw_sql(
        "CREATE TABLE mistyped_view (view_id text PRIMARY KEY, version integer NOT NULL, last_sequence bigint NOT NULL, payload text NOT NULL);
         INSERT INTO mistyped_view VALUES ('a', 1, 0, 'null')",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let repo = PostgresViewRepository::<Mistyped, TestAggregate>::new(db.pool.clone());
    assert!(matches!(repo.load("a").await, Err(PersistenceError::DeserializationError(_))));
    assert!(matches!(
        repo.load_with_context("a").await,
        Err(PersistenceError::DeserializationError(_))
    ));
    assert!(matches!(repo.list().await, Err(PersistenceError::DeserializationError(_))));
    assert!(matches!(
        repo.range("a", "b").await,
        Err(PersistenceError::DeserializationError(_))
    ));
    db.close().await;
}

#[tokio::test]
async fn unrelated_view_uniqueness_is_technical_and_stale_versions_are_conflicts() {
    use cqrsx::journal::PersistenceError;
    let db = Database::new().await;
    create_view(&db.pool).await;
    sqlx::query("CREATE UNIQUE INDEX unrelated_view ON test_view ((payload::text))")
        .execute(&db.pool)
        .await
        .unwrap();
    let repo = PostgresViewRepository::<TestView, TestAggregate>::new(db.pool.clone());
    repo.update_view(TestView::default(), ViewContext::new("one".into(), 0, 1))
        .await
        .unwrap();
    assert!(matches!(
        repo.update_view(TestView::default(), ViewContext::new("two".into(), 0, 1))
            .await,
        Err(PersistenceError::UnknownError(_))
    ));
    assert!(matches!(
        repo.update_view(TestView::default(), ViewContext::new("one".into(), 0, 1))
            .await,
        Err(PersistenceError::OptimisticLockError)
    ));
    let other = TestView {
        events: vec![TestEvent::Created(Created { id: "other".into() })],
    };
    repo.update_view(other.clone(), ViewContext::new("two".into(), 0, 1))
        .await
        .unwrap();
    assert!(matches!(
        repo.update_view(TestView::default(), ViewContext::new("two".into(), 1, 1))
            .await,
        Err(PersistenceError::UnknownError(_))
    ));
    assert!(matches!(
        repo.update_view(other.clone(), ViewContext::new("two".into(), 9, 1)).await,
        Err(PersistenceError::OptimisticLockError)
    ));
    assert!(matches!(
        repo.update_view(other.clone(), ViewContext::new("two".into(), i64::MAX, 1))
            .await,
        Err(PersistenceError::UnknownError(_))
    ));
    let stored = repo.load_with_context("two").await.unwrap().unwrap();
    assert_eq!(stored.view, other);
    assert_eq!(stored.version, 1);
    db.close().await;
}

/// The ids of every repository's ordering test: in the order of their bytes (`F` < `f`; `-` < `:` < `;` < `_` < `y`).
const IDS: [&str; 8] = [
    "Fox:muse-2027-01-16",
    "fox-2:radiohead-2026-11-21",
    "fox:muse-2027-01-16",
    "fox:radiohead-2026-11-21",
    "fox;",
    "fox_1:c-1",
    "foxy:c-1",
    "panda:c-1",
];

/// The column sorts by ICU's root collation, which orders the punctuation otherwise (`_` < `-` < `;` < `:`): the
/// queries order by the bytes of the ids all the same.
#[tokio::test]
async fn documents_are_listed_and_ranged_in_the_byte_order_of_their_ids() {
    use cqrsx::journal::PersistenceError;
    let db = Database::new().await;
    sqlx::raw_sql(r#"CREATE TABLE test_view (view_id text COLLATE "und-x-icu" PRIMARY KEY, version bigint NOT NULL, last_sequence bigint NOT NULL CHECK (last_sequence >= 0), payload json NOT NULL)"#)
        .execute(&db.pool)
        .await
        .unwrap();
    let repo = PostgresViewRepository::<TestView, TestAggregate>::new(db.pool.clone());
    assert!(repo.list().await.unwrap().is_empty());
    assert!(repo.range("fox:", "fox;").await.unwrap().is_empty());
    let view = |n: usize| TestView {
        events: (1..=n)
            .map(|n| TestEvent::Created(Created { id: format!("e-{n}") }))
            .collect(),
    };
    for (n, id) in IDS.iter().rev().enumerate() {
        repo.update_view(view(n), ViewContext::new((*id).to_owned(), 0, n))
            .await
            .unwrap();
    }
    let by_column: Vec<String> = sqlx::query_scalar("SELECT view_id FROM test_view ORDER BY view_id")
        .fetch_all(&db.pool)
        .await
        .unwrap();
    assert_ne!(by_column, IDS, "the column's own order is not the order of the bytes");
    let ids = |stored: Vec<StoredView<TestView>>| stored.into_iter().map(|stored| stored.id).collect::<Vec<_>>();
    assert_eq!(ids(repo.list().await.unwrap()), IDS);
    assert_eq!(
        ids(repo.range("fox:", "fox;").await.unwrap()),
        ["fox:muse-2027-01-16", "fox:radiohead-2026-11-21"]
    );
    // The lower bound is in, the upper one out; an empty or inverted range holds nothing.
    assert_eq!(ids(repo.range("fox;", "foxy:c-1").await.unwrap()), ["fox;", "fox_1:c-1"]);
    for (lower, upper) in [("fox;", "fox:"), ("fox;", "fox;"), ("zebra", "a")] {
        assert!(repo.range(lower, upper).await.unwrap().is_empty(), "{lower}..{upper}");
    }
    assert_eq!(
        repo.range("fox;", "fox_").await.unwrap(),
        [StoredView {
            id: "fox;".into(),
            version: 1,
            last_sequence: 3,
            view: view(3),
        }]
    );
    // A row that is no view fails the listing and every range that holds it; a table nobody created fails both.
    sqlx::query("INSERT INTO test_view VALUES ('zz-broken', 1, 0, '{}')")
        .execute(&db.pool)
        .await
        .unwrap();
    assert!(matches!(repo.list().await, Err(PersistenceError::DeserializationError(_))));
    assert!(matches!(
        repo.range("zz", "zz~").await,
        Err(PersistenceError::DeserializationError(_))
    ));
    assert_eq!(ids(repo.range("F", "zz").await.unwrap()), IDS);
    let missing = PostgresViewRepository::<Unprovisioned, TestAggregate>::new(db.pool.clone());
    assert!(matches!(missing.list().await, Err(PersistenceError::UnknownError(_))));
    assert!(matches!(
        missing.range("a", "b").await,
        Err(PersistenceError::UnknownError(_))
    ));
    db.close().await;
}

#[path = "projection/checkpoints.rs"]
mod checkpoints;
#[path = "projection/processor.rs"]
mod processor;
