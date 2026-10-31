//! `PostgresCheckpointStore`: one statement per operation, fenced by the lease token, and the schema's rules.
use crate::support::Database;
use cqrsx::{
    journal::feed::Position,
    postgres::{PROJECTION_CHECKPOINTS_SCHEMA, checkpoint_store::PostgresCheckpointStore},
    projection::checkpoint::{
        CheckpointError, CheckpointKey, CheckpointStore, LeaseToken, RebuildRequest, RebuildRequests, RebuildState,
    },
};
use serde_json::{Value, json};
use std::time::Duration;
use uuid::Uuid;

fn key() -> CheckpointKey {
    CheckpointKey::new("halls", "Concert")
}

fn other() -> CheckpointKey {
    CheckpointKey::new("wallets", "Fan")
}

const LONG: Duration = Duration::from_secs(60);

#[tokio::test]
async fn a_row_is_claimed_by_one_owner_until_it_is_released_or_its_lease_runs_out() {
    let db = Database::new().await;
    let store = PostgresCheckpointStore::new(db.pool.clone());
    assert!(store.claim(&key(), "a", LONG).await.unwrap().is_none(), "no row yet");
    store.register(&key()).await.unwrap();
    store.register(&key()).await.unwrap();
    let first = store.claim(&key(), "a", LONG).await.unwrap().expect("a free row");
    assert_eq!((first.position, first.failures, first.rebuild), (Position::START, 0, None));
    assert!(store.claim(&key(), "b", LONG).await.unwrap().is_none(), "held by a");
    let stale = LeaseToken::new(Uuid::new_v4());
    assert!(!store.extend(&key(), stale, LONG).await.unwrap());
    assert!(!store.advance(&key(), stale, Position::new(1), 1).await.unwrap());
    assert!(!store.record_failure(&key(), stale, "halls: connection").await.unwrap());
    assert!(!store.begin_rebuild(&key(), stale).await.unwrap());
    assert!(!store.advance_rebuild(&key(), stale, "k", 1).await.unwrap());
    assert!(!store.finish_rebuild(&key(), stale).await.unwrap());
    assert!(store.take_requests(&key(), stale, 5).await.unwrap().is_none());
    store.release(&key(), stale).await.unwrap();
    assert!(
        store.claim(&key(), "b", LONG).await.unwrap().is_none(),
        "a stale release changes nothing"
    );
    assert!(store.extend(&key(), first.token, LONG).await.unwrap());
    store.release(&key(), first.token).await.unwrap();
    let second = store
        .claim(&key(), "b", Duration::from_secs(1))
        .await
        .unwrap()
        .expect("released");
    assert_ne!(first.token, second.token);
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let third = store
        .claim(&key(), "c", LONG)
        .await
        .unwrap()
        .expect("expired by the server clock");
    assert!(
        !store.extend(&key(), second.token, LONG).await.unwrap(),
        "the expired owner is fenced out"
    );
    assert!(store.extend(&key(), third.token, LONG).await.unwrap());
    let rows = store.status(None).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (
            rows[0].owner.as_deref(),
            rows[0].projection.as_str(),
            rows[0].aggregate_type.as_str()
        ),
        (Some("c"), "halls", "Concert")
    );
    assert!(rows[0].locked_until.unwrap() > rows[0].updated_at);
    db.close().await;
}

#[tokio::test]
async fn advance_moves_only_forward_counts_a_repeat_once_and_clears_the_failures() {
    let db = Database::new().await;
    let store = PostgresCheckpointStore::new(db.pool.clone());
    store.register(&key()).await.unwrap();
    let owner = store.claim(&key(), "a", LONG).await.unwrap().unwrap();
    assert!(store.record_failure(&key(), owner.token, "halls: connection").await.unwrap());
    assert!(store.record_failure(&key(), owner.token, "halls: panic").await.unwrap());
    let row = store.status(Some("halls")).await.unwrap().remove(0);
    assert_eq!((row.failures, row.last_error.as_deref()), (2, Some("halls: panic")));
    assert!(store.advance(&key(), owner.token, Position::new(5), 7).await.unwrap());
    assert!(
        store.advance(&key(), owner.token, Position::new(5), 7).await.unwrap(),
        "a repeat is accepted"
    );
    assert!(
        !store.advance(&key(), owner.token, Position::new(4), 1).await.unwrap(),
        "never back"
    );
    assert!(store.advance(&key(), owner.token, Position::new(9), 2).await.unwrap());
    let row = store.status(None).await.unwrap().remove(0);
    assert_eq!(
        (row.position, row.applied, row.failures, row.last_error),
        (Position::new(9), 9, 0, None)
    );
    store.release(&key(), owner.token).await.unwrap();
    let next = store.claim(&key(), "b", LONG).await.unwrap().unwrap();
    assert_eq!((next.position, next.failures), (Position::new(9), 0));
    assert!(store.status(Some("none")).await.unwrap().is_empty());
    db.close().await;
}

#[tokio::test]
async fn rebuild_requests_run_one_at_a_time_and_keys_are_taken_in_request_order_by_the_owner() {
    let db = Database::new().await;
    let store = PostgresCheckpointStore::new(db.pool.clone());
    assert!(matches!(
        store.request_rebuild(&key(), Duration::ZERO).await,
        Err(CheckpointError::Invalid("projection is not registered"))
    ));
    assert!(matches!(
        store.request_rebuild_key(&key(), "c1").await,
        Err(CheckpointError::Invalid("projection is not registered"))
    ));
    store.register(&key()).await.unwrap();
    store.register(&other()).await.unwrap();
    assert!(matches!(
        store
            .request_rebuild(&key(), Duration::from_millis(u64::from(u32::MAX)))
            .await,
        Err(CheckpointError::Invalid(_))
    ));
    assert_eq!(
        store.request_rebuild(&key(), Duration::from_millis(80)).await.unwrap(),
        RebuildRequest::Accepted
    );
    assert_eq!(
        store.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::AlreadyRunning
    );
    for id in ["c2", "c1", "c2", "c3"] {
        store.request_rebuild_key(&key(), id).await.unwrap();
    }
    store.request_rebuild_key(&other(), "z").await.unwrap();
    let owner = store.claim(&key(), "a", LONG).await.unwrap().unwrap();
    let first = store.take_requests(&key(), owner.token, 2).await.unwrap().unwrap();
    assert_eq!(
        first,
        RebuildRequests {
            rebuild: Some(Duration::from_millis(80)),
            keys: vec!["c2".into(), "c1".into()]
        }
    );
    assert!(store.begin_rebuild(&key(), owner.token).await.unwrap());
    let rest = store.take_requests(&key(), owner.token, 2).await.unwrap().unwrap();
    assert_eq!(
        rest,
        RebuildRequests {
            rebuild: None,
            keys: vec!["c3".into()]
        }
    );
    assert_eq!(
        store.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::AlreadyRunning,
        "running"
    );
    assert!(store.advance_rebuild(&key(), owner.token, "c2", 2).await.unwrap());
    assert!(
        store.advance_rebuild(&key(), owner.token, "c2", 2).await.unwrap(),
        "a repeat counts once"
    );
    assert!(
        !store.advance_rebuild(&key(), owner.token, "c1", 1).await.unwrap(),
        "never back"
    );
    assert!(store.advance_rebuild(&key(), owner.token, "c3", 1).await.unwrap());
    let row = store.status(Some("halls")).await.unwrap().remove(0);
    assert_eq!(
        (
            row.rebuild_cursor.as_deref(),
            row.rebuilt,
            row.rebuild_requested,
            row.rebuild_pace
        ),
        (Some("c3"), 3, false, Duration::from_millis(80))
    );
    store.release(&key(), owner.token).await.unwrap();
    let next = store.claim(&key(), "b", LONG).await.unwrap().unwrap();
    assert_eq!(
        next.rebuild,
        Some(RebuildState {
            cursor: "c3".into(),
            pace: Duration::from_millis(80),
            rebuilt: 3
        }),
        "a new owner continues from the cursor"
    );
    assert!(store.finish_rebuild(&key(), next.token).await.unwrap());
    assert!(
        !store.advance_rebuild(&key(), next.token, "c4", 1).await.unwrap(),
        "no rebuild runs"
    );
    assert_eq!(
        store.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::Accepted
    );
    let wallets = store.claim(&other(), "w", LONG).await.unwrap().unwrap();
    assert_eq!(
        store.take_requests(&other(), wallets.token, 10).await.unwrap().unwrap().keys,
        ["z"],
        "another row's keys were not taken"
    );
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM projection_rebuild_keys")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(left, 0, "taken keys are gone");
    db.close().await;
}

#[tokio::test]
async fn the_schema_rejects_inconsistent_rows_and_applies_again_unchanged() {
    let db = Database::new().await;
    sqlx::raw_sql(PROJECTION_CHECKPOINTS_SCHEMA).execute(&db.pool).await.unwrap();
    let store = PostgresCheckpointStore::new(db.pool.clone());
    store.register(&key()).await.unwrap();
    let original: Value = sqlx::query_scalar("SELECT to_jsonb(c) FROM projection_checkpoints c WHERE projection_name = $1")
        .bind("halls")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    let token = Uuid::new_v4();
    let until = chrono::Utc::now() + chrono::Duration::minutes(1);
    for (number, invalid) in [
        json!({"projection_name": ""}),
        json!({"aggregate_type": ""}),
        json!({"applied": -1}),
        json!({"rebuild_pace_ms": -1}),
        json!({"rebuilt": -1}),
        json!({"failures": -1}),
        json!({"owner": "x"}),
        json!({"lock_token": token}),
        json!({"locked_until": until}),
        json!({"owner": "x", "lock_token": token}),
        json!({"rebuild_requested": true, "rebuild_cursor": ""}),
    ]
    .into_iter()
    .enumerate()
    {
        let mut candidate = original.clone();
        candidate["projection_name"] = json!(format!("candidate-{number}"));
        candidate
            .as_object_mut()
            .unwrap()
            .extend(invalid.as_object().unwrap().clone());
        let error = sqlx::query(
            "INSERT INTO projection_checkpoints SELECT * FROM jsonb_populate_record(NULL::projection_checkpoints, $1)",
        )
        .bind(candidate)
        .execute(&db.pool)
        .await
        .expect_err("an inconsistent row must be rejected even when bypassing the Rust API");
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("23514"),
            "{invalid}"
        );
    }
    for (number, valid) in [
        json!({"owner": "x", "lock_token": token, "locked_until": until}),
        json!({"rebuild_requested": true}),
        json!({"rebuild_cursor": ""}),
        json!({"rebuild_cursor": "k", "rebuilt": 3}),
    ]
    .into_iter()
    .enumerate()
    {
        let mut candidate = original.clone();
        candidate["projection_name"] = json!(format!("valid-{number}"));
        candidate.as_object_mut().unwrap().extend(valid.as_object().unwrap().clone());
        sqlx::query("INSERT INTO projection_checkpoints SELECT * FROM jsonb_populate_record(NULL::projection_checkpoints, $1)")
            .bind(candidate)
            .execute(&db.pool)
            .await
            .unwrap();
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM projection_checkpoints")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 5, "the registered row and the four valid ones");
    assert_eq!(store.status(None).await.unwrap().len(), 5);
    db.close().await;
}
