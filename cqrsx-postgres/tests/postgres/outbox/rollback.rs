use super::*;

#[tokio::test]
async fn outbox_snapshot_conflict_publishes_no_orphan_task() {
    let db = Database::new().await;
    let repo = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    repo.persist::<Ledger>(
        &[event_for("a", 1, 10)],
        Some(SnapshotUpdate {
            aggregate_id: "a".into(),
            payload: json!([10]),
            last_sequence: 1,
            next_version: 1,
        }),
    )
    .await
    .unwrap();
    let before = database_state(&db.pool).await;
    let task = new_task();
    let result = repo
        .persist_with_tasks::<Ledger>(
            &[event_for("a", 2, 20)],
            Some(SnapshotUpdate {
                aggregate_id: "a".into(),
                payload: json!([10, 20]),
                last_sequence: 3,
                next_version: 3,
            }),
            &[task],
        )
        .await;
    assert!(matches!(result, Err(PersistenceError::OptimisticLockError)));
    assert_eq!(
        database_state(&db.pool).await,
        before,
        "snapshot conflict must not publish orphan tasks"
    );
    db.close().await;
}

#[tokio::test]
async fn an_event_conflict_rolls_back_an_otherwise_valid_snapshot_update_and_tasks() {
    let db = Database::new().await;
    let reader = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    reader
        .persist::<TestAggregate>(
            &[event("rollback-snapshot", 1)],
            Some(SnapshotUpdate {
                aggregate_id: "rollback-snapshot".into(),
                payload: json!({"state": "initial"}),
                last_sequence: 1,
                next_version: 1,
            }),
        )
        .await
        .unwrap();
    reader
        .persist::<TestAggregate>(&[event("rollback-snapshot", 2)], None)
        .await
        .unwrap();
    let before = reader.snapshot::<TestAggregate>("rollback-snapshot").await.unwrap();
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let repository_tasks = vec![new_task()];
    let result = repository
        .persist_with_tasks::<TestAggregate>(
            &[event("rollback-snapshot", 2)],
            Some(SnapshotUpdate {
                aggregate_id: "rollback-snapshot".into(),
                payload: json!({"state": "uncommitted"}),
                last_sequence: 2,
                next_version: 2,
            }),
            &repository_tasks,
        )
        .await;
    assert!(matches!(result, Err(PersistenceError::OptimisticLockError)));
    assert_eq!(reader.snapshot::<TestAggregate>("rollback-snapshot").await.unwrap(), before);
    assert_eq!(reader.events::<TestAggregate>("rollback-snapshot").await.unwrap().len(), 2);
    let task_count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(task_count, 0);
    db.close().await;
}

#[tokio::test]
async fn rolls_back_events_and_outbox_when_snapshot_lock_is_stale() {
    let database = Database::new().await;
    let repository = PostgresEventRepositoryWithOutbox::new(database.pool.clone());
    let initial_snapshot = json!({ "state": "initial" });

    repository
        .persist::<StreamAggregate>(
            &[serialized_event(1, "first")],
            Some(SnapshotUpdate {
                aggregate_id: AGGREGATE_ID.to_owned(),
                payload: initial_snapshot.clone(),
                last_sequence: 1,
                next_version: 1,
            }),
        )
        .await
        .expect("initial snapshot must persist");
    let result = repository
        .persist::<StreamAggregate>(
            &[serialized_event(2, "second")],
            Some(SnapshotUpdate {
                aggregate_id: AGGREGATE_ID.to_owned(),
                payload: json!({ "state": "stale" }),
                last_sequence: 3,
                next_version: 3,
            }),
        )
        .await;

    assert!(matches!(result, Err(PersistenceError::OptimisticLockError)));
    assert_eq!(row_count(&database.pool, "events").await, 1);
    assert_eq!(row_count(&database.pool, "transactional_outbox").await, 0);
    assert_eq!(
        repository
            .snapshot::<StreamAggregate>(AGGREGATE_ID)
            .await
            .expect("snapshot must load"),
        Some(SerializedSnapshot {
            aggregate_id: AGGREGATE_ID.to_owned(),
            aggregate: initial_snapshot,
            last_sequence: 1,
            snapshot_version: 1,
        })
    );

    database.close().await;
}
