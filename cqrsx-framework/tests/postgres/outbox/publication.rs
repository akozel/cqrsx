use super::*;

#[tokio::test]
async fn explicit_batches_commit_once_and_rollback_with_events_and_snapshots() {
    let db = Database::new().await;
    let first = new_task();
    let mut second = new_task();
    second =
        second.with_metadata(serde_json::from_value(json!({"correlation_id": "delivery", "custom": {"worker": true}})).unwrap());
    let repo = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let repo_tasks = vec![first.clone(), second.clone()];
    let events = [event("batch", 1)];
    repo.persist_with_tasks::<TestAggregate>(
        &events,
        Some(SerializedSnapshot {
            aggregate_id: "batch".into(),
            payload: json!({"state": "original"}),
            last_sequence: 1,
            revision: 1,
        }),
        &repo_tasks,
    )
    .await
    .unwrap();
    for msg in [&first, &second] {
        let saved = row(&db.pool, msg.id()).await;
        assert_eq!(saved.get::<i64, _>("aggregate_sequence"), 1);
        assert_eq!(saved.get::<Value, _>("payload"), *msg.payload());
        assert_eq!(saved.get::<Value, _>("metadata"), *msg.metadata());
        assert_eq!(saved.get::<String, _>("status"), "pending");
        assert_eq!(saved.get::<i32, _>("attempts"), 0);
    }
    // One event may atomically enqueue several explicit tasks.
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
    let invalid = new_task();
    // Inject a storage rejection without constructing an invalid public DTO.
    sqlx::query("ALTER TABLE transactional_outbox ADD CONSTRAINT reject_invalid_task CHECK (max_attempts < 10) NOT VALID")
        .execute(&db.pool)
        .await
        .unwrap();
    let bad_repo = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let bad_repo_tasks = vec![new_task().with_max_attempts(NonZeroU16::new(1).unwrap()), invalid];
    assert!(
        bad_repo
            .persist_with_tasks::<TestAggregate>(
                &[event("batch", 2)],
                Some(SerializedSnapshot {
                    aggregate_id: "batch".into(),
                    payload: json!({"state": "bad"}),
                    last_sequence: 2,
                    revision: 2
                }),
                &bad_repo_tasks
            )
            .await
            .is_err()
    );
    sqlx::query("ALTER TABLE transactional_outbox DROP CONSTRAINT reject_invalid_task")
        .execute(&db.pool)
        .await
        .unwrap();
    assert_eq!(repo.events::<TestAggregate>("batch").await.unwrap().len(), 1);
    assert_eq!(
        repo.snapshot::<TestAggregate>("batch").await.unwrap().unwrap().payload,
        json!({"state": "original"})
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
    let fresh = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let fresh_tasks = vec![new_task()];
    assert!(matches!(
        fresh
            .persist_with_tasks::<TestAggregate>(
                &[event("batch", 2)],
                Some(SerializedSnapshot {
                    aggregate_id: "batch".into(),
                    payload: json!({}),
                    last_sequence: 4,
                    revision: 4
                }),
                &fresh_tasks
            )
            .await,
        Err(PersistenceError::Conflict)
    ));
    let next_task = new_task();
    let next_repo = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    let next_repo_tasks = vec![next_task.clone()];
    next_repo
        .persist_with_tasks::<TestAggregate>(
            &[event("batch", 2)],
            Some(SerializedSnapshot {
                aggregate_id: "batch".into(),
                payload: json!({"state": "next"}),
                last_sequence: 2,
                revision: 2,
            }),
            &next_repo_tasks,
        )
        .await
        .unwrap();
    assert_eq!(row(&db.pool, next_task.id()).await.get::<i64, _>("aggregate_sequence"), 2);
    assert_eq!(
        next_repo.snapshot::<TestAggregate>("batch").await.unwrap().unwrap().payload,
        json!({"state": "next"})
    );
    assert!(
        fresh
            .persist_with_tasks::<TestAggregate>(&[], None, &fresh_tasks)
            .await
            .is_err()
    );
    assert_eq!(
        TaskSchedule::after(Duration::MAX),
        Err(cqrsx::task::TaskValidationError::DelayTooLong)
    );
    // A duplicate task ID is not an optimistic aggregate conflict. The whole
    // statement still rolls its otherwise valid new event back.
    assert!(matches!(
        repo.persist_with_tasks::<TestAggregate>(&[event("duplicate-task", 1)], None, &repo_tasks)
            .await,
        Err(PersistenceError::Unexpected(_))
    ));
    assert!(repo.events::<TestAggregate>("duplicate-task").await.unwrap().is_empty());
    db.close().await;
}

#[tokio::test]
async fn persists_events_and_snapshot_without_implicit_outbox() {
    let database = Database::new().await;
    let repository = PostgresEventRepositoryWithOutbox::new(database.pool.clone());
    let events = vec![serialized_event(1, "first")];
    let snapshot = json!({ "state": "recorded" });

    repository
        .persist::<StreamAggregate>(
            &events,
            Some(SerializedSnapshot {
                aggregate_id: AGGREGATE_ID.to_owned(),
                payload: snapshot.clone(),
                last_sequence: 1,
                revision: 1,
            }),
        )
        .await
        .expect("event batch must persist");

    assert_eq!(row_count(&database.pool, "transactional_outbox").await, 0);

    assert_eq!(
        repository
            .events::<StreamAggregate>(AGGREGATE_ID)
            .await
            .expect("events must load"),
        events.clone()
    );
    assert_eq!(
        repository
            .events_after::<StreamAggregate>(AGGREGATE_ID, 0)
            .await
            .expect("events after sequence one must load"),
        events
    );
    assert_eq!(
        repository
            .snapshot::<StreamAggregate>(AGGREGATE_ID)
            .await
            .expect("snapshot must load"),
        Some(SerializedSnapshot {
            aggregate_id: AGGREGATE_ID.to_owned(),
            payload: snapshot,
            last_sequence: 1,
            revision: 1,
        })
    );

    database.close().await;
}

#[tokio::test]
async fn an_empty_write_requires_no_live_connection() {
    let db = Database::new().await;
    let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    db.pool.close().await;
    repository.persist::<StreamAggregate>(&[], None).await.unwrap();
    db.close().await;
}

#[tokio::test]
async fn updates_snapshot_for_a_later_event_batch() {
    let database = Database::new().await;
    let repository = PostgresEventRepositoryWithOutbox::new(database.pool.clone());
    let first_snapshot = json!({ "state": "first" });
    let second_snapshot = json!({ "state": "second" });

    repository
        .persist::<StreamAggregate>(
            &[serialized_event(1, "first")],
            Some(SerializedSnapshot {
                aggregate_id: AGGREGATE_ID.to_owned(),
                payload: first_snapshot,
                last_sequence: 1,
                revision: 1,
            }),
        )
        .await
        .expect("first snapshot must persist");
    repository
        .persist::<StreamAggregate>(
            &[serialized_event(2, "second")],
            Some(SerializedSnapshot {
                aggregate_id: AGGREGATE_ID.to_owned(),
                payload: second_snapshot.clone(),
                last_sequence: 2,
                revision: 2,
            }),
        )
        .await
        .expect("second snapshot must persist");

    assert_eq!(
        repository
            .snapshot::<StreamAggregate>(AGGREGATE_ID)
            .await
            .expect("snapshot must load"),
        Some(SerializedSnapshot {
            aggregate_id: AGGREGATE_ID.to_owned(),
            payload: second_snapshot,
            last_sequence: 2,
            revision: 2,
        })
    );
    assert_eq!(row_count(&database.pool, "events").await, 2);
    assert_eq!(row_count(&database.pool, "transactional_outbox").await, 0);

    database.close().await;
}

#[tokio::test]
async fn maps_duplicate_event_sequence_to_a_conflict() {
    let database = Database::new().await;
    let repository = PostgresEventRepositoryWithOutbox::new(database.pool.clone());

    repository
        .persist::<StreamAggregate>(&[serialized_event(1, "original")], None)
        .await
        .expect("initial event must persist");
    let result = repository
        .persist::<StreamAggregate>(&[serialized_event(1, "duplicate")], None)
        .await;

    assert!(matches!(result, Err(PersistenceError::Conflict)));
    assert_eq!(row_count(&database.pool, "events").await, 1);
    assert_eq!(row_count(&database.pool, "transactional_outbox").await, 0);

    database.close().await;
}

#[tokio::test]
async fn rejects_multi_event_snapshot_only_and_tasks_without_event_before_writing() {
    let database = Database::new().await;
    let repository = PostgresEventRepositoryWithOutbox::new(database.pool.clone());

    assert!(
        repository
            .persist::<StreamAggregate>(
                &[serialized_event(1, "first"), serialized_event(2, "second")],
                Some(SerializedSnapshot {
                    aggregate_id: AGGREGATE_ID.to_owned(),
                    payload: json!({"state": "second"}),
                    last_sequence: 1,
                    revision: 1
                }),
            )
            .await
            .is_err()
    );
    assert!(
        repository
            .persist::<StreamAggregate>(
                &[],
                Some(SerializedSnapshot {
                    aggregate_id: AGGREGATE_ID.to_owned(),
                    payload: json!({"state": "snapshot"}),
                    last_sequence: 1,
                    revision: 1
                })
            )
            .await
            .is_err()
    );
    assert!(
        PostgresEventRepositoryWithOutbox::new(database.pool.clone())
            .persist_with_tasks::<StreamAggregate>(&[], None, &[new_task()])
            .await
            .is_err()
    );
    assert_eq!(row_count(&database.pool, "events").await, 0);
    assert_eq!(row_count(&database.pool, "transactional_outbox").await, 0);
    assert_eq!(repository.snapshot::<StreamAggregate>(AGGREGATE_ID).await.unwrap(), None);
    database.close().await;
}
