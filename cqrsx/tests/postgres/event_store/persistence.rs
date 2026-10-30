use super::*;

#[tokio::test]
async fn statement_and_deferred_commit_failures_restore_full_database_state() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    repo.persist::<Ledger>(
        &[event_for("existing", 1, 4)],
        Some(SnapshotUpdate {
            aggregate_id: "existing".into(),
            payload: json!([4]),
            last_sequence: 1,
            next_version: 1,
        }),
    )
    .await
    .unwrap();
    // A second event that violates a CHECK must roll back the first event too.
    sqlx::query("ALTER TABLE events ADD CONSTRAINT reject_payload CHECK (payload::text <> '99')")
        .execute(&db.pool)
        .await
        .unwrap();
    let before = database_state(&db.pool).await;
    assert!(matches!(
        repo.persist::<Ledger>(&[event_for("batch", 1, 2), event_for("batch", 2, 99)], None)
            .await,
        Err(PersistenceError::UnknownError(_))
    ));
    assert_eq!(database_state(&db.pool).await, before);
    sqlx::query("ALTER TABLE snapshots ADD CONSTRAINT reject_snapshot CHECK (payload::text <> '[99]')")
        .execute(&db.pool)
        .await
        .unwrap();
    for (id, version, sequence) in [("new", 1, 1), ("existing", 2, 2)] {
        assert!(
            repo.persist::<Ledger>(
                &[event_for(id, sequence, 3)],
                Some(SnapshotUpdate {
                    aggregate_id: id.into(),
                    payload: json!([99]),
                    last_sequence: version,
                    next_version: version
                })
            )
            .await
            .is_err()
        );
        assert_eq!(database_state(&db.pool).await, before);
    }
    sqlx::raw_sql("CREATE FUNCTION reject_event_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='40001', MESSAGE='deferred failure'; END $$; CREATE CONSTRAINT TRIGGER reject_event_commit AFTER INSERT ON events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_event_commit();").execute(&db.pool).await.unwrap();
    for snapshot in [
        None,
        Some(SnapshotUpdate {
            aggregate_id: String::from("deferred"),
            payload: json!([3]),
            last_sequence: 1,
            next_version: 1,
        }),
        Some(SnapshotUpdate {
            aggregate_id: "existing".into(),
            payload: json!([4, 3]),
            last_sequence: 2,
            next_version: 2,
        }),
    ] {
        let id = snapshot.as_ref().map_or("deferred", |s| s.aggregate_id.as_str());
        let sequence = if id == "existing" { 2 } else { 1 };
        assert!(matches!(
            repo.persist::<Ledger>(&[event_for(id, sequence, 3)], snapshot).await,
            Err(PersistenceError::UnknownError(_))
        ));
        assert_eq!(database_state(&db.pool).await, before);
    }
    for snapshot in [
        None,
        Some(SnapshotUpdate {
            aggregate_id: String::from("deferred"),
            payload: json!([3]),
            last_sequence: 1,
            next_version: 1,
        }),
        Some(SnapshotUpdate {
            aggregate_id: String::from("existing"),
            payload: json!([4, 3]),
            last_sequence: 2,
            next_version: 2,
        }),
    ] {
        let id = snapshot.as_ref().map_or("deferred", |s| s.aggregate_id.as_str());
        let sequence = if id == "existing" { 2 } else { 1 };
        let result = PostgresEventRepositoryWithOutbox::new(db.pool.clone())
            .persist_with_tasks::<Ledger>(&[event_for(id, sequence, 3)], snapshot, &[new_task()])
            .await;
        let Err(PersistenceError::UnknownError(error)) = result else {
            panic!("deferred COMMIT failure must remain technical")
        };
        let sqlx::Error::Database(error) = error.downcast_ref::<sqlx::Error>().unwrap() else {
            panic!("retain SQLSTATE cause")
        };
        assert_eq!(error.code().as_deref(), Some("40001"));
        assert_eq!(database_state(&db.pool).await, before);
    }
    sqlx::query("DROP TRIGGER reject_event_commit ON events")
        .execute(&db.pool)
        .await
        .unwrap();
    repo.persist::<Ledger>(&[event_for("usable", 1, 1)], None).await.unwrap();
    db.close().await;
}

#[tokio::test]
async fn cancellation_while_blocked_rolls_back_and_pool_can_be_reused() {
    let db = Database::new().await;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(db.pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let before = database_state(&db.pool).await;
    let mut lock = db.pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE snapshots IN SHARE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let repository = PostgresEventRepository::new(pool.clone());
    let mut task = tokio::spawn(async move {
        repository
            .persist::<Ledger>(
                &[event_for("cancelled", 1, 1)],
                Some(SnapshotUpdate {
                    aggregate_id: "cancelled".into(),
                    payload: json!([1]),
                    last_sequence: 1,
                    next_version: 1,
                }),
            )
            .await
    });
    wait_for_blocked_queries(&db.pool, "INSERT INTO snapshots", 1).await;
    task.abort();
    assert!(
        tokio::time::timeout(Duration::from_secs(10), &mut task)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
    lock.commit().await.unwrap();
    // Acquiring the only connection waits for SQLx's queued rollback to finish.
    sqlx::query("SELECT 1").execute(&pool).await.unwrap();
    assert_eq!(database_state(&db.pool).await, before);
    PostgresEventRepository::new(pool.clone())
        .persist::<Ledger>(&[event_for("next", 1, 2)], None)
        .await
        .unwrap();
    pool.close().await;
    db.close().await;
}

#[cfg(target_pointer_width = "64")]
#[tokio::test]
async fn bigint_sequences_and_snapshot_versions_round_trip_without_narrowing() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    for sequence in [i32::MAX as usize, i32::MAX as usize + 1, i64::MAX as usize] {
        let id = format!("number-{sequence}");
        let event = event_for(&id, sequence, 1);
        repo.persist::<Ledger>(
            std::slice::from_ref(&event),
            Some(SnapshotUpdate {
                aggregate_id: id.clone(),
                payload: json!([1]),
                last_sequence: sequence,
                next_version: 1,
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            repo.events::<Ledger>(&id).await.unwrap().as_slice(),
            std::slice::from_ref(&event)
        );
        assert_eq!(repo.events_after::<Ledger>(&id, sequence - 1).await.unwrap(), [event]);
        assert!(repo.events_after::<Ledger>(&id, sequence).await.unwrap().is_empty());
        let snapshot = repo.snapshot::<Ledger>(&id).await.unwrap().unwrap();
        assert_eq!(snapshot.last_sequence, sequence);
        // Seed only the previous version, without constructing an enormous history.
        sqlx::query("UPDATE snapshots SET current_snapshot=$1 WHERE aggregate_id=$2")
            .bind(i64::try_from(sequence - 1).unwrap())
            .bind(&id)
            .execute(&db.pool)
            .await
            .unwrap();
        repo.persist::<Ledger>(
            &[],
            Some(SnapshotUpdate {
                aggregate_id: id.clone(),
                payload: json!([2]),
                last_sequence: sequence,
                next_version: sequence,
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            repo.snapshot::<Ledger>(&id).await.unwrap().unwrap().snapshot_version,
            sequence
        );
    }
    let before = database_state(&db.pool).await;
    assert!(matches!(
        repo.persist::<Ledger>(
            &[event_for("zero-version", 1, 1)],
            Some(SnapshotUpdate {
                aggregate_id: "zero-version".into(),
                payload: json!([]),
                last_sequence: 0,
                next_version: 0
            })
        )
        .await,
        Err(PersistenceError::UnknownError(_))
    ));
    assert_eq!(database_state(&db.pool).await, before);
    let overflow = i64::MAX as usize + 1;
    for sequence in [overflow, usize::MAX] {
        for events in [
            vec![event_for("invalid", sequence, 1)],
            vec![event_for("invalid", 1, 1), event_for("invalid", sequence, 2)],
        ] {
            assert!(matches!(
                repo.persist::<Ledger>(&events, None).await,
                Err(PersistenceError::UnknownError(_))
            ));
            assert_eq!(database_state(&db.pool).await, before);
        }
        assert!(matches!(
            repo.persist::<Ledger>(
                &[event_for("invalid", 1, 1)],
                Some(SnapshotUpdate {
                    aggregate_id: "invalid".into(),
                    payload: json!([]),
                    last_sequence: sequence,
                    next_version: sequence
                })
            )
            .await,
            Err(PersistenceError::UnknownError(_))
        ));
        assert!(matches!(
            repo.events_after::<Ledger>("invalid", sequence).await,
            Err(PersistenceError::UnknownError(_))
        ));
        assert_eq!(database_state(&db.pool).await, before);
    }
    db.close().await;
}

#[tokio::test]
async fn custom_tables_distinguish_optimistic_keys_from_unrelated_unique_constraints() {
    let db = Database::new().await;
    sqlx::raw_sql("CREATE TABLE custom_events (LIKE events INCLUDING ALL); CREATE TABLE custom_snapshots (LIKE snapshots INCLUDING ALL); CREATE UNIQUE INDEX unrelated_event ON custom_events (event_type); CREATE UNIQUE INDEX unrelated_snapshot ON custom_snapshots ((payload::text));").execute(&db.pool).await.unwrap();
    let repo = PostgresEventRepository::new(db.pool.clone()).with_tables("custom_events", "custom_snapshots");
    let snapshot = |id: &str| {
        Some(SnapshotUpdate {
            aggregate_id: id.into(),
            payload: json!([1]),
            last_sequence: 1,
            next_version: 1,
        })
    };
    repo.persist::<Ledger>(&[event_for("one", 1, 1)], snapshot("one"))
        .await
        .unwrap();
    assert!(matches!(
        repo.persist::<Ledger>(&[event_for("two", 1, 2)], None).await,
        Err(PersistenceError::UnknownError(_))
    ));
    assert!(matches!(
        repo.persist::<Ledger>(&[event_for("one", 1, 1)], None).await,
        Err(PersistenceError::OptimisticLockError)
    ));
    sqlx::query("DROP INDEX unrelated_event").execute(&db.pool).await.unwrap();
    assert!(matches!(
        repo.persist::<Ledger>(&[event_for("two", 1, 2)], snapshot("two")).await,
        Err(PersistenceError::UnknownError(_))
    ));
    assert!(repo.events::<Ledger>("two").await.unwrap().is_empty());
    assert!(matches!(
        repo.persist::<Ledger>(&[event_for("one", 2, 2)], snapshot("one")).await,
        Err(PersistenceError::OptimisticLockError)
    ));
    assert_eq!(repo.events::<Ledger>("one").await.unwrap(), [event_for("one", 1, 1)]);
    assert_eq!(repo.snapshot::<Ledger>("one").await.unwrap().unwrap().snapshot_version, 1);
    db.close().await;
}

#[cfg(target_pointer_width = "64")]
#[tokio::test]
async fn every_out_of_range_write_field_is_rejected_before_acquiring_a_connection() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://localhost/unused")
        .unwrap();
    pool.close().await;
    let repo = PostgresEventRepository::new(pool);
    let event = |sequence| SerializedEvent {
        aggregate_id: "range".into(),
        sequence,
        aggregate_type: Ledger::TYPE.into(),
        event_type: "event".into(),
        event_version: "1".into(),
        payload: serde_json::json!({}),
        metadata: serde_json::json!({}),
    };
    let overflow = i64::MAX as usize + 1;
    for (events, snapshot) in [
        (vec![event(1), event(overflow)], None),
        (
            vec![event(1)],
            Some(SnapshotUpdate {
                aggregate_id: "range".into(),
                payload: serde_json::json!({}),
                last_sequence: overflow,
                next_version: 1,
            }),
        ),
        (
            vec![event(1)],
            Some(SnapshotUpdate {
                aggregate_id: "range".into(),
                payload: serde_json::json!({}),
                last_sequence: 1,
                next_version: overflow,
            }),
        ),
    ] {
        assert!(
            matches!(repo.persist::<Ledger>(&events,snapshot).await,Err(PersistenceError::UnknownError(source)) if source.is::<std::num::TryFromIntError>())
        );
    }
}
