use super::*;

#[tokio::test]
async fn stress_outbox_winners_publish_only_their_own_event_snapshot_and_task() {
    let db = Database::new().await;
    for mode in 0..3 {
        for writers in [2, 4, 8] {
            for round in 0..32 {
                let id = format!("outbox-{mode}-{writers}-{round}");
                let repo = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
                let sequence = if mode == 2 {
                    repo.persist::<Ledger>(
                        &[event_for(&id, 1, 10)],
                        Some(SnapshotUpdate {
                            aggregate_id: id.clone(),
                            payload: json!([10]),
                            last_sequence: 1,
                            next_version: 1,
                        }),
                    )
                    .await
                    .unwrap();
                    2
                } else {
                    1
                };
                let mut lock = db.pool.begin().await.unwrap();
                sqlx::query("LOCK TABLE events IN SHARE MODE")
                    .execute(&mut *lock)
                    .await
                    .unwrap();
                let mut tasks = JoinSet::new();
                for writer in 0..writers {
                    let pool = db.pool.clone();
                    let id = id.clone();
                    tasks.spawn(async move {
                        let value = writer + 1;
                        let event = event_for(&id, sequence, value);
                        let task = NewTask::new(DummyTaskV1 {
                            value: value.to_string(),
                        })
                        .unwrap()
                        .with_metadata(json!({"writer":value}))
                        .unwrap();
                        let snapshot = match mode {
                            0 => None,
                            1 => Some(SnapshotUpdate {
                                aggregate_id: id.clone(),
                                payload: json!([value]),
                                last_sequence: 1,
                                next_version: 1,
                            }),
                            _ => Some(SnapshotUpdate {
                                aggregate_id: id.clone(),
                                payload: json!([10, value]),
                                last_sequence: 2,
                                next_version: 2,
                            }),
                        };
                        let result = PostgresEventRepositoryWithOutbox::new(pool)
                            .with_outbox(vec![task.clone()])
                            .persist::<Ledger>(std::slice::from_ref(&event), snapshot)
                            .await;
                        (event, task, result)
                    });
                }
                wait_for_writers(&db.pool, "written_events AS", i64::from(writers), &mut tasks).await;
                lock.commit().await.unwrap();
                let outcomes = finish_writers(&mut tasks).await;
                let winners: Vec<_> = outcomes.iter().filter(|(_, _, r)| r.is_ok()).collect();
                assert_eq!(winners.len(), 1, "{id}: {outcomes:?}");
                for (_, _, result) in &outcomes {
                    if let Err(error) = result {
                        assert!(matches!(error, PersistenceError::OptimisticLockError));
                    }
                }
                let (event, task, _) = winners[0];
                let mut expected = if mode == 2 { vec![event_for(&id, 1, 10)] } else { vec![] };
                expected.push(event.clone());
                assert_eq!(repo.get_events::<Ledger>(&id).await.unwrap(), expected);
                let rows = sqlx::query("SELECT * FROM transactional_outbox WHERE aggregate_type=$1 AND aggregate_id=$2")
                    .bind(Ledger::TYPE)
                    .bind(&id)
                    .fetch_all(&db.pool)
                    .await
                    .unwrap();
                assert_eq!(rows.len(), 1);
                let saved = &rows[0];
                assert_eq!(saved.get::<Uuid, _>("id"), task.id());
                assert_eq!(saved.get::<Value, _>("payload"), *task.payload());
                assert_eq!(saved.get::<Value, _>("metadata"), *task.metadata());
                assert_eq!(saved.get::<i64, _>("aggregate_sequence"), sequence as i64);
                assert_eq!(saved.get::<String, _>("status"), "pending");
                let snapshot = repo.get_snapshot::<Ledger>(&id).await.unwrap();
                if mode == 0 {
                    assert_eq!(snapshot, None);
                } else {
                    assert_eq!(
                        snapshot,
                        Some(SerializedSnapshot {
                            aggregate_id: id,
                            aggregate: json!(expected.iter().map(|e| e.payload.as_i64().unwrap()).collect::<Vec<_>>()),
                            last_sequence: sequence,
                            snapshot_version: mode as usize
                        })
                    );
                }
            }
        }
    }
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_writes_keep_the_winning_event_snapshot_and_tasks_consistent() {
    let db = Database::new().await;
    for (id, sequence, snapshot_version) in [
        ("events-only", 1, None),
        ("insert-snapshot", 1, Some(1)),
        ("update-snapshot", 2, Some(2)),
    ] {
        let reader = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
        if sequence == 2 {
            reader
                .persist::<TestAggregate>(
                    &[event(id, 1)],
                    Some(SnapshotUpdate {
                        aggregate_id: id.into(),
                        payload: json!({"winner": "initial"}),
                        last_sequence: 1,
                        next_version: 1,
                    }),
                )
                .await
                .unwrap();
        }
        let mut gate = db.pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE events IN SHARE MODE")
            .execute(&mut *gate)
            .await
            .unwrap();
        let mut writes = tokio::task::JoinSet::new();
        let mut candidates = Vec::new();
        for name in ["left", "right"] {
            let task = NewTask::new(DummyTaskV1 { value: name.into() }).unwrap();
            let mut candidate = event(id, sequence);
            candidate.payload = json!({"Recorded": {"value": name}});
            let snapshot = snapshot_version.map(|version| SnapshotUpdate {
                aggregate_id: id.into(),
                payload: json!({"winner": name}),
                last_sequence: sequence,
                next_version: version,
            });
            let repository = PostgresEventRepositoryWithOutbox::new(db.pool.clone()).with_outbox(vec![task.clone()]);
            candidates.push((name, task, candidate.clone()));
            writes.spawn(async move { (name, repository.persist::<TestAggregate>(&[candidate], snapshot).await) });
        }
        wait_for_blocked_queries(&db.pool, "written_events", 2).await;
        gate.commit().await.unwrap();
        let mut winner = None;
        let mut conflicts = 0;
        while let Some(result) = writes.join_next().await {
            let (name, result) = result.unwrap();
            match result {
                Ok(()) => assert!(winner.replace(name).is_none()),
                Err(PersistenceError::OptimisticLockError) => conflicts += 1,
                other => panic!("unexpected competing write result: {other:?}"),
            }
        }
        assert_eq!(conflicts, 1);
        let winner = winner.unwrap();
        let events = reader.get_events::<TestAggregate>(id).await.unwrap();
        assert_eq!(events.len(), sequence);
        let (_, winning_task, winning_event) = candidates.iter().find(|(name, _, _)| *name == winner).unwrap();
        assert_eq!(events.last().unwrap(), winning_event);
        let saved = row(&db.pool, winning_task.id()).await;
        assert_eq!(saved.get::<String, _>("aggregate_id"), id);
        assert_eq!(saved.get::<i64, _>("aggregate_sequence"), sequence as i64);
        assert_eq!(&saved.get::<Value, _>("payload"), winning_task.payload());
        let published: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM transactional_outbox WHERE aggregate_id = $1")
            .bind(id)
            .fetch_all(&db.pool)
            .await
            .unwrap();
        assert_eq!(published, vec![winning_task.id()], "the losing write must publish no tasks");
        let snapshot = reader.get_snapshot::<TestAggregate>(id).await.unwrap();
        if let Some(version) = snapshot_version {
            let snapshot = snapshot.unwrap();
            assert_eq!(snapshot.last_sequence, sequence);
            assert_eq!(snapshot.snapshot_version, version);
            assert_eq!(snapshot.aggregate, json!({"winner": winner}));
        } else {
            assert!(snapshot.is_none());
        }
    }
    db.close().await;
}

/// `xact_rollback` of the database once the session's pending statistics are published.
async fn rollbacks(session: &PgPool, observer: &PgPool) -> i64 {
    // The session's backend flushes its statistics before it answers the next statement.
    sqlx::query("SELECT pg_stat_force_next_flush()")
        .execute(session)
        .await
        .unwrap();
    sqlx::query_scalar("SELECT xact_rollback FROM pg_stat_database WHERE datname = current_database()")
        .fetch_one(observer)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_conflict_without_a_snapshot_is_a_result_and_with_a_new_snapshot_still_an_error() {
    let db = Database::new().await;
    // One connection, so every write below is one backend whose statistics are flushed on demand.
    let session = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(db.pool.connect_options().as_ref().clone())
        .await
        .unwrap();
    let start = rollbacks(&session, &db.pool).await;
    // The counter does see a failed statement.
    sqlx::query("SELECT 1 / 0").execute(&session).await.unwrap_err();
    assert_eq!(rollbacks(&session, &db.pool).await, start + 1);

    let id = "conflict-without-error";
    PostgresEventRepositoryWithOutbox::new(db.pool.clone())
        .persist::<TestAggregate>(&[event(id, 1)], None)
        .await
        .unwrap();
    let task = NewTask::new(DummyTaskV1 { value: "loser".into() }).unwrap();
    let loser = PostgresEventRepositoryWithOutbox::new(session.clone()).with_outbox(vec![task.clone()]);
    assert!(matches!(
        loser.persist::<TestAggregate>(&[event(id, 1)], None).await,
        Err(PersistenceError::OptimisticLockError)
    ));
    assert_eq!(
        rollbacks(&session, &db.pool).await,
        start + 1,
        "a taken sequence without a snapshot must not fail the statement"
    );
    let tasks: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox WHERE id = $1")
        .bind(task.id())
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(tasks, 0);

    // With a first snapshot the conflict stays an error: the snapshot rolls back with the event.
    let loser = PostgresEventRepositoryWithOutbox::new(session.clone()).with_outbox(vec![task.clone()]);
    let snapshot = SnapshotUpdate {
        aggregate_id: id.into(),
        payload: json!({"winner": "loser"}),
        last_sequence: 1,
        next_version: 1,
    };
    assert!(matches!(
        loser.persist::<TestAggregate>(&[event(id, 1)], Some(snapshot)).await,
        Err(PersistenceError::OptimisticLockError)
    ));
    assert_eq!(rollbacks(&session, &db.pool).await, start + 2);
    let reader = PostgresEventRepositoryWithOutbox::new(db.pool.clone());
    assert_eq!(reader.get_snapshot::<TestAggregate>(id).await.unwrap(), None);
    assert_eq!(reader.get_events::<TestAggregate>(id).await.unwrap().len(), 1);
    let tasks: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox WHERE id = $1")
        .bind(task.id())
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(tasks, 0);
    session.close().await;
    db.close().await;
}
