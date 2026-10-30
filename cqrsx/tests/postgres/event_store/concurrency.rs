use super::*;

#[tokio::test]
async fn stress_same_version_has_exactly_one_complete_winner() {
    let db = Database::new().await;
    for mode in ["events", "first-snapshot", "update-snapshot"] {
        for writers in [2, 4, 8] {
            for round in 0..32 {
                let id = format!("{mode}-{writers}-{round}");
                let repository = PostgresEventRepository::new(db.pool.clone());
                let start = if mode == "update-snapshot" {
                    repository
                        .persist::<Ledger>(
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
                let barrier = Arc::new(Barrier::new(writers + 1));
                let mut tasks = JoinSet::new();
                for writer in 0..writers {
                    let barrier = barrier.clone();
                    let pool = db.pool.clone();
                    let id = id.clone();
                    tasks.spawn(async move {
                        let batch = vec![
                            event_for(&id, start, writer as i32 + 1),
                            event_for(&id, start + 1, writer as i32 + 101),
                        ];
                        let snapshot = match mode {
                            "events" => None,
                            "first-snapshot" => Some(SnapshotUpdate {
                                aggregate_id: id.clone(),
                                payload: json!([writer + 1, writer + 101]),
                                last_sequence: start + 1,
                                next_version: 1,
                            }),
                            _ => Some(SnapshotUpdate {
                                aggregate_id: id.clone(),
                                payload: json!([10, writer + 1, writer + 101]),
                                last_sequence: start + 1,
                                next_version: 2,
                            }),
                        };
                        barrier.wait().await;
                        let result = PostgresEventRepository::new(pool).persist::<Ledger>(&batch, snapshot).await;
                        (writer, batch, result)
                    });
                }
                tokio::time::timeout(Duration::from_secs(10), barrier.wait()).await.unwrap();
                wait_for_writers(&db.pool, "INSERT INTO events", writers as i64, &mut tasks).await;
                lock.commit().await.unwrap();
                let outcomes = finish_writers(&mut tasks).await;
                let winners: Vec<_> = outcomes.iter().filter(|(_, _, r)| r.is_ok()).collect();
                assert_eq!(winners.len(), 1, "{id}");
                for (_, _, result) in &outcomes {
                    if let Err(error) = result {
                        assert!(matches!(error, PersistenceError::OptimisticLockError), "{id}: {error:?}");
                    }
                }
                let (_, batch, _) = winners[0];
                let saved = repository.events::<Ledger>(&id).await.unwrap();
                let mut expected = if start == 2 { vec![event_for(&id, 1, 10)] } else { vec![] };
                expected.extend(batch.clone());
                assert_eq!(saved, expected, "{id}: winner payload, metadata and continuous sequences");
                let snapshot = repository.snapshot::<Ledger>(&id).await.unwrap();
                if mode == "events" {
                    assert_eq!(snapshot, None);
                } else {
                    assert_eq!(
                        snapshot,
                        Some(SerializedSnapshot {
                            aggregate_id: id.clone(),
                            aggregate: json!(expected.iter().map(|e| e.payload.as_i64().unwrap()).collect::<Vec<_>>()),
                            last_sequence: start + 1,
                            snapshot_version: if start == 2 { 2 } else { 1 }
                        })
                    );
                }
            }
        }
    }
    db.close().await;
}

#[tokio::test]
async fn stress_snapshot_cas_losers_roll_back_distinct_events() {
    let db = Database::new().await;
    for outbox in [false, true] {
        for writers in [2, 4, 8] {
            for round in 0..32 {
                let id = format!("cas-{outbox}-{writers}-{round}");
                let repo = PostgresEventRepository::new(db.pool.clone());
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
                let mut lock = db.pool.begin().await.unwrap();
                sqlx::query("LOCK TABLE snapshots IN SHARE MODE")
                    .execute(&mut *lock)
                    .await
                    .unwrap();
                let mut tasks = JoinSet::new();
                for writer in 0..writers {
                    let pool = db.pool.clone();
                    let id = id.clone();
                    tasks.spawn(async move {
                        let events = vec![event_for(&id, writer + 2, writer as i32)];
                        let snapshot = Some(SnapshotUpdate {
                            aggregate_id: id.clone(),
                            payload: json!([10, writer]),
                            last_sequence: writer + 2,
                            next_version: 2,
                        });
                        let result = if outbox {
                            PostgresEventRepositoryWithOutbox::new(pool)
                                .persist_with_tasks::<Ledger>(&events, snapshot, &[new_task()])
                                .await
                        } else {
                            PostgresEventRepository::new(pool).persist::<Ledger>(&events, snapshot).await
                        };
                        (writer, events, result)
                    });
                }
                wait_for_writers(&db.pool, "UPDATE snapshots", writers as i64, &mut tasks).await;
                lock.commit().await.unwrap();
                let outcomes = finish_writers(&mut tasks).await;
                let winners: Vec<_> = outcomes.iter().filter(|(_, _, r)| r.is_ok()).collect();
                assert_eq!(winners.len(), 1);
                for (_, _, result) in &outcomes {
                    if let Err(error) = result {
                        assert!(matches!(error, PersistenceError::OptimisticLockError));
                    }
                }
                let (winner, batch, _) = winners[0];
                assert_eq!(
                    repo.events::<Ledger>(&id).await.unwrap(),
                    [vec![event_for(&id, 1, 10)], batch.clone()].concat()
                );
                let snapshot = repo.snapshot::<Ledger>(&id).await.unwrap().unwrap();
                assert_eq!(snapshot.aggregate, json!([10, winner]));
                assert_eq!(snapshot.last_sequence, winner + 2);
                assert_eq!(snapshot.snapshot_version, 2);
                let task_count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox WHERE aggregate_id=$1")
                    .bind(&id)
                    .fetch_one(&db.pool)
                    .await
                    .unwrap();
                assert_eq!(task_count, i64::from(outbox));
            }
        }
    }
    db.close().await;
}

#[tokio::test]
async fn row_lock_on_one_aggregate_does_not_block_independent_id_or_type() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    for id in ["blocked", "independent"] {
        repo.persist::<Ledger>(
            &[event_for(id, 1, 10)],
            Some(SnapshotUpdate {
                aggregate_id: id.into(),
                payload: json!([10]),
                last_sequence: 1,
                next_version: 1,
            }),
        )
        .await
        .unwrap();
    }
    let mut lock = db.pool.begin().await.unwrap();
    sqlx::query("SELECT 1 FROM snapshots WHERE aggregate_type=$1 AND aggregate_id='blocked' FOR UPDATE")
        .bind(Ledger::TYPE)
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    let pool = db.pool.clone();
    let task = tokio::spawn(async move {
        PostgresEventRepository::new(pool)
            .persist::<Ledger>(
                &[event_for("blocked", 2, 20)],
                Some(SnapshotUpdate {
                    aggregate_id: "blocked".into(),
                    payload: json!([10, 20]),
                    last_sequence: 2,
                    next_version: 2,
                }),
            )
            .await
    });
    wait_for_blocked_queries(&db.pool, "UPDATE snapshots", 1).await;
    tokio::time::timeout(
        Duration::from_secs(10),
        repo.persist::<Ledger>(
            &[event_for("independent", 2, 30)],
            Some(SnapshotUpdate {
                aggregate_id: "independent".into(),
                payload: json!([10, 30]),
                last_sequence: 2,
                next_version: 2,
            }),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        repo.persist::<OtherLedger>(
            &[event_for("blocked", 1, 99)],
            Some(SnapshotUpdate {
                aggregate_id: "blocked".into(),
                payload: json!([99]),
                last_sequence: 1,
                next_version: 1,
            }),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    lock.commit().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        repo.events::<Ledger>("blocked").await.unwrap(),
        [event_for("blocked", 1, 10), event_for("blocked", 2, 20)]
    );
    assert_eq!(
        repo.events::<Ledger>("independent").await.unwrap(),
        [event_for("independent", 1, 10), event_for("independent", 2, 30)]
    );
    let other = repo.events::<OtherLedger>("blocked").await.unwrap();
    assert_eq!(other.len(), 1);
    assert_eq!(other[0].payload, json!(99));
    assert_eq!(other[0].aggregate_type, OtherLedger::TYPE);
    assert_eq!(
        repo.snapshot::<Ledger>("blocked").await.unwrap().unwrap().aggregate,
        json!([10, 20])
    );
    assert_eq!(
        repo.snapshot::<Ledger>("independent").await.unwrap().unwrap().aggregate,
        json!([10, 30])
    );
    assert_eq!(
        repo.snapshot::<OtherLedger>("blocked").await.unwrap().unwrap().aggregate,
        json!([99])
    );
    db.close().await;
}
