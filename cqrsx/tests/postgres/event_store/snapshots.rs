use super::*;

#[tokio::test]
async fn snapshot_conflict_rolls_back_single_events_and_batches() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    repo.persist::<Ledger>(
        &[event_for("a", 1, 4)],
        Some(SnapshotUpdate {
            aggregate_id: "a".into(),
            payload: json!([4]),
            last_sequence: 1,
            next_version: 1,
        }),
    )
    .await
    .unwrap();
    let before = database_state(&db.pool).await;
    for events in [vec![event_for("a", 2, 6)], vec![event_for("a", 2, 6), event_for("a", 3, 8)]] {
        assert!(matches!(
            repo.persist::<Ledger>(
                &events,
                Some(SnapshotUpdate {
                    aggregate_id: "a".into(),
                    payload: json!([4, 6]),
                    last_sequence: 3,
                    next_version: 3
                })
            )
            .await,
            Err(PersistenceError::OptimisticLockError)
        ));
        assert_eq!(database_state(&db.pool).await, before);
    }
    db.close().await;
}

#[tokio::test]
async fn snapshot_watermark_retains_tail_across_boundaries() {
    snapshot_histories(false).await;
}

#[tokio::test]
async fn snapshot_framework_commands_restore_the_full_history() {
    snapshot_histories(true).await;
}

async fn snapshot_histories(through_framework: bool) {
    let db = Database::new().await;
    for interval in [1, 2, 25] {
        for initial in [0, 3] {
            for count in [interval - 1, interval, interval + 1, interval * 3 + 1] {
                let id = format!("history-{interval}-{initial}-{count}");
                let repo = PostgresEventRepository::new(db.pool.clone());
                let seed: Vec<_> = (1..=initial).map(|i| event_for(&id, i, i as i32)).collect();
                repo.persist::<Ledger>(&seed, None).await.unwrap();
                let mut expected: Vec<i32> = (1..=initial as i32).collect();
                let mut watermark = 0;
                let mut version = 0;
                for size in [count, 1, interval + 2] {
                    let first = expected.len();
                    let changes: Vec<_> = (first + 1..=first + size).map(|i| i as i32).collect();
                    // Through the framework every change is a command of its own and a commit of its own;
                    // directly, the changes are one commit.
                    let commits: Vec<Vec<i32>> = if through_framework {
                        changes.iter().map(|change| vec![*change]).collect()
                    } else {
                        vec![changes]
                    };
                    for changes in commits {
                        let before = expected.len();
                        let store = PersistedEventStore::<_, Ledger>::new(PostgresEventRepository::new(db.pool.clone()))
                            .with_snapshots(std::num::NonZeroUsize::new(interval).unwrap());
                        if through_framework {
                            CqrsFramework::new(store, ()).execute(&id, changes[0]).await.unwrap();
                        } else {
                            let context = store.load_aggregate(&id).await.unwrap();
                            store
                                .commit(changes.iter().map(|v| Delta(*v)).collect(), context, HashMap::new(), &[])
                                .await
                                .unwrap();
                        }
                        expected.extend(changes);
                        if let Some(boundary) = (before + 1..=expected.len()).rfind(|n| n % interval == 0) {
                            watermark = boundary;
                            version += 1;
                        }
                        let snapshot = repo.snapshot::<Ledger>(&id).await.unwrap();
                        if watermark == 0 {
                            assert_eq!(snapshot, None);
                        } else {
                            assert_eq!(
                                snapshot,
                                Some(SerializedSnapshot {
                                    aggregate_id: id.clone(),
                                    aggregate: json!(&expected[..watermark]),
                                    last_sequence: watermark,
                                    snapshot_version: version
                                })
                            );
                        }
                        let store = PersistedEventStore::<_, Ledger>::new(PostgresEventRepository::new(db.pool.clone()))
                            .with_snapshots(std::num::NonZeroUsize::new(interval).unwrap());
                        let restored = store.load_aggregate(&id).await.unwrap();
                        let replay = PersistedEventStore::<_, Ledger>::new(PostgresEventRepository::new(db.pool.clone()))
                            .load_aggregate(&id)
                            .await
                            .unwrap();
                        assert_eq!(restored.aggregate.0, expected);
                        assert_eq!(replay.aggregate.0, expected);
                        assert_eq!(restored.current_sequence, expected.len());
                    }
                }
            }
        }
    }
    db.close().await;
}

#[tokio::test]
async fn incompatible_snapshot_falls_back_to_complete_history_in_postgres() {
    let db = Database::new().await;
    let repo = PostgresEventRepository::new(db.pool.clone());
    let events: Vec<_> = (1..=3).map(|i| event_for("legacy-snapshot", i, i as i32)).collect();
    repo.persist::<Ledger>(
        &events,
        Some(SnapshotUpdate {
            aggregate_id: "legacy-snapshot".into(),
            payload: json!({"legacy":"incompatible"}),
            last_sequence: 3,
            next_version: 1,
        }),
    )
    .await
    .unwrap();
    let store = PersistedEventStore::<_, Ledger>::new(repo).with_snapshots(std::num::NonZeroUsize::new(2).unwrap());
    let context = store.load_aggregate("legacy-snapshot").await.unwrap();
    assert_eq!(context.aggregate, Ledger(vec![1, 2, 3]));
    assert_eq!(context.current_sequence, 3);
    assert_eq!(context.snapshot_version, Some(1));
    assert_eq!(context.aggregate_id, "legacy-snapshot");
    store.commit(vec![Delta(4)], context, HashMap::new(), &[]).await.unwrap();
    let recovered = store.load_aggregate("legacy-snapshot").await.unwrap();
    assert_eq!(recovered.aggregate.0, [1, 2, 3, 4]);
    assert_eq!(recovered.snapshot_version, Some(2));
    assert_eq!(recovered.current_sequence, 4);
    db.close().await;
}

#[tokio::test]
async fn both_adapters_use_explicit_snapshot_watermarks_for_insert_and_update() {
    let db = Database::new().await;
    for outbox in [false, true] {
        let id = format!("explicit-{outbox}");
        let repo = PostgresEventRepository::new(db.pool.clone());
        repo.persist::<Ledger>(&[event_for(&id, 1, 1)], None).await.unwrap();
        for sequence in [2, 3] {
            let update = Some(SnapshotUpdate {
                aggregate_id: id.clone(),
                payload: json!((1..sequence).collect::<Vec<_>>()),
                last_sequence: sequence - 1,
                next_version: sequence - 1,
            });
            let events = [event_for(&id, sequence, sequence as i32)];
            if outbox {
                PostgresEventRepositoryWithOutbox::new(db.pool.clone())
                    .persist::<Ledger>(&events, update)
                    .await
                    .unwrap();
            } else {
                repo.persist::<Ledger>(&events, update).await.unwrap();
            }
            let restored = PersistedEventStore::<_, Ledger>::new(PostgresEventRepository::new(db.pool.clone()))
                .with_snapshots(std::num::NonZeroUsize::new(2).unwrap())
                .load_aggregate(&id)
                .await
                .unwrap();
            assert_eq!(restored.aggregate.0, (1..=sequence as i32).collect::<Vec<_>>());
            assert_eq!(restored.current_sequence, sequence);
        }
    }
    db.close().await;
}
