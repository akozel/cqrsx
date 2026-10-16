use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_recovery_skips_locked_rows_and_reports_each_failed_transition_once() {
    let db = Database::new().await;
    let queue = Arc::new(PostgresTaskQueue::new(db.pool.clone()));
    let tasks: Vec<_> = (0..24)
        .map(|index| new_task().with_max_attempts(if index % 2 == 0 { 1 } else { 3 }).unwrap())
        .collect();
    let exhausted: HashSet<_> = tasks
        .iter()
        .filter(|task| task.max_attempts() == 1)
        .map(NewTask::id)
        .collect();
    enqueue(&db.pool, tasks.clone()).await;
    let deliveries = claim(&queue).await;
    assert_eq!(deliveries.len(), tasks.len());
    sqlx::query("UPDATE transactional_outbox SET locked_until = now() - interval '1 second'")
        .execute(&db.pool)
        .await
        .unwrap();
    let mut gate = db.pool.begin().await.unwrap();
    let locked = tasks[0].id();
    sqlx::query("SELECT id FROM transactional_outbox WHERE id = $1 FOR UPDATE")
        .bind(locked)
        .fetch_one(&mut *gate)
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(6));
    let mut recoveries = tokio::task::JoinSet::new();
    for _ in 0..6 {
        let queue = queue.clone();
        let barrier = barrier.clone();
        recoveries.spawn(async move {
            barrier.wait().await;
            queue.recover_expired(4).await.unwrap()
        });
    }
    let mut processed = 0;
    let mut retried = 0;
    let mut failures = HashSet::new();
    while let Some(result) = recoveries.join_next().await {
        let report = result.unwrap();
        assert!(report.processed_count() <= 4);
        processed += report.processed_count();
        retried += report.retry_scheduled_count();
        for failure in report.failures() {
            assert!(
                failures.insert(failure.task_id),
                "failure must be reported by only one recovery transaction"
            );
            assert_eq!(failure.attempt_number, 1);
            assert_eq!(failure.cause, TaskFailureCause::LeaseExpired);
        }
    }
    assert_eq!(processed, 23);
    assert_eq!(retried, 12);
    assert_eq!(failures, exhausted.iter().copied().filter(|id| *id != locked).collect());
    assert_worker_state(&db.pool, locked, "processing", 1, None).await;
    gate.rollback().await.unwrap();
    let final_recovery = queue.recover_expired(4).await.unwrap();
    assert_eq!(final_recovery.processed_count(), 1);
    assert_eq!(final_recovery.failures()[0].task_id, locked);
    assert_eq!(queue.recover_expired(4).await.unwrap().processed_count(), 0);
    for task in &tasks {
        assert_worker_state(
            &db.pool,
            task.id(),
            if exhausted.contains(&task.id()) { "failed" } else { "pending" },
            1,
            Some("worker lease expired"),
        )
        .await;
        let saved = row(&db.pool, task.id()).await;
        assert_eq!(&saved.get::<Value, _>("payload"), task.payload());
        assert_eq!(&saved.get::<Value, _>("metadata"), task.metadata());
    }
    for delivery in &deliveries {
        assert_eq!(
            queue.settle(delivery, SettlementRequest::Completed).await.unwrap(),
            SettlementResult::OwnershipLost
        );
    }
    db.close().await;
}

#[tokio::test]
async fn a_mid_batch_recovery_error_rolls_back_prior_updates_and_preserves_all_leases() {
    let db = Database::new().await;
    let queue = PostgresTaskQueue::new(db.pool.clone());
    let retry = new_task();
    let rejected = NewTask::new(DummyTaskV1 {
        value: "reject_recovery".into(),
    })
    .unwrap()
    .with_max_attempts(1)
    .unwrap();
    enqueue(&db.pool, vec![retry.clone(), rejected.clone()]).await;
    let deliveries = claim(&queue).await;
    sqlx::query("UPDATE transactional_outbox SET locked_until = now() - CASE WHEN id = $1 THEN interval '2 seconds' ELSE interval '1 second' END")
        .bind(retry.id()).execute(&db.pool).await.unwrap();
    let mut before = Vec::new();
    for delivery in &deliveries {
        let saved: Value = sqlx::query_scalar("SELECT to_jsonb(o) FROM transactional_outbox o WHERE id = $1")
            .bind(delivery.attempt().task_id())
            .fetch_one(&db.pool)
            .await
            .unwrap();
        before.push((delivery.attempt().task_id(), saved));
    }
    sqlx::query("ALTER TABLE transactional_outbox ADD CONSTRAINT reject_recovery CHECK (status = 'processing' OR payload->>'value' <> 'reject_recovery') NOT VALID")
        .execute(&db.pool).await.unwrap();
    assert_eq!(queue.recover_expired(2).await.unwrap_err(), TaskProcessingError::Unavailable);
    for (id, expected) in before {
        let saved: Value = sqlx::query_scalar("SELECT to_jsonb(o) FROM transactional_outbox o WHERE id = $1")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(
            saved, expected,
            "a failed batch must retain its original ownership and schedule"
        );
    }
    sqlx::query("ALTER TABLE transactional_outbox DROP CONSTRAINT reject_recovery")
        .execute(&db.pool)
        .await
        .unwrap();
    let recovered = queue.recover_expired(2).await.unwrap();
    assert_eq!(recovered.retry_scheduled_count(), 1);
    assert_eq!(recovered.failures().len(), 1);
    assert_eq!(recovered.failures()[0].task_id, rejected.id());
    assert_worker_state(&db.pool, retry.id(), "pending", 1, Some("worker lease expired")).await;
    assert_worker_state(&db.pool, rejected.id(), "failed", 1, Some("worker lease expired")).await;
    db.close().await;
}

#[tokio::test]
async fn retries_failed_tasks_and_expired_leases_preserve_identity_and_fence_old_owners() {
    let db = Database::new().await;
    let queue = PostgresTaskQueue::new(db.pool.clone());
    let mut msg = new_task();
    msg = msg.with_max_attempts(2).unwrap();
    enqueue(&db.pool, vec![msg.clone()]).await;
    let first = claim(&queue).await.pop().unwrap();
    assert!(
        queue
            .settle(
                &first,
                SettlementRequest::Retry {
                    delay: Duration::from_secs(300),
                    reason: "try later".into()
                }
            )
            .await
            .unwrap()
            .is_applied()
    );
    assert!(claim(&queue).await.is_empty());
    assert_eq!(row(&db.pool, msg.id()).await.get::<String, _>("last_error"), "try later");
    make_ready(&db.pool, msg.id()).await;
    let second = claim(&queue).await.pop().unwrap();
    assert_eq!(second.attempt().number(), 2);
    assert_ne!(first.lease().lock_token(), second.lease().lock_token());
    assert!(!queue.settle(&first, SettlementRequest::Completed).await.unwrap().is_applied());
    expire(&db.pool, msg.id()).await;
    assert!(
        !queue
            .settle(&second, SettlementRequest::Completed)
            .await
            .unwrap()
            .is_applied()
    );
    assert_eq!(queue.recover_expired(10).await.unwrap().processed_count(), 1);
    assert_eq!(row(&db.pool, msg.id()).await.get::<String, _>("status"), "failed");
    assert!(
        row(&db.pool, msg.id())
            .await
            .get::<Option<DateTime<Utc>>, _>("finished_at")
            .is_some()
    );
    sqlx::query(REQUEUE_FAILED_OUTBOX_TASK)
        .bind(msg.id())
        .execute(&db.pool)
        .await
        .unwrap();
    let restarted = claim(&queue).await.pop().unwrap();
    assert_eq!(restarted.attempt().task_id(), msg.id());
    assert_eq!(restarted.attempt().number(), 1);
    expire(&db.pool, msg.id()).await;
    assert_eq!(queue.recover_expired(10).await.unwrap().processed_count(), 1);
    assert_eq!(queue.recover_expired(10).await.unwrap().processed_count(), 0);
    assert_eq!(row(&db.pool, msg.id()).await.get::<String, _>("status"), "pending");
    assert!(claim(&queue).await.is_empty());
    make_ready(&db.pool, msg.id()).await;
    let last = claim(&queue).await.pop().unwrap();
    assert!(
        queue
            .settle(
                &last,
                SettlementRequest::Retry {
                    delay: Duration::ZERO,
                    reason: "failed twice".into()
                }
            )
            .await
            .unwrap()
            .is_applied()
    );
    assert_eq!(row(&db.pool, msg.id()).await.get::<String, _>("status"), "failed");
    sqlx::query(REQUEUE_FAILED_OUTBOX_TASK)
        .bind(msg.id())
        .execute(&db.pool)
        .await
        .unwrap();
    let permanent = claim(&queue).await.pop().unwrap();
    assert!(
        queue
            .settle(
                &permanent,
                SettlementRequest::Failed {
                    reason: "broken payload".into()
                }
            )
            .await
            .unwrap()
            .is_applied()
    );
    assert_eq!(row(&db.pool, msg.id()).await.get::<i32, _>("attempts"), 1);
    let zero_delay = new_task();
    enqueue(&db.pool, vec![zero_delay.clone()]).await;
    let claimed = claim(&queue).await.pop().unwrap();
    let settled = queue
        .settle(
            &claimed,
            SettlementRequest::Retry {
                delay: Duration::ZERO,
                reason: "retry".into(),
            },
        )
        .await
        .unwrap();
    assert!(settled.is_applied());
    assert!(settled.failure().is_none());
    assert_eq!(claim(&queue).await[0].attempt().task_id(), zero_delay.id());
    db.close().await;
}
