use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_settlements_apply_exactly_one_transition_for_the_same_token() {
    let db = Database::new().await;
    let queue = Arc::new(PostgresTaskQueue::new(db.pool.clone()));
    let task = new_task().with_metadata(json!({"operation": "same-effect"})).unwrap();
    enqueue(&db.pool, vec![task.clone()]).await;
    let claimed = claim(&queue).await.pop().unwrap();
    let mut gate = db.pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM transactional_outbox WHERE id = $1 FOR UPDATE")
        .bind(task.id())
        .fetch_one(&mut *gate)
        .await
        .unwrap();
    let mut settlements = tokio::task::JoinSet::new();
    for request in [
        SettlementRequest::Completed,
        SettlementRequest::Retry {
            delay: Duration::from_secs(300),
            reason: "retry winner".into(),
        },
        SettlementRequest::Failed {
            reason: "failure winner".into(),
        },
    ] {
        let queue = queue.clone();
        let claimed = claimed.clone();
        settlements.spawn(async move { (request.clone(), queue.settle(&claimed, request).await.unwrap()) });
    }
    wait_for_blocked_queries(&db.pool, "UPDATE transactional_outbox", 3).await;
    gate.commit().await.unwrap();
    let mut winner = None;
    let mut lost = 0;
    while let Some(result) = settlements.join_next().await {
        let (request, result) = result.unwrap();
        if result == SettlementResult::OwnershipLost {
            lost += 1;
        } else {
            assert!(winner.replace((request, result)).is_none());
        }
    }
    assert_eq!(lost, 2);
    match winner.unwrap() {
        (SettlementRequest::Completed, SettlementResult::Completed) => {
            assert_worker_state(&db.pool, task.id(), "completed", 1, None).await;
        }
        (SettlementRequest::Retry { .. }, SettlementResult::RetryScheduled) => {
            assert_worker_state(&db.pool, task.id(), "pending", 1, Some("retry winner")).await;
            assert!(claim(&queue).await.is_empty());
        }
        (SettlementRequest::Failed { .. }, SettlementResult::Failed(failure)) => {
            assert_eq!(failure.task_id, task.id());
            assert_eq!(failure.cause, TaskFailureCause::PermanentRejection);
            assert_worker_state(&db.pool, task.id(), "failed", 1, Some("failure winner")).await;
        }
        other => panic!("request and confirmed outcome disagree: {other:?}"),
    }
    let saved = row(&db.pool, task.id()).await;
    assert_eq!(&saved.get::<Value, _>("payload"), task.payload());
    assert_eq!(&saved.get::<Value, _>("metadata"), task.metadata());
    assert_eq!(
        queue.settle(&claimed, SettlementRequest::Completed).await.unwrap(),
        SettlementResult::OwnershipLost
    );
    db.close().await;
}

#[tokio::test]
async fn settlement_rechecks_lease_expiry_after_waiting_for_a_row_lock() {
    let db = Database::new().await;
    let queue = Arc::new(PostgresTaskQueue::new(db.pool.clone()));
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    let claimed = claim(&queue).await.pop().unwrap();
    let mut gate = db.pool.begin().await.unwrap();
    // The settling statement initially sees the old, valid lease, then waits.
    sqlx::query("UPDATE transactional_outbox SET locked_until = now() - interval '1 second' WHERE id = $1")
        .bind(task.id())
        .execute(&mut *gate)
        .await
        .unwrap();
    let settling = tokio::spawn({
        let queue = queue.clone();
        async move { queue.settle(&claimed, SettlementRequest::Completed).await.unwrap() }
    });
    wait_for_blocked_queries(&db.pool, "UPDATE transactional_outbox", 1).await;
    gate.commit().await.unwrap();
    assert_eq!(settling.await.unwrap(), SettlementResult::OwnershipLost);
    assert_worker_state(&db.pool, task.id(), "processing", 1, None).await;
    assert_eq!(queue.recover_expired(1).await.unwrap().retry_scheduled_count(), 1);
    assert_worker_state(&db.pool, task.id(), "pending", 1, Some("worker lease expired")).await;
    db.close().await;
}

#[tokio::test]
async fn a_lease_expiring_while_settlement_waits_on_an_unchanged_row_cannot_be_completed() {
    let db = Database::new().await;
    let queue = Arc::new(PostgresTaskQueue::new(db.pool.clone()));
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    let claimed = claim(&queue).await.pop().unwrap();
    // A short real lease expires while an unchanged row is held by another
    // transaction. PostgreSQL's own clock determines when to release the lock.
    sqlx::query("UPDATE transactional_outbox SET locked_until = clock_timestamp() + interval '1 second' WHERE id = $1")
        .bind(task.id())
        .execute(&db.pool)
        .await
        .unwrap();
    let mut gate = db.pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM transactional_outbox WHERE id = $1 FOR UPDATE")
        .bind(task.id())
        .fetch_one(&mut *gate)
        .await
        .unwrap();
    let settling = tokio::spawn({
        let queue = queue.clone();
        async move { queue.settle(&claimed, SettlementRequest::Completed).await.unwrap() }
    });
    wait_for_blocked_queries(&db.pool, "UPDATE transactional_outbox", 1).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let expired: bool =
                sqlx::query_scalar("SELECT locked_until <= clock_timestamp() FROM transactional_outbox WHERE id = $1")
                    .bind(task.id())
                    .fetch_one(&db.pool)
                    .await
                    .unwrap();
            if expired {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    gate.commit().await.unwrap();
    assert_eq!(settling.await.unwrap(), SettlementResult::OwnershipLost);
    assert_worker_state(&db.pool, task.id(), "processing", 1, None).await;
    assert_eq!(queue.recover_expired(1).await.unwrap().retry_scheduled_count(), 1);
    db.close().await;
}
