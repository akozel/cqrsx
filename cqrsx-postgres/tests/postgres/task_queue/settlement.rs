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

/// Every index scan of `transactional_outbox` in an `EXPLAIN (FORMAT JSON)` plan, by index name.
fn outbox_indexes(plan: &Value, found: &mut Vec<String>) {
    match plan {
        Value::Object(node) => {
            if node.get("Relation Name").and_then(Value::as_str) == Some("transactional_outbox") {
                let index = node.get("Index Name").and_then(Value::as_str).unwrap_or("(no index)");
                found.push(index.to_owned());
            }
            node.values().for_each(|child| outbox_indexes(child, found));
        }
        Value::Array(items) => items.iter().for_each(|child| outbox_indexes(child, found)),
        _ => {}
    }
}

#[tokio::test]
async fn settlement_finds_its_row_by_the_primary_key_while_closed_tasks_fill_the_expired_lease_index() {
    let db = Database::new().await;
    // One connection: the settle statement the queue prepares is then in this session's
    // `pg_prepared_statements`, with the text the queue really sends.
    let pool = pool_with(&db, PgPoolOptions::new().max_connections(1), StatementTimeout::Unlimited).await;
    let queue = PostgresTaskQueue::new(pool.clone());
    // A drain without VACUUM: every settled task leaves an entry in the expired-lease index, while the
    // statistics say that no task is in flight, so that index looks as good as empty.
    sqlx::query("ALTER TABLE transactional_outbox SET (autovacuum_enabled = false)")
        .execute(&pool)
        .await
        .unwrap();
    enqueue(&pool, (0..2_000).map(|_| new_task()).collect()).await;
    loop {
        let claimed = claim(&queue).await;
        if claimed.is_empty() {
            break;
        }
        for task in claimed {
            assert_eq!(
                queue.settle(&task, SettlementRequest::Completed).await.unwrap(),
                SettlementResult::Completed
            );
        }
    }
    sqlx::query("ANALYZE transactional_outbox").execute(&pool).await.unwrap();
    enqueue(&pool, vec![new_task()]).await;
    let task = claim(&queue).await.pop().unwrap();
    assert_eq!(
        queue.settle(&task, SettlementRequest::Completed).await.unwrap(),
        SettlementResult::Completed
    );
    let (name, types): (String, Vec<String>) = sqlx::query_as(
        "SELECT name, parameter_types::text[] FROM pg_prepared_statements WHERE statement LIKE '%WITH owned AS MATERIALIZED%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(types.len(), 5, "settle binds id, token, status, delay and error");
    let values = [
        format!("'{}'", task.attempt().task_id()),
        format!("'{}'", task.lease().lock_token()),
        "'completed'".to_owned(),
        "'0'".to_owned(),
        "NULL".to_owned(),
    ];
    let arguments = values
        .iter()
        .zip(&types)
        .map(|(value, kind)| format!("{value}::{kind}"))
        .collect::<Vec<_>>()
        .join(", ");
    // The plan of the first executions (custom) and the one the statement settles on later (generic).
    for mode in ["force_custom_plan", "force_generic_plan"] {
        let mut connection = pool.acquire().await.unwrap();
        sqlx::query(&format!("SET plan_cache_mode = {mode}"))
            .execute(&mut *connection)
            .await
            .unwrap();
        let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON) EXECUTE {name}({arguments})"))
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        let mut indexes = Vec::new();
        outbox_indexes(&plan, &mut indexes);
        assert!(
            indexes.iter().any(|index| index == "transactional_outbox_pkey")
                && !indexes.iter().any(|index| index == "transactional_outbox_expired_idx"),
            "{mode}: settle reads {indexes:?}, plan {plan}"
        );
    }
    pool.close().await;
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
