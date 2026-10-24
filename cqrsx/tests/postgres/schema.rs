use super::*;

#[tokio::test]
async fn schema_rejects_inconsistent_direct_writes_and_accepts_valid_lifecycle_states() {
    let db = Database::new().await;
    let task = new_task().with_max_attempts(3).unwrap();
    enqueue(&db.pool, vec![task.clone()]).await;
    let original: Value = sqlx::query_scalar("SELECT to_jsonb(o) FROM transactional_outbox o WHERE id = $1")
        .bind(task.id())
        .fetch_one(&db.pool)
        .await
        .unwrap();
    let token = Uuid::new_v4();
    let deadline = Utc::now() + chrono::Duration::minutes(1);
    let finished = Utc::now();
    for invalid in [
        json!({"aggregate_sequence": 0}),
        json!({"task_type": ""}),
        json!({"task_version": 0}),
        json!({"payload": []}),
        json!({"metadata": "not-an-object"}),
        json!({"status": "unknown"}),
        json!({"attempts": -1}),
        json!({"max_attempts": 0}),
        json!({"attempts": 4}),
        json!({"attempts": 3}),
        json!({"locked_by": "unexpected-owner"}),
        json!({"lock_token": token}),
        json!({"locked_until": deadline}),
        json!({"finished_at": finished}),
        json!({"status": "processing", "attempts": 1, "lock_token": token, "locked_until": deadline}),
        json!({"status": "processing", "attempts": 1, "locked_by": "owner", "locked_until": deadline}),
        json!({"status": "processing", "attempts": 1, "locked_by": "owner", "lock_token": token}),
        json!({"status": "processing", "attempts": 1, "locked_by": "owner", "lock_token": token, "locked_until": deadline, "finished_at": finished}),
        json!({"status": "completed"}),
        json!({"status": "failed"}),
        json!({"status": "completed", "finished_at": finished, "locked_by": "owner", "lock_token": token, "locked_until": deadline}),
    ] {
        let mut candidate = original.clone();
        candidate
            .as_object_mut()
            .unwrap()
            .extend(invalid.as_object().unwrap().clone());
        candidate["id"] = json!(Uuid::new_v4());
        let error =
            sqlx::query("INSERT INTO transactional_outbox SELECT * FROM jsonb_populate_record(NULL::transactional_outbox, $1)")
                .bind(candidate)
                .execute(&db.pool)
                .await
                .expect_err("invalid durable state must be rejected even when bypassing the Rust API");
        assert_eq!(
            error.as_database_error().unwrap().code().as_deref(),
            Some("23514"),
            "{invalid}"
        );
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "rejected direct writes must leave no rows");
    for valid in [
        json!({"status": "pending", "attempts": 1}),
        json!({"status": "processing", "attempts": 1, "locked_by": "owner", "lock_token": token, "locked_until": deadline}),
        json!({"status": "completed", "attempts": 1, "finished_at": finished}),
        json!({"status": "failed", "attempts": 3, "finished_at": finished}),
    ] {
        let mut candidate = original.clone();
        candidate.as_object_mut().unwrap().extend(valid.as_object().unwrap().clone());
        candidate["id"] = json!(Uuid::new_v4());
        sqlx::query("INSERT INTO transactional_outbox SELECT * FROM jsonb_populate_record(NULL::transactional_outbox, $1)")
            .bind(candidate)
            .execute(&db.pool)
            .await
            .unwrap();
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM transactional_outbox")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 5);
    db.close().await;
}

/// The journal reads stay on indexes: feed pages and the head on `events_feed_idx`, the aggregate ids
/// on the primary key. 200 commits of two interleaved aggregate types give `tx` 200 distinct values,
/// so the page comes from the middle of a real journal, not from one transaction.
#[tokio::test]
async fn feed_pages_use_the_feed_index_and_key_pages_use_the_primary_key() {
    let db = Database::new().await;
    for commit in 1..=200i64 {
        sqlx::query(
            "INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) \
             SELECT CASE WHEN $1 % 2 = 0 THEN 'contract_ledger' ELSE 'other_ledger' END, \
                    'ledger-' || lpad((($1 - 1) / 2)::text, 3, '0'), n, 'delta', '1', '1', '{}' \
               FROM generate_series(1, 100) AS n",
        )
        .bind(commit)
        .execute(&db.pool)
        .await
        .unwrap();
    }
    sqlx::query("ANALYZE events").execute(&db.pool).await.unwrap();
    let middle: String = sqlx::query_scalar(
        "SELECT tx::text FROM (SELECT DISTINCT tx FROM events WHERE aggregate_type = 'contract_ledger' ORDER BY tx OFFSET 50 LIMIT 1) t",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    let feed = format!(
        "EXPLAIN (FORMAT JSON) SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata, tx::text AS position \
         FROM events WHERE aggregate_type = 'contract_ledger' AND tx > '{middle}'::xid8 AND tx < pg_snapshot_xmin(pg_current_snapshot()) \
         ORDER BY tx, aggregate_id, sequence LIMIT 100"
    );
    let tail = format!(
        "EXPLAIN (FORMAT JSON) SELECT aggregate_id FROM events WHERE aggregate_type = 'contract_ledger' AND tx = '{middle}'::xid8 \
         AND (aggregate_id, sequence) > ('ledger-050', 40) ORDER BY aggregate_id, sequence"
    );
    // The tail of one transaction is small: a bitmap scan of the index plus a sort of its rows is fine
    // there; a page, the head and the keys must come out of an index in order.
    for (query, index, sort_allowed) in [
        (feed.as_str(), "events_feed_idx", false),
        (tail.as_str(), "events_feed_idx", true),
        (
            // `AS position` matters: a bare `tx::text` would be named `tx` and ORDER BY would sort the text.
            "EXPLAIN (FORMAT JSON) SELECT tx::text AS position FROM events WHERE aggregate_type = 'contract_ledger' ORDER BY tx DESC LIMIT 1",
            "events_feed_idx",
            false,
        ),
        (
            "EXPLAIN (FORMAT JSON) SELECT DISTINCT aggregate_id FROM events WHERE aggregate_type = 'contract_ledger' AND aggregate_id > 'ledger-010' ORDER BY aggregate_id LIMIT 20",
            "events_pkey",
            false,
        ),
    ] {
        let plan: Value = sqlx::query_scalar(query).fetch_one(&db.pool).await.unwrap();
        let text = plan.to_string();
        assert!(text.contains(index), "{query}\n{text}");
        assert!(!text.contains("Seq Scan"), "{query}\n{text}");
        assert!(sort_allowed || !text.contains("\"Sort\""), "{query}\n{text}");
    }
    db.close().await;
}

#[tokio::test]
async fn queue_indexes_serve_ready_and_expired_queries_with_finished_history() {
    let db = Database::new().await;
    // Keep most history terminal, with a large future queue and a few due rows.
    sqlx::raw_sql(
        r#"
        INSERT INTO transactional_outbox
            (id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version,
             payload, status, available_at, finished_at)
        SELECT gen_random_uuid(), 'runtime_test', n::text, 1, 'dummy_task', 1,
            '{}', CASE WHEN n <= 10000 THEN 'completed' ELSE 'pending' END,
            now() + CASE WHEN n > 19990 THEN interval '-1 minute' ELSE interval '1 day' END,
            CASE WHEN n <= 10000 THEN now() ELSE NULL END
        FROM generate_series(1, 20000) AS n;
        INSERT INTO transactional_outbox
            (id, aggregate_type, aggregate_id, aggregate_sequence, task_type, task_version,
             payload, status, attempts, locked_by, lock_token, locked_until)
        SELECT gen_random_uuid(), 'runtime_test', n::text, 1, 'dummy_task', 1,
            '{}', 'processing', 1, 'expired-worker', gen_random_uuid(),
            now() + CASE WHEN n > 1990 THEN interval '-1 minute' ELSE interval '1 day' END
        FROM generate_series(1, 2000) AS n;
        ANALYZE transactional_outbox;
    "#,
    )
    .execute(&db.pool)
    .await
    .unwrap();
    for (query, index) in [
        (
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id FROM transactional_outbox WHERE status = 'pending' AND task_type = 'dummy_task' AND task_version = 1 AND available_at <= statement_timestamp() ORDER BY available_at, id LIMIT 4 FOR UPDATE SKIP LOCKED",
            "transactional_outbox_ready_idx",
        ),
        (
            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT id, attempts FROM transactional_outbox WHERE status = 'processing' AND locked_until <= statement_timestamp() ORDER BY locked_until, id LIMIT 4 FOR UPDATE SKIP LOCKED",
            "transactional_outbox_expired_idx",
        ),
    ] {
        let plan: Value = sqlx::query_scalar(query).fetch_one(&db.pool).await.unwrap();
        let text = plan.to_string();
        assert!(text.contains(index), "{text}");
        assert!(!text.contains("Seq Scan"), "{text}");
    }
    let queue = PostgresTaskQueue::new(db.pool.clone());
    db.pool.close().await;
    assert_eq!(
        queue.claim(&format(), "test", 1, Duration::from_secs(60)).await.unwrap_err(),
        TaskProcessingError::Unavailable
    );
    assert_eq!(queue.recover_expired(1).await.unwrap_err(), TaskProcessingError::Unavailable);
    let dummy = LeasedTask::new(
        TaskAttempt::new(Uuid::new_v4(), format(), json!({}), json!({}), 1).unwrap(),
        TaskLease::new(Uuid::new_v4(), tokio::time::Instant::now() + Duration::from_secs(60)),
    );
    assert_eq!(
        queue.settle(&dummy, SettlementRequest::Completed).await.unwrap_err(),
        TaskProcessingError::Unavailable
    );
    db.close().await;
}
