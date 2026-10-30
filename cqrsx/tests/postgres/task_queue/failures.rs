use super::*;
use cqrsx::task::queue::settlement::{SettlementResult, TaskFailureCause};

#[tokio::test]
async fn deferred_commit_failures_roll_back_claim_settlement_and_recovery() {
    let db = Database::new().await;
    let pool = pool_with(&db, PgPoolOptions::new().max_connections(1), StatementTimeout::Default).await;
    let queue = PostgresTaskQueue::new(pool.clone());
    let task = new_task().with_max_attempts(NonZeroU16::new(1).unwrap());
    enqueue(&db.pool, vec![task.clone()]).await;
    sqlx::raw_sql(
        "CREATE FUNCTION reject_queue_commit() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN RAISE EXCEPTION USING ERRCODE = '40001', MESSAGE = 'injected deferred commit failure'; END $$;",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let mut owned = None;
    for operation in ["claim", "settle", "recovery"] {
        if operation == "settle" {
            owned = Some(claim(&queue).await.pop().unwrap());
        }
        if operation == "recovery" {
            expire(&db.pool, task.id()).await;
        }
        let before: Value = sqlx::query_scalar("SELECT to_jsonb(o) FROM transactional_outbox o WHERE id = $1")
            .bind(task.id())
            .fetch_one(&db.pool)
            .await
            .unwrap();
        // The UPDATE succeeds; PostgreSQL rejects the deferred constraint at the statement's
        // implicit commit, exercising a different path from statement failure.
        sqlx::raw_sql(
            "CREATE CONSTRAINT TRIGGER reject_queue_commit AFTER UPDATE ON transactional_outbox \
             DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_queue_commit();",
        )
        .execute(&db.pool)
        .await
        .unwrap();
        let result = match operation {
            "claim" => queue.claim(&format(), "test", 1, Duration::from_secs(60)).await.map(|_| ()),
            "settle" => queue
                .settle(owned.as_ref().unwrap(), SettlementRequest::Completed)
                .await
                .map(|_| ()),
            _ => queue.recover_expired(1).await.map(|_| ()),
        };
        assert_eq!(result, Err(TaskProcessingError::Unavailable), "{operation}");
        let after: Value = sqlx::query_scalar("SELECT to_jsonb(o) FROM transactional_outbox o WHERE id = $1")
            .bind(task.id())
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(after, before, "{operation} must not publish an uncommitted transition");
        let timeout: String = sqlx::query_scalar("SHOW statement_timeout").fetch_one(&pool).await.unwrap();
        assert_eq!(timeout, "4s", "a commit failure leaves the queue session's timeout as it was");
        sqlx::query("DROP TRIGGER reject_queue_commit ON transactional_outbox")
            .execute(&db.pool)
            .await
            .unwrap();
    }
    let recovered = queue.recover_expired(1).await.unwrap();
    assert_eq!(recovered.processed_count(), 1);
    assert_eq!(recovered.failures().len(), 1);
    assert_eq!(recovered.failures()[0].task_id, task.id());
    assert_eq!(recovered.failures()[0].cause, TaskFailureCause::LeaseExpired);
    assert_worker_state(&db.pool, task.id(), "failed", 1, Some("worker lease expired")).await;
    pool.close().await;
    db.close().await;
}

#[tokio::test]
async fn queue_statements_need_no_transaction_nor_the_right_to_change_settings() {
    let db = Database::new().await;
    // A restricted role that may read and update the queue but not call set_config: the queue's
    // timeout is a session setting of the pool, not something its statements install.
    sqlx::raw_sql(
        "CREATE ROLE queue_test LOGIN PASSWORD 'queue_test'; \
         GRANT USAGE ON SCHEMA public TO queue_test; \
         GRANT SELECT, UPDATE ON transactional_outbox TO queue_test; \
         REVOKE EXECUTE ON FUNCTION pg_catalog.set_config(text, text, boolean) FROM PUBLIC;",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let options = db
        .pool
        .connect_options()
        .as_ref()
        .clone()
        .username("queue_test")
        .password("queue_test");
    let pool = connect_pool_with(
        options,
        PgPoolOptions::new().max_connections(1),
        ConnectionPingPolicy::Default,
        StatementTimeout::Custom(Duration::from_millis(125)),
    )
    .await
    .unwrap();
    let timeout: String = sqlx::query_scalar("SHOW statement_timeout").fetch_one(&pool).await.unwrap();
    assert_eq!(timeout, "125ms");
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    let queue = PostgresTaskQueue::new(pool.clone());
    let owned = claim(&queue).await.pop().unwrap();
    assert_eq!(
        queue.settle(&owned, SettlementRequest::Completed).await.unwrap(),
        SettlementResult::Completed
    );
    assert_eq!(queue.recover_expired(1).await.unwrap().processed_count(), 0);
    assert_worker_state(&db.pool, task.id(), "completed", 1, None).await;
    pool.close().await;
    db.close().await;
}
