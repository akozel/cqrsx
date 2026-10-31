use super::*;
#[tokio::test]
async fn ready_time_formats_and_skip_locked_distribute_without_overlap() {
    let db = Database::new().await;
    let queue = PostgresTaskQueue::new(db.pool.clone());
    let tasks: Vec<_> = (0..20).map(|_| new_task()).collect();
    let ids: HashSet<_> = tasks.iter().map(|m| m.id()).collect();
    let delayed = new_task()
        .with_schedule(TaskSchedule::After(Duration::from_secs(300)))
        .unwrap();
    let absolute_time = DateTime::from_timestamp_micros((Utc::now() + chrono::Duration::hours(1)).timestamp_micros()).unwrap();
    let absolute = new_task().with_schedule(TaskSchedule::At(absolute_time)).unwrap();
    let past = new_task()
        .with_schedule(TaskSchedule::At(Utc::now() - chrono::Duration::minutes(1)))
        .unwrap();
    let unknown = NewTask::from_payload(
        TaskFormat::try_new(format().task_type(), 3).unwrap(),
        json!({"value":"runtime"}),
    )
    .unwrap();
    let unknown_type = NewTask::from_payload(TaskFormat::try_new("future_task", 1).unwrap(), json!({"value":"runtime"})).unwrap();
    enqueue(&db.pool, tasks).await;
    enqueue(
        &db.pool,
        vec![
            delayed.clone(),
            absolute.clone(),
            past.clone(),
            unknown.clone(),
            unknown_type.clone(),
        ],
    )
    .await;
    let saved = row(&db.pool, delayed.id()).await;
    let delta = saved.get::<DateTime<Utc>, _>("available_at") - saved.get::<DateTime<Utc>, _>("created_at");
    assert_eq!(delta.num_seconds(), 300);
    assert_eq!(
        row(&db.pool, absolute.id()).await.get::<DateTime<Utc>, _>("available_at"),
        absolute_time
    );
    // Hold a row lock independently: another consumer must still make progress.
    let locked_id = *ids.iter().next().unwrap();
    let mut tx = db.pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM transactional_outbox WHERE id = $1 FOR UPDATE")
        .bind(locked_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let supported_format = format();
    let (left, right) = tokio::join!(
        queue.claim(&supported_format, "left", 10, Duration::from_secs(60)),
        queue.claim(&supported_format, "right", 10, Duration::from_secs(60))
    );
    let claimed: Vec<_> = left.unwrap().into_iter().chain(right.unwrap()).collect();
    assert_eq!(claimed.len(), 20);
    let claimed_ids: HashSet<_> = claimed.iter().map(|m| m.attempt().task_id()).collect();
    assert_eq!(claimed_ids.len(), 20);
    assert!(claimed_ids.contains(&past.id()));
    assert!(!claimed_ids.contains(&locked_id));
    tx.rollback().await.unwrap();
    assert_eq!(claim(&queue).await[0].attempt().task_id(), locked_id);
    assert!(claim(&queue).await.is_empty());
    for task in &claimed {
        assert!(queue.settle(task, SettlementRequest::Completed).await.unwrap().is_applied());
        assert!(!queue.settle(task, SettlementRequest::Completed).await.unwrap().is_applied());
    }
    make_ready(&db.pool, delayed.id()).await;
    assert_eq!(claim(&queue).await[0].attempt().task_id(), delayed.id());
    for id in [unknown.id(), unknown_type.id()] {
        let saved = row(&db.pool, id).await;
        assert_eq!(saved.get::<String, _>("status"), "pending");
        assert_eq!(saved.get::<i32, _>("attempts"), 0);
    }
    assert!(queue.claim(&format(), "test", 1, Duration::ZERO).await.is_err());
    assert!(queue.claim(&format(), "test", 1, Duration::from_nanos(1)).await.is_err());
    assert!(queue.claim(&format(), "test", 1, Duration::MAX).await.is_err());
    db.close().await;
}

#[tokio::test]
async fn an_invalid_row_stays_leased_for_recovery_and_the_rest_of_the_batch_is_delivered() {
    let db = Database::new().await;
    let queue = PostgresTaskQueue::new(db.pool.clone());
    let valid = new_task();
    let poisoned = new_task().with_max_attempts(NonZeroU16::new(1).unwrap());
    enqueue(&db.pool, vec![valid.clone(), poisoned.clone()]).await;
    // A payload that is not an object is not a valid delivery. The schema's check forbids it; a
    // drifted schema without that check is what the claim has to survive.
    sqlx::query("ALTER TABLE transactional_outbox DROP CONSTRAINT transactional_outbox_payload_check")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE transactional_outbox SET payload = '[]'::json WHERE id = $1")
        .bind(poisoned.id())
        .execute(&db.pool)
        .await
        .unwrap();
    let claimed = claim(&queue).await;
    assert_eq!(
        claimed.iter().map(|task| task.attempt().task_id()).collect::<Vec<_>>(),
        vec![valid.id()]
    );
    // The claim is committed: the invalid row is leased, its attempt counted, and lease recovery
    // retires it instead of every poll handing it out first again.
    assert_worker_state(&db.pool, poisoned.id(), "processing", 1, None).await;
    expire(&db.pool, poisoned.id()).await;
    let recovered = queue.recover_expired(10).await.unwrap();
    assert_eq!(recovered.failures().len(), 1);
    assert_eq!(recovered.failures()[0].task_id, poisoned.id());
    assert_worker_state(&db.pool, poisoned.id(), "failed", 1, Some("worker lease expired")).await;
    assert!(
        queue
            .settle(&claimed[0], SettlementRequest::Completed)
            .await
            .unwrap()
            .is_applied()
    );
    db.close().await;
}
