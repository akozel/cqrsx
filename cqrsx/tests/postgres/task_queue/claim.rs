use super::*;
#[tokio::test]
async fn ready_time_formats_and_skip_locked_distribute_without_overlap() {
    let db = Database::new().await;
    let queue = PostgresTaskQueue::new(db.pool.clone());
    let tasks: Vec<_> = (0..20).map(|_| new_task()).collect();
    let ids: HashSet<_> = tasks.iter().map(|m| m.id()).collect();
    let delayed = new_task().scheduled(TaskSchedule::After(Duration::from_secs(300))).unwrap();
    let absolute_time = DateTime::from_timestamp_micros((Utc::now() + chrono::Duration::hours(1)).timestamp_micros()).unwrap();
    let absolute = new_task().scheduled(TaskSchedule::At(absolute_time)).unwrap();
    let past = new_task()
        .scheduled(TaskSchedule::At(Utc::now() - chrono::Duration::minutes(1)))
        .unwrap();
    let unknown = NewTask::from_payload(TaskFormat::new(format().task_type(), 3).unwrap(), json!({"value":"runtime"})).unwrap();
    let unknown_type = NewTask::from_payload(TaskFormat::new("future_task", 1).unwrap(), json!({"value":"runtime"})).unwrap();
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
