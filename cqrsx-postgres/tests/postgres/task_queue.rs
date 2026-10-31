use super::*;
use cqrsx::worker::queue::{
    QueueRequestError,
    settlement::{SettlementResult, TaskFailureCause},
};

#[path = "task_queue/claim.rs"]
mod claim;
#[path = "task_queue/execution.rs"]
mod execution;
#[path = "task_queue/failures.rs"]
mod failures;
#[path = "task_queue/recovery.rs"]
mod recovery;
#[path = "task_queue/settlement.rs"]
mod settlement;
#[path = "task_queue/timeouts.rs"]
mod timeouts;

#[cfg(target_pointer_width = "64")]
#[tokio::test]
async fn unrepresentable_requests_cannot_change_queue_state_or_consume_ownership() {
    let db = Database::new().await;
    let queue = PostgresTaskQueue::new(db.pool.clone());
    let task = new_task();
    enqueue(&db.pool, vec![task.clone()]).await;
    assert_eq!(
        queue
            .claim(&format(), "test", usize::MAX, Duration::from_secs(60))
            .await
            .unwrap_err(),
        TaskProcessingError::InvalidRequest(QueueRequestError::InvalidLimit)
    );
    assert_eq!(
        queue.recover_expired(usize::MAX).await.unwrap_err(),
        TaskProcessingError::InvalidRequest(QueueRequestError::InvalidLimit)
    );
    assert_worker_state(&db.pool, task.id(), "pending", 0, None).await;
    assert!(
        queue
            .claim(&format(), "test", 0, Duration::from_secs(60))
            .await
            .unwrap()
            .is_empty()
    );
    let claimed = claim(&queue).await.pop().unwrap();
    assert_eq!(
        queue
            .settle(
                &claimed,
                SettlementRequest::Retry {
                    delay: Duration::MAX,
                    reason: "invalid delay".into()
                }
            )
            .await
            .unwrap_err(),
        TaskProcessingError::InvalidRequest(QueueRequestError::InvalidRetryDelay)
    );
    assert_worker_state(&db.pool, task.id(), "processing", 1, None).await;
    assert_eq!(
        row(&db.pool, task.id()).await.get::<Option<Uuid>, _>("lock_token"),
        Some(claimed.lease().lock_token())
    );
    assert_eq!(
        queue.settle(&claimed, SettlementRequest::Completed).await.unwrap(),
        SettlementResult::Completed
    );
    db.close().await;
}
