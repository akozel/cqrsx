//! PostgreSQL task ownership. Claim, settlement and recovery are one autocommitted statement each,
//! bounded by the pool's statement timeout, and keep token/expiry fencing.
mod claim;
mod recovery;
mod settlement;

use cqrsx::task::format::TaskFormat;
use cqrsx::worker::queue::{
    QueueRequestError, TaskProcessingError, TaskQueue,
    lease::LeasedTask,
    settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult},
};
use sqlx::PgPool;
use std::time::Duration;

/// The task queue over `transactional_outbox` on the process's pool. Its statements are bounded by
/// that pool's `StatementTimeout` (a session setting, see `pool`); with `Unlimited`, a
/// statement the runtime's storage timeout has given up on may still finish later, which the token
/// and expiry fencing keeps safe.
#[derive(Debug)]
pub struct PostgresTaskQueue {
    pool: PgPool,
}

impl PostgresTaskQueue {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn storage_error(error: &sqlx::Error, operation: &'static str) -> TaskProcessingError {
    crate::diagnostics::log_sqlx_failure(operation, error);
    TaskProcessingError::Unavailable
}

fn milliseconds(duration: Duration, invalid: QueueRequestError) -> Result<f64, TaskProcessingError> {
    i64::try_from(duration.as_millis())
        .map(|n| n as f64)
        .map_err(|_| TaskProcessingError::from(invalid))
}

impl TaskQueue for PostgresTaskQueue {
    async fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
        claim::claim(self, format, owner, limit, lease_duration).await
    }
    async fn settle(&self, task: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
        settlement::settle(self, task, request).await
    }
    async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
        recovery::recover_expired(self, limit).await
    }
}

/// A task a worker holds, for the unit tests of the queue and its settlement.
#[cfg(test)]
pub(crate) mod test_support {
    use cqrsx::{
        task::{attempt::TaskAttempt, format::TaskFormat},
        worker::queue::lease::{LeasedTask, TaskLease},
    };
    use std::time::Duration;
    use uuid::Uuid;

    pub(crate) fn claimed() -> LeasedTask {
        LeasedTask::new(
            TaskAttempt::try_new(
                Uuid::new_v4(),
                TaskFormat::try_new("dummy_task", 1).unwrap(),
                serde_json::json!({"value": "runtime"}),
                serde_json::json!({"correlation_id": "test"}),
                2,
            )
            .unwrap(),
            TaskLease::new(Uuid::new_v4(), tokio::time::Instant::now() + Duration::from_secs(60)),
        )
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    #[tokio::test]
    async fn unavailable_queue_logs_operation_and_cause_before_erasing_source() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        pool.close().await;
        let queue = PostgresTaskQueue::new(pool);
        let task = test_support::claimed();
        let (_, log) = crate::diagnostics::test_support::capture(async {
            assert_eq!(
                queue
                    .claim(task.attempt().format(), "private_owner", 1, Duration::from_secs(60))
                    .await
                    .unwrap_err(),
                TaskProcessingError::Unavailable
            );
            assert_eq!(
                queue.settle(&task, SettlementRequest::Completed).await.unwrap_err(),
                TaskProcessingError::Unavailable
            );
            assert_eq!(queue.recover_expired(1).await.unwrap_err(), TaskProcessingError::Unavailable);
        })
        .await;
        assert_eq!(log.matches("storage operation failed").count(), 3);
        for operation in ["claim", "settle", "recover_expired"] {
            assert!(log.contains(operation));
        }
        assert!(log.contains("pool_closed"));
        assert!(!log.contains("private_"));
    }
}
