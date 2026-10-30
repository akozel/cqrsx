//! Atomic expired-lease recovery in one statement and confirmed failure reporting.
use super::storage_error;
use crate::{
    postgres::task_queue::PostgresTaskQueue,
    task::{
        format::TaskFormat,
        queue::{
            QueueRequestError, TaskProcessingError,
            settlement::{LeaseRecoveryReport, TaskFailure, TaskFailureCause},
        },
    },
};
use sqlx::{FromRow, Row, postgres::PgRow};
struct RecoveryRow {
    id: uuid::Uuid,
    task_type: String,
    task_version: i32,
    attempts: i32,
    max_attempts: i32,
}

impl FromRow<'_, PgRow> for RecoveryRow {
    fn from_row(row: &PgRow) -> Result<Self, sqlx::Error> {
        Ok(Self {
            id: row.try_get("id")?,
            task_type: row.try_get("task_type")?,
            task_version: row.try_get("task_version")?,
            attempts: row.try_get("attempts")?,
            max_attempts: row.try_get("max_attempts")?,
        })
    }
}

impl RecoveryRow {
    fn into_failure(self) -> Result<TaskFailure, TaskProcessingError> {
        Ok(TaskFailure {
            task_id: self.id,
            format: TaskFormat::new(self.task_type, self.task_version).map_err(|_| TaskProcessingError::InvalidDelivery)?,
            attempt_number: self.attempts,
            cause: TaskFailureCause::LeaseExpired,
        })
    }
}
pub(super) async fn recover_expired(queue: &PostgresTaskQueue, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
    // One statement: lock the expired rows, return each to `pending` after its redelivery backoff or
    // to `failed` on its last attempt. The backoff is `redelivery_backoff` in SQL, so every row draws
    // its own jitter: min(5 s * 2^min(max(n - 1, 0), 6), 300 s) * U(0.8, 1.2), at most 300 s.
    let rows = sqlx::query_as::<_, RecoveryRow>(
        r#"
        WITH expired AS (
            SELECT id FROM transactional_outbox
            WHERE status = 'processing' AND locked_until <= statement_timestamp()
            ORDER BY locked_until, id LIMIT $1 FOR UPDATE SKIP LOCKED
        )
        UPDATE transactional_outbox AS o
        SET status = CASE WHEN o.attempts >= o.max_attempts THEN 'failed' ELSE 'pending' END,
            available_at = statement_timestamp() + LEAST(
                LEAST(5000 * power(2, LEAST(GREATEST(o.attempts - 1, 0), 6)), 300000) * (0.8 + random() * 0.4),
                300000) * interval '1 millisecond',
            finished_at = CASE WHEN o.attempts >= o.max_attempts THEN statement_timestamp() ELSE NULL END,
            last_error = 'worker lease expired', locked_by = NULL, lock_token = NULL, locked_until = NULL
        FROM expired WHERE o.id = expired.id
        RETURNING o.id, o.task_type, o.task_version, o.attempts, o.max_attempts
    "#,
    )
    .bind(i64::try_from(limit).map_err(|_| TaskProcessingError::from(QueueRequestError::InvalidLimit))?)
    .fetch_all(&queue.pool)
    .await
    .map_err(|error| storage_error(&error, "recover_expired"))?;
    let processed_count = rows.len();
    let failed = rows
        .into_iter()
        .filter(|row| row.attempts >= row.max_attempts)
        .map(RecoveryRow::into_failure)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(LeaseRecoveryReport::new(processed_count - failed.len(), failed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_recovery_format_is_an_invalid_delivery() {
        let row = RecoveryRow {
            id: uuid::Uuid::new_v4(),
            task_type: "test".into(),
            task_version: 0,
            attempts: 1,
            max_attempts: 1,
        };
        assert_eq!(row.into_failure(), Err(TaskProcessingError::InvalidDelivery));
    }
}
