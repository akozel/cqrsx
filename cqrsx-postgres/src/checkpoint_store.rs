//! The checkpoint rows of projections in PostgreSQL: `projection_checkpoints` and
//! `projection_rebuild_keys` (`PROJECTION_CHECKPOINTS_SCHEMA`). Every operation is one autocommitted
//! statement on the process's pool, bounded by its `StatementTimeout`; the writes of an owner are fenced
//! by its lease token, the lease compares against `clock_timestamp()` of the server.
use cqrsx::{
    journal::feed::Position,
    projection::checkpoint::{
        CheckpointError, CheckpointKey, CheckpointStatus, CheckpointStore, Claimed, LeaseToken, RebuildRequest, RebuildRequests,
        RebuildState,
    },
};
use sqlx::{PgPool, Row, postgres::PgRow};
use std::time::Duration;
use uuid::Uuid;

const REGISTER: &str =
    "INSERT INTO projection_checkpoints (projection_name, aggregate_type) VALUES ($1, $2) ON CONFLICT DO NOTHING";

const CLAIM: &str = r"
UPDATE projection_checkpoints
   SET owner = $3, lock_token = gen_random_uuid(),
       locked_until = clock_timestamp() + $4 * interval '1 millisecond', updated_at = statement_timestamp()
 WHERE projection_name = $1 AND aggregate_type = $2
   AND (locked_until IS NULL OR locked_until < clock_timestamp())
RETURNING lock_token, position::text AS position, failures, rebuild_cursor, rebuild_pace_ms, rebuilt";

const EXTEND: &str = r"
UPDATE projection_checkpoints SET locked_until = clock_timestamp() + $4 * interval '1 millisecond'
 WHERE projection_name = $1 AND aggregate_type = $2 AND lock_token = $3";

/// Only forward; a repeat of the row's position changes nothing and counts nothing.
const ADVANCE: &str = r"
UPDATE projection_checkpoints
   SET applied = applied + CASE WHEN position < $4::text::xid8 THEN $5 ELSE 0 END,
       position = $4::text::xid8, failures = 0, last_error = NULL, updated_at = statement_timestamp()
 WHERE projection_name = $1 AND aggregate_type = $2 AND lock_token = $3 AND position <= $4::text::xid8";

const RECORD_FAILURE: &str = r"
UPDATE projection_checkpoints SET failures = failures + 1, last_error = $4, updated_at = statement_timestamp()
 WHERE projection_name = $1 AND aggregate_type = $2 AND lock_token = $3";

/// One rebuild at a time: the flag is set only while none is requested or running (PostgreSQL 18 `old`).
const REQUEST_REBUILD: &str = r"
UPDATE projection_checkpoints
   SET rebuild_requested = rebuild_requested OR rebuild_cursor IS NULL,
       rebuild_pace_ms = CASE WHEN rebuild_requested OR rebuild_cursor IS NOT NULL THEN rebuild_pace_ms ELSE $3 END,
       updated_at = statement_timestamp()
 WHERE projection_name = $1 AND aggregate_type = $2
RETURNING (NOT old.rebuild_requested AND old.rebuild_cursor IS NULL) AS accepted";

/// Only for a registered projection; a repeated key is one request.
const REQUEST_REBUILD_KEY: &str = r"
WITH known AS (SELECT 1 FROM projection_checkpoints WHERE projection_name = $1 AND aggregate_type = $2),
     inserted AS (INSERT INTO projection_rebuild_keys (projection_name, aggregate_type, aggregate_id)
                  SELECT $1, $2, $3 FROM known ON CONFLICT DO NOTHING RETURNING 1)
SELECT EXISTS (SELECT 1 FROM known) AS known";

/// The flag with its pace and the first $4 requested keys, removed only through the owner's token; no row
/// means the lease is lost and nothing was removed.
const TAKE_REQUESTS: &str = r"
WITH taken AS (
    DELETE FROM projection_rebuild_keys k USING projection_checkpoints c
     WHERE c.projection_name = $1 AND c.aggregate_type = $2 AND c.lock_token = $3
       AND k.projection_name = c.projection_name AND k.aggregate_type = c.aggregate_type
       AND k.aggregate_id IN (SELECT aggregate_id FROM projection_rebuild_keys
                               WHERE projection_name = $1 AND aggregate_type = $2
                               ORDER BY requested_at, aggregate_id LIMIT $4)
    RETURNING k.aggregate_id, k.requested_at)
SELECT c.rebuild_requested, c.rebuild_pace_ms,
       coalesce((SELECT array_agg(aggregate_id ORDER BY requested_at, aggregate_id) FROM taken), '{}') AS keys
  FROM projection_checkpoints c
 WHERE c.projection_name = $1 AND c.aggregate_type = $2 AND c.lock_token = $3";

const BEGIN_REBUILD: &str = r"
UPDATE projection_checkpoints SET rebuild_requested = false, rebuild_cursor = '', rebuilt = 0, updated_at = statement_timestamp()
 WHERE projection_name = $1 AND aggregate_type = $2 AND lock_token = $3";

/// The cursor only forward; a repeat of the cursor counts nothing.
const ADVANCE_REBUILD: &str = r"
UPDATE projection_checkpoints
   SET rebuilt = rebuilt + CASE WHEN rebuild_cursor < $4 THEN $5 ELSE 0 END, rebuild_cursor = $4, updated_at = statement_timestamp()
 WHERE projection_name = $1 AND aggregate_type = $2 AND lock_token = $3 AND rebuild_cursor IS NOT NULL AND rebuild_cursor <= $4";

const FINISH_REBUILD: &str = r"
UPDATE projection_checkpoints SET rebuild_cursor = NULL, updated_at = statement_timestamp()
 WHERE projection_name = $1 AND aggregate_type = $2 AND lock_token = $3";

const RELEASE: &str = r"
UPDATE projection_checkpoints SET owner = NULL, lock_token = NULL, locked_until = NULL, updated_at = statement_timestamp()
 WHERE projection_name = $1 AND aggregate_type = $2 AND lock_token = $3";

const STATUS: &str = r"
SELECT projection_name, aggregate_type, position::text AS position, applied, rebuild_requested, rebuild_pace_ms, rebuild_cursor,
       rebuilt, owner, locked_until, failures, last_error, updated_at
  FROM projection_checkpoints
 WHERE $1::text IS NULL OR projection_name = $1
 ORDER BY projection_name, aggregate_type";

/// The checkpoint store over the process's pool. Needs PostgreSQL 18 (`RETURNING old`).
#[derive(Debug)]
pub struct PostgresCheckpointStore {
    pool: PgPool,
}

impl PostgresCheckpointStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn storage_error(operation: &'static str) -> impl FnOnce(sqlx::Error) -> CheckpointError {
    move |error| {
        crate::diagnostics::log_sqlx_failure(operation, &error);
        CheckpointError::Storage(Box::new(error))
    }
}

/// A lease in milliseconds for `interval` arithmetic.
fn lease_millis(lease: Duration) -> Result<f64, CheckpointError> {
    i64::try_from(lease.as_millis())
        .map(|millis| millis as f64)
        .map_err(|_| CheckpointError::Invalid("lease does not fit the row"))
}

/// A pace in milliseconds for the row's `integer`.
fn pace_millis(pace: Duration) -> Result<i32, CheckpointError> {
    i32::try_from(pace.as_millis()).map_err(|_| CheckpointError::Invalid("rebuild pace does not fit the row"))
}

fn count_in(value: u64, what: &'static str) -> Result<i64, CheckpointError> {
    i64::try_from(value).map_err(|_| CheckpointError::Invalid(what))
}

fn count_out(value: i64) -> Result<u64, CheckpointError> {
    u64::try_from(value).map_err(|error| CheckpointError::Corrupt(Box::new(error)))
}

fn failures_out(value: i32) -> Result<u32, CheckpointError> {
    u32::try_from(value).map_err(|error| CheckpointError::Corrupt(Box::new(error)))
}

fn position_out(text: &str) -> Result<Position, CheckpointError> {
    text.parse().map_err(|error| CheckpointError::Corrupt(Box::new(error)))
}

fn pace_out(millis: i32) -> Result<Duration, CheckpointError> {
    u64::try_from(millis)
        .map(Duration::from_millis)
        .map_err(|error| CheckpointError::Corrupt(Box::new(error)))
}

fn column<'r, T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>>(
    row: &'r PgRow,
    name: &str,
) -> Result<T, CheckpointError> {
    row.try_get(name).map_err(|error| CheckpointError::Corrupt(Box::new(error)))
}

fn decode_claim(row: &PgRow) -> Result<Claimed, CheckpointError> {
    let cursor: Option<String> = column(row, "rebuild_cursor")?;
    let rebuild = match cursor {
        Some(cursor) => Some(RebuildState {
            cursor,
            pace: pace_out(column(row, "rebuild_pace_ms")?)?,
            rebuilt: count_out(column(row, "rebuilt")?)?,
        }),
        None => None,
    };
    Ok(Claimed {
        token: LeaseToken::new(column::<Uuid>(row, "lock_token")?),
        position: position_out(&column::<String>(row, "position")?)?,
        failures: failures_out(column(row, "failures")?)?,
        rebuild,
    })
}

fn decode_status(row: &PgRow) -> Result<CheckpointStatus, CheckpointError> {
    Ok(CheckpointStatus {
        projection: column(row, "projection_name")?,
        aggregate_type: column(row, "aggregate_type")?,
        position: position_out(&column::<String>(row, "position")?)?,
        applied: count_out(column(row, "applied")?)?,
        rebuild_requested: column(row, "rebuild_requested")?,
        rebuild_pace: pace_out(column(row, "rebuild_pace_ms")?)?,
        rebuild_cursor: column(row, "rebuild_cursor")?,
        rebuilt: count_out(column(row, "rebuilt")?)?,
        owner: column(row, "owner")?,
        locked_until: column(row, "locked_until")?,
        failures: failures_out(column(row, "failures")?)?,
        last_error: column(row, "last_error")?,
        updated_at: column(row, "updated_at")?,
    })
}

impl PostgresCheckpointStore {
    /// One fenced write: `true` when the row was the owner's.
    async fn fenced(
        &self,
        operation: &'static str,
        query: sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>,
    ) -> Result<bool, CheckpointError> {
        let result = query.execute(&self.pool).await.map_err(storage_error(operation))?;
        Ok(result.rows_affected() == 1)
    }
}

impl CheckpointStore for PostgresCheckpointStore {
    async fn register(&self, key: &CheckpointKey) -> Result<(), CheckpointError> {
        sqlx::query(REGISTER)
            .bind(key.projection())
            .bind(key.aggregate_type())
            .execute(&self.pool)
            .await
            .map_err(storage_error("register"))?;
        Ok(())
    }

    async fn claim(&self, key: &CheckpointKey, owner: &str, lease: Duration) -> Result<Option<Claimed>, CheckpointError> {
        let lease = lease_millis(lease)?;
        let row = sqlx::query(CLAIM)
            .bind(key.projection())
            .bind(key.aggregate_type())
            .bind(owner)
            .bind(lease)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error("claim"))?;
        row.as_ref().map(decode_claim).transpose()
    }

    async fn extend(&self, key: &CheckpointKey, token: LeaseToken, lease: Duration) -> Result<bool, CheckpointError> {
        let lease = lease_millis(lease)?;
        self.fenced(
            "extend",
            sqlx::query(EXTEND)
                .bind(key.projection())
                .bind(key.aggregate_type())
                .bind(token.value())
                .bind(lease),
        )
        .await
    }

    async fn advance(&self, key: &CheckpointKey, token: LeaseToken, to: Position, applied: u64) -> Result<bool, CheckpointError> {
        let applied = count_in(applied, "applied count does not fit the row")?;
        self.fenced(
            "advance",
            sqlx::query(ADVANCE)
                .bind(key.projection())
                .bind(key.aggregate_type())
                .bind(token.value())
                .bind(to.to_string())
                .bind(applied),
        )
        .await
    }

    async fn record_failure(&self, key: &CheckpointKey, token: LeaseToken, error: &str) -> Result<bool, CheckpointError> {
        self.fenced(
            "record_failure",
            sqlx::query(RECORD_FAILURE)
                .bind(key.projection())
                .bind(key.aggregate_type())
                .bind(token.value())
                .bind(error),
        )
        .await
    }

    async fn request_rebuild(&self, key: &CheckpointKey, pace: Duration) -> Result<RebuildRequest, CheckpointError> {
        let pace = pace_millis(pace)?;
        let row = sqlx::query(REQUEST_REBUILD)
            .bind(key.projection())
            .bind(key.aggregate_type())
            .bind(pace)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error("request_rebuild"))?;
        let Some(row) = row else {
            return Err(CheckpointError::Invalid("projection is not registered"));
        };
        Ok(if column::<bool>(&row, "accepted")? {
            RebuildRequest::Accepted
        } else {
            RebuildRequest::AlreadyRunning
        })
    }

    async fn request_rebuild_key(&self, key: &CheckpointKey, aggregate_id: &str) -> Result<(), CheckpointError> {
        let row = sqlx::query(REQUEST_REBUILD_KEY)
            .bind(key.projection())
            .bind(key.aggregate_type())
            .bind(aggregate_id)
            .fetch_one(&self.pool)
            .await
            .map_err(storage_error("request_rebuild_key"))?;
        if column::<bool>(&row, "known")? {
            Ok(())
        } else {
            Err(CheckpointError::Invalid("projection is not registered"))
        }
    }

    async fn take_requests(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        limit: usize,
    ) -> Result<Option<RebuildRequests>, CheckpointError> {
        let limit = i64::try_from(limit).map_err(|_| CheckpointError::Invalid("request limit does not fit the row"))?;
        let row = sqlx::query(TAKE_REQUESTS)
            .bind(key.projection())
            .bind(key.aggregate_type())
            .bind(token.value())
            .bind(limit)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage_error("take_requests"))?;
        let Some(row) = row else {
            return Ok(None);
        };
        let requested: bool = column(&row, "rebuild_requested")?;
        let pace = pace_out(column(&row, "rebuild_pace_ms")?)?;
        Ok(Some(RebuildRequests {
            rebuild: requested.then_some(pace),
            keys: column(&row, "keys")?,
        }))
    }

    async fn begin_rebuild(&self, key: &CheckpointKey, token: LeaseToken) -> Result<bool, CheckpointError> {
        self.fenced(
            "begin_rebuild",
            sqlx::query(BEGIN_REBUILD)
                .bind(key.projection())
                .bind(key.aggregate_type())
                .bind(token.value()),
        )
        .await
    }

    async fn advance_rebuild(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        cursor: &str,
        keys: u64,
    ) -> Result<bool, CheckpointError> {
        let keys = count_in(keys, "rebuilt count does not fit the row")?;
        self.fenced(
            "advance_rebuild",
            sqlx::query(ADVANCE_REBUILD)
                .bind(key.projection())
                .bind(key.aggregate_type())
                .bind(token.value())
                .bind(cursor)
                .bind(keys),
        )
        .await
    }

    async fn finish_rebuild(&self, key: &CheckpointKey, token: LeaseToken) -> Result<bool, CheckpointError> {
        self.fenced(
            "finish_rebuild",
            sqlx::query(FINISH_REBUILD)
                .bind(key.projection())
                .bind(key.aggregate_type())
                .bind(token.value()),
        )
        .await
    }

    async fn release(&self, key: &CheckpointKey, token: LeaseToken) -> Result<(), CheckpointError> {
        self.fenced(
            "release",
            sqlx::query(RELEASE)
                .bind(key.projection())
                .bind(key.aggregate_type())
                .bind(token.value()),
        )
        .await?;
        Ok(())
    }

    async fn status(&self, projection: Option<&str>) -> Result<Vec<CheckpointStatus>, CheckpointError> {
        let rows = sqlx::query(STATUS)
            .bind(projection)
            .fetch_all(&self.pool)
            .await
            .map_err(storage_error("status"))?;
        rows.iter().map(decode_status).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_that_do_not_fit_the_row_are_invalid_and_values_the_row_cannot_hold_are_corrupt() {
        assert!(matches!(
            lease_millis(Duration::MAX),
            Err(CheckpointError::Invalid("lease does not fit the row"))
        ));
        assert_eq!(lease_millis(Duration::from_millis(1500)).unwrap(), 1500.0);
        assert!(matches!(
            pace_millis(Duration::from_millis(u64::from(u32::MAX))),
            Err(CheckpointError::Invalid("rebuild pace does not fit the row"))
        ));
        assert_eq!(pace_millis(Duration::from_millis(80)).unwrap(), 80);
        assert!(matches!(
            count_in(u64::MAX, "too big"),
            Err(CheckpointError::Invalid("too big"))
        ));
        assert_eq!(count_in(7, "x").unwrap(), 7);
        assert!(matches!(count_out(-1), Err(CheckpointError::Corrupt(_))));
        assert!(matches!(failures_out(-1), Err(CheckpointError::Corrupt(_))));
        assert!(matches!(position_out("x"), Err(CheckpointError::Corrupt(_))));
        assert!(matches!(pace_out(-1), Err(CheckpointError::Corrupt(_))));
        assert_eq!(
            (
                count_out(3).unwrap(),
                failures_out(2).unwrap(),
                position_out("9").unwrap(),
                pace_out(5).unwrap()
            ),
            (3, 2, Position::new(9), Duration::from_millis(5))
        );
    }

    #[tokio::test]
    async fn an_unreachable_store_fails_every_operation_with_a_logged_storage_error() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        pool.close().await;
        let store = PostgresCheckpointStore::new(pool);
        let key = CheckpointKey::new("halls", "Concert");
        let token = LeaseToken::new(Uuid::new_v4());
        let (_, log) = crate::diagnostics::test_support::capture(async {
            let errors = [
                store.register(&key).await.err(),
                store.claim(&key, "a", Duration::from_secs(1)).await.err(),
                store.extend(&key, token, Duration::from_secs(1)).await.err(),
                store.advance(&key, token, Position::new(1), 1).await.err(),
                store.record_failure(&key, token, "halls: connection").await.err(),
                store.request_rebuild(&key, Duration::ZERO).await.err(),
                store.request_rebuild_key(&key, "k").await.err(),
                store.take_requests(&key, token, 1).await.err(),
                store.begin_rebuild(&key, token).await.err(),
                store.advance_rebuild(&key, token, "k", 1).await.err(),
                store.finish_rebuild(&key, token).await.err(),
                store.release(&key, token).await.err(),
                store.status(None).await.err(),
            ];
            assert!(
                errors.iter().all(|error| matches!(error, Some(CheckpointError::Storage(_)))),
                "{errors:?}"
            );
            // Checked before any statement: no log line for these.
            assert!(matches!(
                store.claim(&key, "a", Duration::MAX).await,
                Err(CheckpointError::Invalid(_))
            ));
            assert!(matches!(
                store.advance(&key, token, Position::new(1), u64::MAX).await,
                Err(CheckpointError::Invalid(_))
            ));
            assert!(matches!(
                store.take_requests(&key, token, usize::MAX).await,
                Err(CheckpointError::Invalid(_))
            ));
            assert!(matches!(
                store.advance_rebuild(&key, token, "k", u64::MAX).await,
                Err(CheckpointError::Invalid(_))
            ));
        })
        .await;
        assert_eq!(log.matches("storage operation failed").count(), 13, "{log}");
        for operation in [
            "register",
            "claim",
            "extend",
            "advance",
            "record_failure",
            "request_rebuild",
            "request_rebuild_key",
            "take_requests",
            "begin_rebuild",
            "advance_rebuild",
            "finish_rebuild",
            "release",
            "status",
        ] {
            assert!(log.contains(&format!("operation=\"{operation}\"")), "{operation}: {log}");
        }
        assert!(log.contains("pool_closed"));
    }
}
