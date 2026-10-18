//! Event history and snapshot reads, including checked row decoding.
// Derived from postgres-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use super::{LedgerTotals, PostgresEventRepository, decode_count, decode_usize};
use crate::{
    cqrs::{
        aggregate::Aggregate,
        store::{PersistenceError, serialized_event::SerializedEvent, snapshot::SerializedSnapshot},
    },
    postgres::error::PostgresRepositoryError,
};
use futures::TryStreamExt;
use sqlx::{Row, postgres::PgRow};

impl PostgresEventRepository {
    /// Counts the saved events of aggregate type `A` and the distinct aggregates holding them, in
    /// one query. Other aggregate types in the same table are not counted.
    ///
    /// ```
    /// use cqrsx::cqrs::{aggregate::Aggregate, store::PersistenceError};
    /// use cqrsx::postgres::event_repository::{LedgerTotals, PostgresEventRepository};
    ///
    /// async fn history_size<A: Aggregate>(repository: &PostgresEventRepository) -> Result<LedgerTotals, PersistenceError> {
    ///     repository.totals::<A>().await
    /// }
    /// ```
    pub async fn totals<A: Aggregate>(&self) -> Result<LedgerTotals, PersistenceError> {
        let row = sqlx::query(self.query_factory.count_events())
            .bind(A::TYPE)
            .fetch_one(&self.pool)
            .await
            .map_err(PostgresRepositoryError::from)?;
        Ok(LedgerTotals {
            events: decode_count(row.try_get("events").map_err(row_decode_error)?)?,
            aggregates: decode_count(row.try_get("aggregates").map_err(row_decode_error)?)?,
        })
    }

    pub(super) async fn load_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        let Some(row) = sqlx::query(self.query_factory.select_snapshot())
            .bind(A::TYPE)
            .bind(aggregate_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(PostgresRepositoryError::from)?
        else {
            return Ok(None);
        };
        Ok(Some(decode_snapshot_row(&row)?))
    }

    pub(super) async fn select_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        query: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let mut rows = sqlx::query(query).bind(A::TYPE).bind(aggregate_id).fetch(&self.pool);
        let mut result = Vec::default();
        while let Some(row) = rows.try_next().await.map_err(PostgresRepositoryError::from)? {
            result.push(decode_event_row(&row)?);
        }
        Ok(result)
    }
}

pub(super) fn decode_event_row(row: &PgRow) -> Result<SerializedEvent, PostgresRepositoryError> {
    Ok(SerializedEvent::new(
        row.try_get("aggregate_id").map_err(row_decode_error)?,
        decode_usize(row.try_get("sequence").map_err(row_decode_error)?)?,
        row.try_get("aggregate_type").map_err(row_decode_error)?,
        row.try_get("event_type").map_err(row_decode_error)?,
        row.try_get("event_version").map_err(row_decode_error)?,
        row.try_get("payload").map_err(row_decode_error)?,
        row.try_get("metadata").map_err(row_decode_error)?,
    ))
}

pub(super) fn decode_snapshot_row(row: &PgRow) -> Result<SerializedSnapshot, PostgresRepositoryError> {
    Ok(SerializedSnapshot {
        aggregate_id: row.try_get("aggregate_id").map_err(row_decode_error)?,
        aggregate: row.try_get("payload").map_err(row_decode_error)?,
        last_sequence: decode_usize(row.try_get("last_sequence").map_err(row_decode_error)?)?,
        snapshot_version: decode_usize(row.try_get("current_snapshot").map_err(row_decode_error)?)?,
    })
}

fn row_decode_error(error: sqlx::Error) -> PostgresRepositoryError {
    PostgresRepositoryError::DeserializationError(Box::new(error))
}
