//! Event history and snapshot reads, including checked row decoding.
// Derived from postgres-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use super::{LedgerTotals, PostgresEventRepository, decode_count, decode_usize, encode_bigint, row_decode_error};
use crate::{
    cqrs::{
        aggregate::Aggregate,
        store::{
            PersistenceError,
            feed::{FeedEntry, Position},
            serialized_event::SerializedEvent,
            snapshot::SerializedSnapshot,
        },
    },
    postgres::error::PostgresRepositoryError,
};
use futures::TryStreamExt;
use sqlx::{
    Postgres, Row,
    postgres::{PgArguments, PgRow},
    query::Query,
};
use std::num::ParseIntError;

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

    /// Counts the events of aggregate type `A` above `after`, at most `cap` of them: how far a reader whose
    /// checkpoint stands at `after` is behind the journal. A position is a transaction id shared by the whole
    /// database, so the distance between two positions is not a number of events; this is. The count walks
    /// the feed index and stops at `cap`, which must be positive.
    ///
    /// ```
    /// use cqrsx::cqrs::{aggregate::Aggregate, store::{PersistenceError, feed::Position}};
    /// use cqrsx::postgres::event_repository::{DEFAULT_COUNT_AFTER_CAP, PostgresEventRepository};
    ///
    /// async fn lag<A: Aggregate>(repository: &PostgresEventRepository, checkpoint: Position) -> Result<u64, PersistenceError> {
    ///     repository.count_after::<A>(checkpoint, DEFAULT_COUNT_AFTER_CAP).await
    /// }
    /// ```
    pub async fn count_after<A: Aggregate>(&self, after: Position, cap: u64) -> Result<u64, PersistenceError> {
        if cap == 0 {
            return Err(PostgresRepositoryError::UnknownError("count cap must be positive".into()).into());
        }
        let cap = i64::try_from(cap).map_err(|error| PostgresRepositoryError::UnknownError(Box::new(error)))?;
        let row = sqlx::query(self.query_factory.count_after())
            .bind(A::TYPE)
            .bind(after.to_string())
            .bind(cap)
            .fetch_one(&self.pool)
            .await
            .map_err(PostgresRepositoryError::from)?;
        Ok(decode_count(row.try_get("events").map_err(row_decode_error)?)?)
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

    /// Runs an event query whose arguments are bound already and decodes its rows in order.
    pub(super) async fn fetch_events(
        &self,
        query: Query<'_, Postgres, PgArguments>,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let mut rows = query.fetch(&self.pool);
        let mut result = Vec::default();
        while let Some(row) = rows.try_next().await.map_err(PostgresRepositoryError::from)? {
            result.push(decode_event_row(&row)?);
        }
        Ok(result)
    }

    /// One page of the journal of `A`: the events after `after` below the snapshot horizon, and the
    /// rest of the transaction a full page ended in, so a commit is never cut in two.
    pub(super) async fn feed_page<A: Aggregate>(
        &self,
        after: Position,
        limit: usize,
    ) -> Result<Vec<FeedEntry>, PersistenceError> {
        let page_limit = page_limit(limit)?;
        let mut page = self
            .fetch_feed(
                sqlx::query(self.query_factory.feed_page())
                    .bind(A::TYPE)
                    .bind(after.to_string())
                    .bind(page_limit),
            )
            .await?;
        if page.len() == limit
            && let Some(last) = page.last()
        {
            let tail = sqlx::query(self.query_factory.feed_tail())
                .bind(A::TYPE)
                .bind(last.position.to_string())
                .bind(last.event.aggregate_id.clone())
                .bind(encode_bigint(last.event.sequence)?);
            page.extend(self.fetch_feed(tail).await?);
        }
        Ok(page)
    }

    /// The greatest position written for `A`; `Position::START` without events.
    pub(super) async fn journal_head<A: Aggregate>(&self) -> Result<Position, PersistenceError> {
        let row = sqlx::query(self.query_factory.head())
            .bind(A::TYPE)
            .fetch_optional(&self.pool)
            .await
            .map_err(PostgresRepositoryError::from)?;
        match row {
            None => Ok(Position::START),
            Some(row) => Ok(parse_position(
                &row.try_get::<String, _>("position").map_err(row_decode_error)?,
            )?),
        }
    }

    /// The aggregate ids of `A` above `after` in id order, at most `limit` of them.
    pub(super) async fn aggregate_ids_page<A: Aggregate>(
        &self,
        after: &str,
        limit: usize,
    ) -> Result<Vec<String>, PersistenceError> {
        let page_limit = page_limit(limit)?;
        let mut rows = sqlx::query(self.query_factory.aggregate_ids_after())
            .bind(A::TYPE)
            .bind(after)
            .bind(page_limit)
            .fetch(&self.pool);
        let mut ids = Vec::new();
        while let Some(row) = rows.try_next().await.map_err(PostgresRepositoryError::from)? {
            ids.push(row.try_get("aggregate_id").map_err(row_decode_error)?);
        }
        Ok(ids)
    }

    async fn fetch_feed(&self, query: Query<'_, Postgres, PgArguments>) -> Result<Vec<FeedEntry>, PersistenceError> {
        let mut rows = query.fetch(&self.pool);
        let mut result = Vec::default();
        while let Some(row) = rows.try_next().await.map_err(PostgresRepositoryError::from)? {
            result.push(decode_feed_row(&row)?);
        }
        Ok(result)
    }
}

/// A journal page needs a positive limit that fits `bigint`, checked before any statement.
fn page_limit(limit: usize) -> Result<i64, PostgresRepositoryError> {
    if limit == 0 {
        return Err(PostgresRepositoryError::UnknownError(
            "feed page limit must be positive".into(),
        ));
    }
    encode_bigint(limit)
}

pub(super) fn decode_feed_row(row: &PgRow) -> Result<FeedEntry, PostgresRepositoryError> {
    Ok(FeedEntry {
        position: parse_position(&row.try_get::<String, _>("position").map_err(row_decode_error)?)?,
        event: decode_event_row(row)?,
    })
}

/// An `xid8` travels as decimal text; anything else is a damaged row.
pub(super) fn parse_position(text: &str) -> Result<Position, PostgresRepositoryError> {
    text.parse()
        .map_err(|error: ParseIntError| PostgresRepositoryError::DeserializationError(Box::new(error)))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positions_and_page_limits_are_checked_before_any_statement() {
        assert_eq!(parse_position("42").unwrap(), Position::new(42));
        assert!(matches!(
            parse_position("x"),
            Err(PostgresRepositoryError::DeserializationError(_))
        ));
        assert_eq!(page_limit(1).unwrap(), 1);
        assert!(matches!(
            page_limit(0),
            Err(PostgresRepositoryError::UnknownError(error)) if error.to_string() == "feed page limit must be positive"
        ));
        #[cfg(target_pointer_width = "64")]
        assert!(matches!(
            page_limit(usize::MAX),
            Err(PostgresRepositoryError::UnknownError(_))
        ));
    }
}
