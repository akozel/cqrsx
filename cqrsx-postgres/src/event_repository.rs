// Derived from postgres-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see cqrsx/doc/components.md#upstream-attribution and cqrsx-postgres/licenses/.
mod read;
mod sql_query;
mod write;

use self::sql_query::SqlQueryFactory;
use crate::{diagnostics::storage_failure, error::PostgresRepositoryError};
use cqrsx::{
    domain::aggregate::Aggregate,
    journal::{
        PersistenceError,
        feed::{FeedEntry, Position},
        repository::PersistedEventRepository,
        serialized_event::SerializedEvent,
        snapshot::SerializedSnapshot,
    },
};
use sqlx::{Pool, Postgres};
use std::sync::Arc;

const DEFAULT_EVENT_TABLE: &str = "events";
const DEFAULT_SNAPSHOT_TABLE: &str = "snapshots";

/// Size of the saved history of one aggregate type: the events and the distinct aggregates that
/// hold them. Counted in one round trip by [`PostgresEventRepository::totals`]; meant for operator
/// dashboards. It reads the whole journal of the type: give it a pool without a statement timeout.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LedgerTotals {
    pub events: u64,
    pub aggregates: u64,
}

/// The default bound of [`PostgresEventRepository::count_after`]: a status page asks every second, and a reader
/// far behind is shown as "at least this many".
pub const DEFAULT_COUNT_AFTER_CAP: u64 = 100_000;

/// An event repository relying on a Postgres database for persistence.
///
/// A clone shares the pool and the SQL texts, which are built once when the repository is: a command
/// executor builds its repository once and gives every command a clone.
#[derive(Clone, Debug)]
#[must_use]
pub struct PostgresEventRepository {
    pool: Pool<Postgres>,
    query_factory: Arc<SqlQueryFactory>,
}

impl PersistedEventRepository for PostgresEventRepository {
    async fn events<A: Aggregate>(&self, aggregate_id: &str) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.fetch_events(
            sqlx::query(self.query_factory.select_events())
                .bind(A::TYPE)
                .bind(aggregate_id),
        )
        .await
    }

    async fn events_after<A: Aggregate>(
        &self,
        aggregate_id: &str,
        after_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let query = sqlx::query(self.query_factory.select_events_after())
            .bind(A::TYPE)
            .bind(aggregate_id)
            .bind(encode_bigint(after_sequence)?);
        self.fetch_events(query).await
    }

    async fn snapshot<A: Aggregate>(&self, aggregate_id: &str) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        self.load_snapshot::<A>(aggregate_id).await
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<cqrsx::journal::snapshot::SnapshotUpdate>,
    ) -> Result<(), PersistenceError> {
        self.persist_batch::<A>(events, snapshot_update).await
    }

    async fn feed_after<A: Aggregate>(&self, after: Position, limit: usize) -> Result<Vec<FeedEntry>, PersistenceError> {
        self.feed_page::<A>(after, limit).await
    }

    async fn head<A: Aggregate>(&self) -> Result<Position, PersistenceError> {
        self.journal_head::<A>().await
    }

    async fn aggregate_ids_after<A: Aggregate>(&self, after: &str, limit: usize) -> Result<Vec<String>, PersistenceError> {
        self.aggregate_ids_page::<A>(after, limit).await
    }
}

impl PostgresEventRepository {
    /// Creates a new `PostgresEventRepository` from the provided database connection.
    /// This uses the default tables 'events' and 'snapshots'.
    ///
    /// ```
    /// use sqlx::Pool;
    /// use sqlx::Postgres;
    /// use cqrsx_postgres::event_repository::PostgresEventRepository;
    ///
    /// fn configure_repo(pool: Pool<Postgres>) -> PostgresEventRepository {
    ///     PostgresEventRepository::new(pool)
    /// }
    /// ```
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self::use_tables(pool, DEFAULT_EVENT_TABLE, DEFAULT_SNAPSHOT_TABLE)
    }

    /// Configures a `PostgresEventRepository` to use the provided table names.
    ///
    /// _Example: configure the repository to use "my_event_table" and "my_snapshot_table"
    /// for the event and snapshot table names._
    /// ```
    /// use sqlx::Pool;
    /// use sqlx::Postgres;
    /// use cqrsx_postgres::event_repository::PostgresEventRepository;
    ///
    /// fn configure_repo(pool: Pool<Postgres>) -> PostgresEventRepository {
    ///     let store = PostgresEventRepository::new(pool);
    ///     store.with_tables("my_event_table", "my_snapshot_table")
    /// }
    /// ```
    pub fn with_tables(mut self, events_table: &str, snapshots_table: &str) -> Self {
        self.query_factory = Arc::new(SqlQueryFactory::new(events_table, snapshots_table));
        self
    }

    fn use_tables(pool: Pool<Postgres>, events_table: &str, snapshots_table: &str) -> Self {
        Self {
            pool,
            query_factory: Arc::new(SqlQueryFactory::new(events_table, snapshots_table)),
        }
    }

    /// Whether `other` uses the very SQL texts of this repository (a clone of it), not a copy of them.
    #[cfg(test)]
    pub(crate) fn shares_queries_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.query_factory, &other.query_factory)
    }
}

pub(super) fn encode_bigint(value: usize) -> Result<i64, PostgresRepositoryError> {
    i64::try_from(value).map_err(|error| PostgresRepositoryError::UnknownError(Box::new(error)))
}

/// A column a row lacks or holds in another type than the code reads: the table is not what it should be.
pub(super) fn row_decode_error(error: sqlx::Error) -> PostgresRepositoryError {
    PostgresRepositoryError::DeserializationError(Box::new(storage_failure(error)))
}

pub(super) fn decode_usize(value: i64) -> Result<usize, PostgresRepositoryError> {
    usize::try_from(value).map_err(|error| PostgresRepositoryError::DeserializationError(Box::new(error)))
}

/// `count(*)` is a `bigint`; a negative count can only mean a broken row decode.
fn decode_count(value: i64) -> Result<u64, PostgresRepositoryError> {
    u64::try_from(value).map_err(|error| PostgresRepositoryError::DeserializationError(Box::new(error)))
}

#[cfg(test)]
mod numeric_tests {
    use super::*;
    #[test]
    fn persisted_numbers_are_checked_in_both_directions() {
        assert_eq!(encode_bigint(0).unwrap(), 0);
        assert_eq!(decode_usize(0).unwrap(), 0);
        assert_eq!(decode_count(0).unwrap(), 0);
        assert_eq!(decode_count(i64::MAX).unwrap(), i64::MAX as u64);
        for invalid in [-1, i64::MIN] {
            assert!(matches!(
                decode_usize(invalid),
                Err(PostgresRepositoryError::DeserializationError(_))
            ));
            assert!(matches!(
                decode_count(invalid),
                Err(PostgresRepositoryError::DeserializationError(_))
            ));
        }
        #[cfg(target_pointer_width = "64")]
        {
            assert_eq!(encode_bigint(i64::MAX as usize).unwrap(), i64::MAX);
            assert_eq!(decode_usize(i64::MAX).unwrap(), i64::MAX as usize);
            assert!(matches!(
                encode_bigint(usize::MAX),
                Err(PostgresRepositoryError::UnknownError(_))
            ));
        }
    }
}

#[cfg(test)]
mod configuration_tests {
    use super::*;

    #[test]
    fn a_row_the_code_cannot_read_is_a_deserialization_error_holding_the_driver_error_with_its_category() {
        let PostgresRepositoryError::DeserializationError(source) = row_decode_error(sqlx::Error::RowNotFound) else {
            panic!("a row decode error is a deserialization error")
        };
        let failure = source.downcast_ref::<cqrsx::diagnostics::StorageFailure>().unwrap();
        assert_eq!((failure.category(), failure.code()), ("other", None));
        assert!(matches!(
            failure.cause().downcast_ref::<sqlx::Error>(),
            Some(sqlx::Error::RowNotFound)
        ));
    }

    #[tokio::test]
    async fn custom_tables_reach_every_query() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        let repository = PostgresEventRepository::new(pool.clone()).with_tables("custom_events", "custom_snapshots");
        assert!(repository.query_factory.select_events().contains("FROM custom_events"));
        assert!(repository.query_factory.count_events().contains("FROM custom_events"));
        assert!(repository.query_factory.count_after().contains("FROM custom_events"));
        assert!(repository.query_factory.select_snapshot().contains("FROM custom_snapshots"));
        pool.close().await;
    }

    #[tokio::test]
    async fn clones_share_the_sql_texts_and_new_tables_build_their_own() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        let repository = PostgresEventRepository::new(pool.clone()).with_tables("custom_events", "custom_snapshots");
        let clone = repository.clone();
        assert!(repository.shares_queries_with(&clone));
        assert!(clone.query_factory.select_events().contains("FROM custom_events"));
        let other = clone.clone().with_tables("other_events", "other_snapshots");
        assert!(!repository.shares_queries_with(&other));
        assert!(other.query_factory.select_events().contains("FROM other_events"));
        assert!(repository.query_factory.select_events().contains("FROM custom_events"));
        pool.close().await;
    }
}
