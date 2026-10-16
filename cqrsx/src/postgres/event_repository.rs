// Derived from postgres-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
mod read;
mod sql_query;
mod stream;
mod write;

use self::sql_query::SqlQueryFactory;
use crate::{
    cqrs::{
        aggregate::Aggregate,
        store::{
            PersistenceError, repository::PersistedEventRepository, serialized_event::SerializedEvent,
            snapshot::SerializedSnapshot, stream::ReplayStream,
        },
    },
    postgres::error::PostgresRepositoryError,
};
use sqlx::{Pool, Postgres};

const DEFAULT_EVENT_TABLE: &str = "events";
const DEFAULT_SNAPSHOT_TABLE: &str = "snapshots";

const DEFAULT_STREAMING_CHANNEL_SIZE: usize = 200;

/// An event repository relying on a Postgres database for persistence.
pub struct PostgresEventRepository {
    pool: Pool<Postgres>,
    query_factory: SqlQueryFactory,
    stream_channel_size: usize,
}

impl PersistedEventRepository for PostgresEventRepository {
    async fn get_events<A: Aggregate>(&self, aggregate_id: &str) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.select_events::<A>(aggregate_id, self.query_factory.select_events())
            .await
    }

    async fn get_events_after<A: Aggregate>(
        &self,
        aggregate_id: &str,
        after_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        let after_sequence = encode_bigint(after_sequence)?;
        let query = self.query_factory.select_events_after(after_sequence);
        self.select_events::<A>(aggregate_id, &query).await
    }

    async fn get_snapshot<A: Aggregate>(&self, aggregate_id: &str) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        self.load_snapshot::<A>(aggregate_id).await
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<crate::cqrs::store::snapshot::SnapshotUpdate>,
    ) -> Result<(), PersistenceError> {
        self.persist_batch::<A>(events, snapshot_update).await
    }

    async fn stream_events<A: Aggregate>(&self, aggregate_id: &str) -> Result<ReplayStream, PersistenceError> {
        Ok(stream::spawn_stream(
            self.query_factory.select_events().to_string(),
            A::TYPE.to_string(),
            Some(aggregate_id.to_string()),
            self.pool.clone(),
            self.stream_channel_size,
        ))
    }

    async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
        Ok(stream::spawn_stream(
            self.query_factory.all_events().to_string(),
            A::TYPE.to_string(),
            None,
            self.pool.clone(),
            self.stream_channel_size,
        ))
    }
}

impl PostgresEventRepository {
    /// Creates a new `PostgresEventRepository` from the provided database connection.
    /// This uses the default tables 'events' and 'snapshots'.
    ///
    /// ```
    /// use sqlx::Pool;
    /// use sqlx::Postgres;
    /// use cqrsx::postgres::event_repository::PostgresEventRepository;
    ///
    /// fn configure_repo(pool: Pool<Postgres>) -> PostgresEventRepository {
    ///     PostgresEventRepository::new(pool)
    /// }
    /// ```
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self::use_tables(pool, DEFAULT_EVENT_TABLE, DEFAULT_SNAPSHOT_TABLE)
    }

    /// Configures a `PostgresEventRepository` to use a streaming queue of the provided size.
    ///
    /// _Example: configure the repository to stream with a 1000 event buffer._
    /// ```
    /// use sqlx::Pool;
    /// use sqlx::Postgres;
    /// use cqrsx::postgres::event_repository::PostgresEventRepository;
    ///
    /// fn configure_repo(pool: Pool<Postgres>) -> PostgresEventRepository {
    ///     let store = PostgresEventRepository::new(pool);
    ///     store.with_streaming_channel_size(1000)
    /// }
    /// ```
    pub fn with_streaming_channel_size(self, stream_channel_size: usize) -> Self {
        Self {
            pool: self.pool,
            query_factory: self.query_factory,
            stream_channel_size,
        }
    }

    /// Configures a `PostgresEventRepository` to use the provided table names.
    ///
    /// _Example: configure the repository to use "my_event_table" and "my_snapshot_table"
    /// for the event and snapshot table names._
    /// ```
    /// use sqlx::Pool;
    /// use sqlx::Postgres;
    /// use cqrsx::postgres::event_repository::PostgresEventRepository;
    ///
    /// fn configure_repo(pool: Pool<Postgres>) -> PostgresEventRepository {
    ///     let store = PostgresEventRepository::new(pool);
    ///     store.with_tables("my_event_table", "my_snapshot_table")
    /// }
    /// ```
    pub fn with_tables(mut self, events_table: &str, snapshots_table: &str) -> Self {
        self.query_factory = SqlQueryFactory::new(events_table, snapshots_table);
        self
    }

    fn use_tables(pool: Pool<Postgres>, events_table: &str, snapshots_table: &str) -> Self {
        Self {
            pool,
            query_factory: SqlQueryFactory::new(events_table, snapshots_table),
            stream_channel_size: DEFAULT_STREAMING_CHANNEL_SIZE,
        }
    }
}

fn encode_bigint(value: usize) -> Result<i64, PostgresRepositoryError> {
    i64::try_from(value).map_err(|error| PostgresRepositoryError::UnknownError(Box::new(error)))
}

fn decode_usize(value: i64) -> Result<usize, PostgresRepositoryError> {
    usize::try_from(value).map_err(|error| PostgresRepositoryError::DeserializationError(Box::new(error)))
}

#[cfg(test)]
mod numeric_tests {
    use super::*;
    #[test]
    fn persisted_numbers_are_checked_in_both_directions() {
        assert_eq!(encode_bigint(0).unwrap(), 0);
        assert_eq!(decode_usize(0).unwrap(), 0);
        for invalid in [-1, i64::MIN] {
            assert!(matches!(
                decode_usize(invalid),
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

    #[tokio::test]
    async fn table_and_stream_buffer_configuration_are_independent_of_call_order() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        for repository in [
            PostgresEventRepository::new(pool.clone())
                .with_streaming_channel_size(1)
                .with_tables("custom_events", "custom_snapshots"),
            PostgresEventRepository::new(pool.clone())
                .with_tables("custom_events", "custom_snapshots")
                .with_streaming_channel_size(1),
        ] {
            assert_eq!(repository.stream_channel_size, 1);
            assert!(repository.query_factory.select_events().contains("FROM custom_events"));
            assert!(repository.query_factory.select_snapshot().contains("FROM custom_snapshots"));
        }
        pool.close().await;
    }
}
