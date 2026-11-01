//! Event/snapshot writes; each operation owns its complete transaction.
// Derived from postgres-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and cqrsx-postgres/licenses/.
use super::{PostgresEventRepository, encode_bigint};
use crate::error::PostgresRepositoryError;
use cqrsx::{
    domain::aggregate::Aggregate,
    journal::{PersistenceError, serialized_event::SerializedEvent, snapshot::SnapshotUpdate},
};
use sqlx::{Postgres, Transaction};

impl PostgresEventRepository {
    pub(super) async fn persist_batch<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<cqrsx::journal::snapshot::SnapshotUpdate>,
    ) -> Result<(), PersistenceError> {
        // Validate the entire request before beginning a transaction or writing rows.
        for event in events {
            encode_bigint(event.sequence)?;
        }
        if let Some(snapshot) = &snapshot_update {
            encode_bigint(snapshot.last_sequence)?;
            encode_bigint(snapshot.next_version)?;
            if snapshot.next_version == 0 {
                return Err(PostgresRepositoryError::Unexpected("snapshot version must be positive".into()).into());
            }
        }
        match snapshot_update {
            None => self.persist_events_only::<A>(events).await?,
            Some(snapshot) if snapshot.next_version == 1 => self.persist_with_new_snapshot::<A>(snapshot, events).await?,
            Some(snapshot) => self.persist_with_snapshot_update::<A>(snapshot, events).await?,
        }
        Ok(())
    }

    pub(super) async fn persist_events_only<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
    ) -> Result<(), PostgresRepositoryError> {
        let mut tx: Transaction<'_, Postgres> = sqlx::Acquire::begin(&self.pool).await?;
        insert_events::<A>(self.query_factory.insert_event(), &mut tx, events).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn persist_with_new_snapshot<A: Aggregate>(
        &self,
        snapshot: SnapshotUpdate,
        events: &[SerializedEvent],
    ) -> Result<(), PostgresRepositoryError> {
        let mut tx: Transaction<'_, Postgres> = sqlx::Acquire::begin(&self.pool).await?;
        insert_events::<A>(self.query_factory.insert_event(), &mut tx, events).await?;
        let result = sqlx::query(self.query_factory.insert_snapshot())
            .bind(A::TYPE)
            .bind(&snapshot.aggregate_id)
            .bind(encode_bigint(snapshot.last_sequence)?)
            .bind(encode_bigint(snapshot.next_version)?)
            .bind(&snapshot.payload)
            .execute(&mut *tx)
            .await?;
        if result.rows_affected() != 1 {
            return Err(PostgresRepositoryError::Conflict);
        }
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn persist_with_snapshot_update<A: Aggregate>(
        &self,
        snapshot: SnapshotUpdate,
        events: &[SerializedEvent],
    ) -> Result<(), PostgresRepositoryError> {
        let mut tx: Transaction<'_, Postgres> = sqlx::Acquire::begin(&self.pool).await?;
        insert_events::<A>(self.query_factory.insert_event(), &mut tx, events).await?;
        let result = sqlx::query(self.query_factory.update_snapshot())
            .bind(A::TYPE)
            .bind(&snapshot.aggregate_id)
            .bind(encode_bigint(snapshot.last_sequence)?)
            .bind(encode_bigint(snapshot.next_version)?)
            .bind(encode_bigint(snapshot.next_version - 1)?)
            .bind(&snapshot.payload)
            .execute(&mut *tx)
            .await?;
        if result.rows_affected() != 1 {
            return Err(PostgresRepositoryError::Conflict);
        }
        tx.commit().await?;
        Ok(())
    }
}

async fn insert_events<A: Aggregate>(
    insert_event_sql: &str,
    tx: &mut Transaction<'_, Postgres>,
    events: &[SerializedEvent],
) -> Result<(), PostgresRepositoryError> {
    for event in events {
        let event_type = &event.event_type;
        let event_version = &event.event_version;
        let payload = serde_json::to_value(&event.payload)?;
        let metadata = serde_json::to_value(&event.metadata)?;
        let result = sqlx::query(insert_event_sql)
            .bind(A::TYPE)
            .bind(event.aggregate_id.as_str())
            .bind(encode_bigint(event.sequence)?)
            .bind(event_type)
            .bind(event_version)
            .bind(&payload)
            .bind(&metadata)
            .execute(&mut **tx)
            .await?;
        if result.rows_affected() != 1 {
            return Err(PostgresRepositoryError::Conflict);
        }
    }
    Ok(())
}
