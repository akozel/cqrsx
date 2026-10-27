// Derived from postgres-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use super::{
    error::PostgresRepositoryError,
    event_repository::{decode_usize, encode_bigint},
};
use crate::cqrs::{
    aggregate::Aggregate,
    projection::{
        View,
        repository::{StoredView, ViewContext, ViewRepository},
    },
    store::PersistenceError,
};
use sqlx::{Pool, Postgres, Row, postgres::PgRow};
use std::marker::PhantomData;

/// Stores serialized read-model views in a PostgreSQL table with optimistic versions and the last applied
/// event of each document's aggregate.
pub struct PostgresViewRepository<V, A> {
    insert_sql: String,
    update_sql: String,
    select_sql: String,
    list_sql: String,
    range_sql: String,
    pool: Pool<Postgres>,
    _phantom: PhantomData<(V, A)>,
}

impl<V, A> PostgresViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    /// Creates a new `PostgresViewRepository` that will store serialized views in a Postgres table named after
    /// the view, [`View::NAME`]. The application creates the table before use, with this shape (`last_sequence` is
    /// the last applied event of the view's aggregate, see `ViewContext`):
    ///
    /// ```sql
    /// CREATE TABLE my_view_table (
    ///     view_id       text   PRIMARY KEY,
    ///     version       bigint NOT NULL,
    ///     last_sequence bigint NOT NULL CHECK (last_sequence >= 0),
    ///     payload       json   NOT NULL
    /// );
    /// ```
    ///
    /// `list` and `range` order the ids by their bytes (`COLLATE "C"`), whatever the collation of the column, as
    /// the other repositories do; a column declared `view_id text COLLATE "C" PRIMARY KEY` lets its index serve
    /// them.
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use serde_json::Value;
    /// # use cqrsx::cqrs::aggregate::Aggregate;
    /// # use cqrsx::cqrs::event::DomainEvent;
    /// # use cqrsx::cqrs::event::EventEnvelope;
    /// # use cqrsx::cqrs::event_handler::EventHandler;
    /// # use cqrsx::cqrs::projection::View;
    /// # use cqrsx::cqrs::command::decide::Decide;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
    /// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
    /// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
    /// # use cqrsx::cqrs::projection::repository::ViewContext;
    /// # use cqrsx::cqrs::projection::repository::ViewRepository;
    /// #
    /// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    /// # pub enum MyEvents {
    /// #     SomethingWasDone,
    /// #     SomethingElseWasDone,
    /// # }
    /// #
    /// # impl DomainEvent for MyEvents {
    /// #     fn event_type(&self) -> String {
    /// #         format!("{:?}", &self)
    /// #     }
    /// #     fn event_version(&self) -> String {
    /// #         "0.1.0".to_string()
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Serialize, Deserialize)]
    /// # pub enum MyCommands {
    /// #     DoSomething,
    /// #     BadCommand,
    /// # }
    /// #
    /// # #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    /// # pub struct MyAggregate;
    /// #
    /// # impl Aggregate for MyAggregate {
    /// #     const TYPE: &'static str = "MyAggregate";
    /// #     type Id = String;
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Error = MyUserError;
    /// #     type Rules = MyService;
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
    /// # }
    /// #
    /// # impl Decide<MyAggregate> for MyCommands {
    /// #     fn decide(self, _: &MyAggregate, _: &MyService) -> Result<MyEvents, MyUserError> {
    /// #         match self {
    /// #             MyCommands::DoSomething => Ok(MyEvents::SomethingWasDone),
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    /// # #[error("{0}")]
    /// # pub struct MyUserError(pub String);
    /// #
    /// # impl From<&str> for MyUserError {
    /// #     fn from(msg: &str) -> Self {
    /// #         Self(msg.to_string())
    /// #     }
    /// # }
    /// #
    /// # #[derive(Debug, Clone, Default)]
    /// # pub struct MyService;
    /// #
    /// # #[derive(Debug, Default, Serialize, Deserialize)]
    /// # pub struct MyView;
    /// #
    /// # impl View<MyAggregate> for MyView {
    /// #     const NAME: &'static str = "my_view_table";
    /// #     fn update(&mut self, _event: &EventEnvelope<MyAggregate>) {
    /// #         todo!()
    /// #     }
    /// # }
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyView;
    /// use sqlx::Pool;
    /// use sqlx::Postgres;
    /// use cqrsx::postgres::view_repository::PostgresViewRepository;
    ///
    /// fn configure_view_repo(pool: Pool<Postgres>) -> PostgresViewRepository<MyView,MyAggregate> {
    ///     PostgresViewRepository::new(pool) // the table `my_view_table`
    /// }
    /// ```
    pub fn new(pool: Pool<Postgres>) -> Self {
        let table_name = V::NAME;
        let insert_sql = format!(
            "INSERT INTO {table_name} (payload, version, last_sequence, view_id) VALUES ( $1, $2, $3, $4 ) ON CONFLICT (view_id) DO NOTHING"
        );
        let update_sql =
            format!("UPDATE {table_name} SET payload= $1 , version= $2 , last_sequence= $3 WHERE view_id= $4 AND version= $5");
        let select_sql = format!("SELECT version, last_sequence, payload FROM {table_name} WHERE view_id= $1");
        let list_sql =
            format!(r#"SELECT view_id, version, last_sequence, payload FROM {table_name} ORDER BY view_id COLLATE "C""#);
        let range_sql = format!(
            r#"SELECT view_id, version, last_sequence, payload FROM {table_name} WHERE view_id COLLATE "C" >= $1 AND view_id COLLATE "C" < $2 ORDER BY view_id COLLATE "C""#
        );
        Self {
            insert_sql,
            update_sql,
            select_sql,
            list_sql,
            range_sql,
            pool,
            _phantom: PhantomData,
        }
    }
}

impl<V, A> ViewRepository<V, A> for PostgresViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError> {
        let row: Option<PgRow> = sqlx::query(&self.select_sql)
            .bind(view_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(PostgresRepositoryError::from)?;
        match row {
            None => Ok(None),
            Some(row) => {
                let view = serde_json::from_value(row.get("payload"))?;
                Ok(Some(view))
            }
        }
    }

    async fn load_with_context(&self, view_id: &str) -> Result<Option<(V, ViewContext)>, PersistenceError> {
        let row: Option<PgRow> = sqlx::query(&self.select_sql)
            .bind(view_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(PostgresRepositoryError::from)?;
        match row {
            None => Ok(None),
            Some(row) => {
                let version = row.get("version");
                let last_sequence = decode_usize(row.get("last_sequence"))?;
                let view = serde_json::from_value(row.get("payload"))?;
                let view_context = ViewContext::new(view_id.to_string(), version, last_sequence);
                Ok(Some((view, view_context)))
            }
        }
    }

    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        let sql = match context.version {
            0 => &self.insert_sql,
            _ => &self.update_sql,
        };
        let version = context
            .version
            .checked_add(1)
            .ok_or_else(|| PersistenceError::UnknownError("view version overflow".into()))?;
        let last_sequence = encode_bigint(context.last_sequence)?;
        let payload = serde_json::to_value(&view).map_err(PostgresRepositoryError::from)?;
        // The INSERT reads the first four binds, the UPDATE all five.
        let rows_affected = sqlx::query(sql.as_str())
            .bind(payload)
            .bind(version)
            .bind(last_sequence)
            .bind(context.view_instance_id)
            .bind(context.version)
            .execute(&self.pool)
            .await
            .map_err(PostgresRepositoryError::from)?
            .rows_affected();
        if rows_affected < 1 {
            return Err(PersistenceError::OptimisticLockError);
        }
        Ok(())
    }

    async fn list(&self) -> Result<Vec<StoredView<V>>, PersistenceError> {
        let rows = sqlx::query(&self.list_sql)
            .fetch_all(&self.pool)
            .await
            .map_err(PostgresRepositoryError::from)?;
        rows.iter().map(stored_view).collect()
    }

    async fn range(&self, lower: &str, upper: &str) -> Result<Vec<StoredView<V>>, PersistenceError> {
        let rows = sqlx::query(&self.range_sql)
            .bind(lower)
            .bind(upper)
            .fetch_all(&self.pool)
            .await
            .map_err(PostgresRepositoryError::from)?;
        rows.iter().map(stored_view).collect()
    }
}

/// A row of `list` or `range`, decoded.
fn stored_view<V: serde::de::DeserializeOwned>(row: &PgRow) -> Result<StoredView<V>, PersistenceError> {
    Ok(StoredView {
        id: row.get("view_id"),
        version: row.get("version"),
        last_sequence: decode_usize(row.get("last_sequence"))?,
        view: serde_json::from_value(row.get("payload"))?,
    })
}
