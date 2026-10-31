//! Read-only PostgreSQL inspection. Every poll runs inside ONE `REPEATABLE READ READ ONLY`
//! transaction so `events` and `transactional_outbox` come from the same snapshot: the UI never
//! shows an event without its task (or a task without its event) because of a race between two
//! SELECTs — the atomicity of the outbox is the main thing this demo proves.
//! All SQL is constant text; user input only ever becomes bind parameters.
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres, Row, Transaction, postgres::PgConnectOptions};
use std::collections::BTreeMap;

pub const EVENTS_QUERY: &str = "SELECT row_to_json(t) AS row FROM (SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata \
FROM events ORDER BY metadata->>'recorded_at' DESC NULLS LAST, aggregate_id, sequence DESC LIMIT $1) t";
pub const EVENTS_BY_AGGREGATE_QUERY: &str = "SELECT row_to_json(t) AS row FROM (SELECT aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata \
FROM events WHERE aggregate_id = $2 ORDER BY sequence DESC LIMIT $1) t";
pub const SNAPSHOTS_QUERY: &str = "SELECT row_to_json(t) AS row FROM (SELECT aggregate_type, aggregate_id, last_sequence, current_snapshot, payload \
FROM snapshots ORDER BY aggregate_type, aggregate_id LIMIT $1) t";
pub const SNAPSHOTS_BY_AGGREGATE_QUERY: &str = "SELECT row_to_json(t) AS row FROM (SELECT aggregate_type, aggregate_id, last_sequence, current_snapshot, payload \
FROM snapshots WHERE aggregate_id = $2 ORDER BY aggregate_type LIMIT $1) t";
pub const OUTBOX_QUERY: &str =
    "SELECT row_to_json(t) AS row FROM (SELECT * FROM transactional_outbox ORDER BY created_at DESC, id LIMIT $1) t";
pub const TABLES_QUERY: &str = "SELECT table_name, column_name, data_type FROM information_schema.columns \
WHERE table_schema = 'public' ORDER BY table_name, ordinal_position";

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct Table {
    pub source: &'static str,
    pub table: &'static str,
    pub query: String,
    pub total: i64,
    pub rows: Vec<Value>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
pub struct Stats {
    pub events_total: i64,
    pub events_by_aggregate: Vec<Value>,
    pub tasks_by_status: BTreeMap<String, i64>,
    pub snapshots_total: i64,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PgState {
    pub events: Table,
    pub snapshots: Table,
    pub outbox: Table,
    pub stats: Stats,
    pub db_time: String,
}

#[derive(Clone)]
pub struct PgInspector {
    pool: PgPool,
}

pub struct Limits {
    pub events: i64,
    pub snapshots: i64,
    pub outbox: i64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            events: 200,
            snapshots: 50,
            outbox: 200,
        }
    }
}

type Tx<'a> = Transaction<'a, Postgres>;

async fn rows(tx: &mut Tx<'_>, query: &str, limit: i64, aggregate_id: Option<&str>) -> Result<Vec<Value>, sqlx::Error> {
    let mut q = sqlx::query(query).bind(limit);
    if let Some(id) = aggregate_id {
        q = q.bind(id);
    }
    q.fetch_all(&mut **tx)
        .await?
        .iter()
        .map(|row| row.try_get::<Value, _>("row"))
        .collect()
}

async fn count(tx: &mut Tx<'_>, sql: &str, aggregate_id: Option<&str>) -> Result<i64, sqlx::Error> {
    let mut q = sqlx::query_scalar::<_, i64>(sql);
    if let Some(id) = aggregate_id {
        q = q.bind(id);
    }
    q.fetch_one(&mut **tx).await
}

fn shown_query(query: &str, limit: i64, aggregate_id: Option<&str>) -> String {
    let shown = query.replace("$1", &limit.to_string());
    match aggregate_id {
        Some(id) => shown.replace("$2", &format!("'{}'", id.replace('\'', "''"))),
        None => shown,
    }
}

impl PgInspector {
    /// Lazy pool: the sidecar starts even when PostgreSQL is down and reports it in `errors`.
    pub fn new(options: PgConnectOptions) -> Self {
        Self {
            pool: sqlx::postgres::PgPoolOptions::new()
                .max_connections(3)
                .acquire_timeout(std::time::Duration::from_secs(3))
                .connect_lazy_with(options),
        }
    }

    async fn begin(&self) -> Result<Tx<'_>, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        // One consistent snapshot for everything read in this poll (second line of defence after
        // the role's default_transaction_read_only).
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .execute(&mut *tx)
            .await?;
        Ok(tx)
    }

    pub async fn events(&self, aggregate_id: Option<&str>, limit: i64) -> Result<Table, sqlx::Error> {
        let mut tx = self.begin().await?;
        let table = Self::events_in(&mut tx, aggregate_id, limit).await?;
        tx.rollback().await?;
        Ok(table)
    }

    async fn events_in(tx: &mut Tx<'_>, aggregate_id: Option<&str>, limit: i64) -> Result<Table, sqlx::Error> {
        let (query, total) = match aggregate_id {
            Some(id) => (
                EVENTS_BY_AGGREGATE_QUERY,
                count(tx, "SELECT count(*) FROM events WHERE aggregate_id = $1", Some(id)).await?,
            ),
            None => (EVENTS_QUERY, count(tx, "SELECT count(*) FROM events", None).await?),
        };
        Ok(Table {
            source: "postgres",
            table: "events",
            query: shown_query(query, limit, aggregate_id),
            total,
            rows: rows(tx, query, limit, aggregate_id).await?,
        })
    }

    pub async fn snapshots(&self, aggregate_id: Option<&str>, limit: i64) -> Result<Table, sqlx::Error> {
        let mut tx = self.begin().await?;
        let table = Self::snapshots_in(&mut tx, aggregate_id, limit).await?;
        tx.rollback().await?;
        Ok(table)
    }

    async fn snapshots_in(tx: &mut Tx<'_>, aggregate_id: Option<&str>, limit: i64) -> Result<Table, sqlx::Error> {
        let (query, total) = match aggregate_id {
            Some(id) => (
                SNAPSHOTS_BY_AGGREGATE_QUERY,
                count(tx, "SELECT count(*) FROM snapshots WHERE aggregate_id = $1", Some(id)).await?,
            ),
            None => (SNAPSHOTS_QUERY, count(tx, "SELECT count(*) FROM snapshots", None).await?),
        };
        Ok(Table {
            source: "postgres",
            table: "snapshots",
            query: shown_query(query, limit, aggregate_id),
            total,
            rows: rows(tx, query, limit, aggregate_id).await?,
        })
    }

    pub async fn outbox(&self, limit: i64) -> Result<Table, sqlx::Error> {
        let mut tx = self.begin().await?;
        let table = Self::outbox_in(&mut tx, limit).await?;
        tx.rollback().await?;
        Ok(table)
    }

    async fn outbox_in(tx: &mut Tx<'_>, limit: i64) -> Result<Table, sqlx::Error> {
        Ok(Table {
            source: "postgres",
            table: "transactional_outbox",
            query: shown_query(OUTBOX_QUERY, limit, None),
            total: count(tx, "SELECT count(*) FROM transactional_outbox", None).await?,
            rows: rows(tx, OUTBOX_QUERY, limit, None).await?,
        })
    }

    async fn stats_in(tx: &mut Tx<'_>) -> Result<Stats, sqlx::Error> {
        let events_by_aggregate =
            sqlx::query("SELECT aggregate_type, aggregate_id, count(*) AS events FROM events GROUP BY 1, 2 ORDER BY 1, 2")
                .fetch_all(&mut **tx)
                .await?
                .iter()
                .map(|row| {
                    json!({
                        "aggregate_type": row.get::<String, _>("aggregate_type"),
                        "aggregate_id": row.get::<String, _>("aggregate_id"),
                        "events": row.get::<i64, _>("events"),
                    })
                })
                .collect();
        let tasks_by_status =
            sqlx::query("SELECT status, count(*) AS tasks FROM transactional_outbox GROUP BY status ORDER BY status")
                .fetch_all(&mut **tx)
                .await?
                .iter()
                .map(|row| (row.get::<String, _>("status"), row.get::<i64, _>("tasks")))
                .collect();
        Ok(Stats {
            events_total: count(tx, "SELECT count(*) FROM events", None).await?,
            events_by_aggregate,
            tasks_by_status,
            snapshots_total: count(tx, "SELECT count(*) FROM snapshots", None).await?,
        })
    }

    /// `[P1]` tables and columns of the public schema plus row counts.
    pub async fn tables(&self) -> Result<Value, sqlx::Error> {
        let mut tx = self.begin().await?;
        let mut tables: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for row in sqlx::query(TABLES_QUERY).fetch_all(&mut *tx).await? {
            tables
                .entry(row.get::<String, _>("table_name"))
                .or_default()
                .push(json!({ "name": row.get::<String, _>("column_name"), "type": row.get::<String, _>("data_type") }));
        }
        let mut result = Vec::new();
        for (name, columns) in tables {
            let rows = match name.as_str() {
                "events" => count(&mut tx, "SELECT count(*) FROM events", None).await?,
                "snapshots" => count(&mut tx, "SELECT count(*) FROM snapshots", None).await?,
                "transactional_outbox" => count(&mut tx, "SELECT count(*) FROM transactional_outbox", None).await?,
                _ => -1,
            };
            result.push(json!({ "table": name, "columns": columns, "rows": rows }));
        }
        tx.rollback().await?;
        Ok(json!({ "source": "postgres", "query": TABLES_QUERY, "tables": result }))
    }

    /// Everything the poller needs, from one snapshot.
    pub async fn snapshot(&self, limits: &Limits) -> Result<PgState, sqlx::Error> {
        let mut tx = self.begin().await?;
        let events = Self::events_in(&mut tx, None, limits.events).await?;
        let snapshots = Self::snapshots_in(&mut tx, None, limits.snapshots).await?;
        let outbox = Self::outbox_in(&mut tx, limits.outbox).await?;
        let stats = Self::stats_in(&mut tx).await?;
        let db_time: DateTime<Utc> = sqlx::query_scalar("SELECT now()").fetch_one(&mut *tx).await?;
        tx.rollback().await?;
        Ok(PgState {
            events,
            snapshots,
            outbox,
            stats,
            db_time: db_time.to_rfc3339_opts(SecondsFormat::Millis, true),
        })
    }

    pub async fn ping(&self) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT 1").execute(&self.pool).await.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shown_queries_inline_parameters_safely() {
        assert!(shown_query(EVENTS_QUERY, 200, None).ends_with("LIMIT 200) t"));
        let shown = shown_query(EVENTS_BY_AGGREGATE_QUERY, 10, Some("it's"));
        assert!(shown.contains("aggregate_id = 'it''s'"));
        assert!(shown.contains("LIMIT 10"));
    }
}
