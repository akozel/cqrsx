//! The aggregated state broadcast to SSE clients, and its change-detection hash.
use crate::{
    mongo::CollectionView,
    postgres::{PgState, Stats, Table},
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SourceError {
    pub source: &'static str,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Default)]
pub struct State {
    pub events: Option<Table>,
    pub snapshots: Option<Table>,
    pub outbox: Option<Table>,
    pub mongo: BTreeMap<String, CollectionView>,
    pub stats: Value,
    pub db_time: Option<String>,
    pub polled_at: String,
    pub errors: Vec<SourceError>,
}

impl State {
    pub fn with_postgres(mut self, pg: PgState) -> Self {
        self.db_time = Some(pg.db_time);
        self.stats = merge_stats(&self.stats, &pg.stats);
        self.events = Some(pg.events);
        self.snapshots = Some(pg.snapshots);
        self.outbox = Some(pg.outbox);
        self
    }

    /// SHA-256 over the canonical JSON of everything except `db_time`/`polled_at`, which change
    /// every poll. `serde_json::Value` objects are sorted maps, so the encoding is canonical.
    pub fn content_hash(&self) -> String {
        let mut json = serde_json::to_value(self).unwrap_or(Value::Null);
        if let Value::Object(map) = &mut json {
            map.remove("db_time");
            map.remove("polled_at");
        }
        let mut hasher = Sha256::new();
        hasher.update(json.to_string().as_bytes());
        hex::encode(hasher.finalize())
    }
}

fn merge_stats(existing: &Value, pg: &Stats) -> Value {
    let mut merged = existing.as_object().cloned().unwrap_or_default();
    merged.insert("events_total".into(), json!(pg.events_total));
    merged.insert("events_by_aggregate".into(), json!(pg.events_by_aggregate));
    merged.insert("tasks_by_status".into(), json!(pg.tasks_by_status));
    merged.insert("snapshots_total".into(), json!(pg.snapshots_total));
    Value::Object(merged)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(db_time: &str, total: i64) -> State {
        State {
            events: Some(Table {
                source: "postgres",
                table: "events",
                query: "q".into(),
                total,
                rows: vec![json!({ "sequence": 1 })],
            }),
            snapshots: None,
            outbox: None,
            mongo: BTreeMap::new(),
            stats: json!({ "events_total": total }),
            db_time: Some(db_time.into()),
            polled_at: db_time.into(),
            errors: vec![],
        }
    }

    #[test]
    fn hash_ignores_time_but_notices_data() {
        let a = sample("2026-10-17T09:00:00Z", 1);
        let b = sample("2026-10-17T09:00:01Z", 1);
        let c = sample("2026-10-17T09:00:00Z", 2);
        assert_eq!(a.content_hash(), b.content_hash());
        assert_ne!(a.content_hash(), c.content_hash());
        assert_eq!(a.content_hash().len(), 64);
    }
}
