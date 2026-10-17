//! In-memory activity log: a `tokio::broadcast` channel plus a ring buffer of the last entries so
//! a new SSE subscriber sees recent history. This is a log, not state — nothing is persisted.
use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use serde_json::{Map, Value};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Serialize)]
pub struct ActivityEntry {
    pub id: u64,
    pub at: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(flatten)]
    pub fields: Map<String, Value>,
}

pub struct ActivityHub {
    sender: broadcast::Sender<Arc<ActivityEntry>>,
    backlog: Mutex<VecDeque<Arc<ActivityEntry>>>,
    backlog_size: usize,
    next_id: AtomicU64,
}

impl ActivityHub {
    pub fn new(channel_capacity: usize, backlog_size: usize) -> Self {
        let (sender, _) = broadcast::channel(channel_capacity.max(1));
        Self {
            sender,
            backlog: Mutex::new(VecDeque::with_capacity(backlog_size)),
            backlog_size,
            next_id: AtomicU64::new(1),
        }
    }

    /// Publishes one entry; `fields` must be a JSON object (other values are wrapped as `data`).
    pub fn publish(&self, kind: &str, fields: Value) -> Arc<ActivityEntry> {
        let fields = match fields {
            Value::Object(map) => map,
            other => Map::from_iter([("data".to_owned(), other)]),
        };
        let entry = Arc::new(ActivityEntry {
            id: self.next_id.fetch_add(1, Ordering::SeqCst),
            at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            kind: kind.to_owned(),
            fields,
        });
        if let Ok(mut backlog) = self.backlog.lock() {
            if backlog.len() >= self.backlog_size {
                backlog.pop_front();
            }
            backlog.push_back(entry.clone());
        }
        // No subscribers is not an error for a log.
        let _ = self.sender.send(entry.clone());
        entry
    }

    /// Recent entries (oldest first) and a live receiver, taken under one lock so nothing is lost
    /// between the two.
    pub fn subscribe(&self) -> (Vec<Arc<ActivityEntry>>, broadcast::Receiver<Arc<ActivityEntry>>) {
        let backlog = self.backlog.lock().map(|b| b.iter().cloned().collect()).unwrap_or_default();
        (backlog, self.sender.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn backlog_is_bounded_and_live_entries_reach_subscribers() {
        let hub = ActivityHub::new(8, 2);
        hub.publish("a", json!({ "n": 1 }));
        hub.publish("b", json!({ "n": 2 }));
        hub.publish("c", json!("not an object"));
        let (backlog, mut live) = hub.subscribe();
        assert_eq!(backlog.iter().map(|e| e.kind.as_str()).collect::<Vec<_>>(), ["b", "c"]);
        assert_eq!(backlog[1].fields["data"], json!("not an object"));
        let published = hub.publish("d", json!({}));
        let received = live.recv().await.unwrap();
        assert_eq!(received.id, published.id);
        assert!(received.id > backlog[1].id);
        let json = serde_json::to_value(&*received).unwrap();
        assert_eq!(json["type"], "d");
        assert!(json["at"].as_str().unwrap().ends_with('Z'));
    }
}
