//! In-memory activity log: a `tokio::broadcast` channel plus a ring buffer of the last entries so
//! a new SSE subscriber sees recent history. This is a log, not state — nothing is persisted.
use chrono::{SecondsFormat, Utc};
use cqrsx::mutex::lock;
use serde::Serialize;
use serde_json::{Map, Value};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
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

struct Backlog {
    entries: VecDeque<Arc<ActivityEntry>>,
    next_id: u64,
}

pub struct ActivityHub {
    sender: broadcast::Sender<Arc<ActivityEntry>>,
    backlog: Mutex<Backlog>,
    backlog_size: usize,
}

impl ActivityHub {
    pub fn new(channel_capacity: usize, backlog_size: usize) -> Self {
        let (sender, _) = broadcast::channel(channel_capacity.max(1));
        Self {
            sender,
            backlog: Mutex::new(Backlog {
                entries: VecDeque::with_capacity(backlog_size),
                next_id: 1,
            }),
            backlog_size,
        }
    }

    /// Publishes one entry; `fields` must be a JSON object (other values are wrapped as `data`).
    ///
    /// Id assignment, the backlog push and the broadcast happen under one lock, so a subscriber
    /// (which takes the same lock) sees every entry exactly once: either in its backlog snapshot
    /// or on the live channel, and ids are monotonic in both.
    pub fn publish(&self, kind: &str, fields: Value) -> Arc<ActivityEntry> {
        let fields = match fields {
            Value::Object(map) => map,
            other => Map::from_iter([("data".to_owned(), other)]),
        };
        let mut backlog = lock(&self.backlog, "activity_backlog");
        let entry = Arc::new(ActivityEntry {
            id: backlog.next_id,
            at: Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            kind: kind.to_owned(),
            fields,
        });
        backlog.next_id += 1;
        if backlog.entries.len() >= self.backlog_size {
            backlog.entries.pop_front();
        }
        backlog.entries.push_back(entry.clone());
        // No subscribers is not an error for a log.
        let _ = self.sender.send(entry.clone());
        entry
    }

    /// Recent entries (oldest first) and a live receiver that continues exactly where the
    /// backlog ends.
    pub fn subscribe(&self) -> (Vec<Arc<ActivityEntry>>, broadcast::Receiver<Arc<ActivityEntry>>) {
        let backlog = lock(&self.backlog, "activity_backlog");
        (backlog.entries.iter().cloned().collect(), self.sender.subscribe())
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

    /// Publishers on other threads race with `subscribe`: every entry must show up exactly once,
    /// either in the backlog snapshot or on the live channel, with no gap in between.
    #[tokio::test]
    async fn a_subscriber_racing_with_publishers_misses_nothing_and_sees_nothing_twice() {
        const PUBLISHERS: u64 = 4;
        const PER_PUBLISHER: u64 = 500;
        let total = PUBLISHERS * PER_PUBLISHER;
        let hub = Arc::new(ActivityHub::new(total as usize, total as usize));
        let threads: Vec<_> = (0..PUBLISHERS)
            .map(|_| {
                let hub = hub.clone();
                std::thread::spawn(move || {
                    for n in 0..PER_PUBLISHER {
                        hub.publish("tick", json!({ "n": n }));
                    }
                })
            })
            .collect();
        let (backlog, mut live) = hub.subscribe();
        for thread in threads {
            thread.join().unwrap();
        }
        let mut ids: Vec<u64> = backlog.iter().map(|e| e.id).collect();
        while let Ok(entry) = live.try_recv() {
            ids.push(entry.id);
        }
        assert_eq!(ids, (1..=total).collect::<Vec<_>>());
    }
}
