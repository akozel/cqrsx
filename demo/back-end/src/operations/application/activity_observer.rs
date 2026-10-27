//! The projection processors' reports as activity log entries: what the feed and the X-ray show of the
//! read side. A processor calls the observer on its own task, so every method only publishes.
use crate::shared::application::activity::ActivityHub;
use cqrsx::cqrs::projection::processor::observer::{Applied, Failed, LeaseEvent, ProjectionObserver, RebuildReason, Rebuilt};
use serde_json::json;
use std::sync::Arc;

/// The observer of one read model's processor on this node.
pub struct ActivityObserver {
    projection: &'static str,
    owner: String,
    activity: Arc<ActivityHub>,
}

impl ActivityObserver {
    pub fn new(projection: &'static str, owner: impl Into<String>, activity: Arc<ActivityHub>) -> Self {
        Self {
            projection,
            owner: owner.into(),
            activity,
        }
    }
}

impl ProjectionObserver for ActivityObserver {
    /// One document took the events of a live batch (or skipped them as already applied).
    fn applied(&self, report: &Applied<'_>) {
        self.activity.publish(
            "projection.updated",
            json!({
                "projection": self.projection,
                "view_id": report.aggregate_id,
                "sequence": report.last_sequence,
                "events": report.events,
            }),
        );
    }

    /// A round failed: the processor stops before it, pauses and tries again; the status shows it stalled.
    fn failed(&self, report: &Failed<'_>) {
        self.activity.publish(
            "projection.failed",
            json!({
                "projection": self.projection,
                "view_id": report.aggregate_id,
                "handler": report.handler,
                "category": report.category,
                "failures": report.failures,
                "message": format!("{}: {}", report.handler, report.category),
            }),
        );
    }

    /// A document was replaced by the fold of its history: a requested rebuild or a healed gap.
    fn rebuilt(&self, report: &Rebuilt<'_>) {
        let reason = match report.reason {
            RebuildReason::Requested => "requested",
            RebuildReason::Healed => "healed",
        };
        self.activity.publish(
            "projection.rebuilt",
            json!({
                "projection": self.projection,
                "view_id": report.aggregate_id,
                "events": report.events,
                "reason": reason,
            }),
        );
    }

    /// This node took, lost or gave back the projection's checkpoint.
    fn lease(&self, event: LeaseEvent) {
        let lease = match event {
            LeaseEvent::Acquired => "acquired",
            LeaseEvent::Lost => "lost",
            LeaseEvent::Released => "released",
        };
        self.activity.publish(
            "projection.lease",
            json!({ "projection": self.projection, "lease": lease, "owner": self.owner }),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::cqrs::store::feed::Position;
    use serde_json::Value;

    fn entries(activity: &ActivityHub) -> Vec<(String, Value)> {
        activity
            .subscribe()
            .0
            .iter()
            .map(|entry| (entry.kind.clone(), Value::Object(entry.fields.clone())))
            .collect()
    }

    #[test]
    fn every_report_becomes_one_entry_with_the_projection_and_this_node() {
        let activity = Arc::new(ActivityHub::new(16, 16));
        let observer = ActivityObserver::new("concert_halls", "node-a", activity.clone());
        observer.applied(&Applied {
            aggregate_id: "c-1",
            last_sequence: 7,
            events: 2,
            position: Position::new(9),
        });
        observer.failed(&Failed {
            aggregate_id: Some("c-1"),
            handler: "concert_halls",
            category: "connection",
            position: Position::new(9),
            failures: 3,
        });
        observer.failed(&Failed {
            aggregate_id: None,
            handler: "journal",
            category: "storage",
            position: Position::START,
            failures: 1,
        });
        for reason in [RebuildReason::Requested, RebuildReason::Healed] {
            observer.rebuilt(&Rebuilt {
                aggregate_id: "c-2",
                events: 41,
                reason,
            });
        }
        for event in [LeaseEvent::Acquired, LeaseEvent::Lost, LeaseEvent::Released] {
            observer.lease(event);
        }
        let entries = entries(&activity);
        let kinds: Vec<&str> = entries.iter().map(|(kind, _)| kind.as_str()).collect();
        assert_eq!(
            kinds,
            [
                "projection.updated",
                "projection.failed",
                "projection.failed",
                "projection.rebuilt",
                "projection.rebuilt",
                "projection.lease",
                "projection.lease",
                "projection.lease"
            ]
        );
        assert_eq!(
            entries[0].1,
            json!({ "projection": "concert_halls", "view_id": "c-1", "sequence": 7, "events": 2 })
        );
        assert_eq!(
            entries[1].1,
            json!({ "projection": "concert_halls", "view_id": "c-1", "handler": "concert_halls", "category": "connection",
                    "failures": 3, "message": "concert_halls: connection" })
        );
        assert_eq!(entries[2].1["view_id"], Value::Null, "a failure without a key");
        assert_eq!(
            [entries[3].1["reason"].as_str(), entries[4].1["reason"].as_str()],
            [Some("requested"), Some("healed")]
        );
        assert_eq!(
            entries[5..]
                .iter()
                .map(|(_, fields)| fields["lease"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["acquired", "lost", "released"]
        );
        assert!(entries[5..].iter().all(|(_, fields)| fields["owner"] == "node-a"));
    }
}
