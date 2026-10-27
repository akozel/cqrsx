//! The metadata of a command: who issued it and how it relates to earlier work (the cqrsx causation keys
//! `command_id`, `correlation_id`, `causation_id`, `source`), plus `recorded_at`. It travels through
//! `execute_with_metadata` (decision D-04) and ends up in the `events.metadata` column of every committed event. Until
//! the engine stamps it (backlog 05/11), the entry points build it: the HTTP handler for a UI command, the chain step
//! for an internal one.
use chrono::{SecondsFormat, Utc};
use cqrsx::{cqrs::command::causation::Causation, task::attempt::TaskAttempt};
use std::collections::HashMap;

/// Who issued the command and how it relates to earlier work ([`Causation`]: for UI commands the
/// three ids are equal; internal commands inherit the task's `correlation_id` and point
/// `causation_id` at the task), plus when. `recorded_at` has a fixed width so the sidecar can
/// sort it as text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandMetadata {
    pub causation: Causation,
    pub recorded_at: String,
}

impl CommandMetadata {
    fn now() -> String {
        Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
    }

    pub fn with_source(source: impl Into<String>) -> Self {
        Self {
            causation: Causation::root(source),
            recorded_at: Self::now(),
        }
    }

    pub fn ui() -> Self {
        Self::with_source("ui")
    }

    /// Metadata of an internal command issued by an outbox task handler.
    pub fn from_task(attempt: &TaskAttempt) -> Self {
        Self {
            causation: Causation::from_task(attempt),
            recorded_at: Self::now(),
        }
    }

    /// What every committed event stores: the four causation keys and `recorded_at`.
    pub fn to_map(&self) -> HashMap<String, String> {
        let mut map = self.causation.event_metadata();
        map.insert("recorded_at".to_owned(), self.recorded_at.clone());
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::task::format::TaskFormat;
    use std::collections::BTreeSet;
    use uuid::Uuid;

    #[test]
    fn ui_metadata_uses_one_id_for_all_three_roles_and_fixed_width_time() {
        let metadata = CommandMetadata::ui();
        let causation = &metadata.causation;
        assert_eq!(causation.command_id, causation.correlation_id);
        assert_eq!(causation.command_id, causation.causation_id);
        assert_eq!(causation.source, "ui");
        assert_eq!(metadata.recorded_at.len(), "2026-10-17T09:12:03.123456Z".len());
        let map = metadata.to_map();
        assert_eq!(
            map.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            BTreeSet::from(["causation_id", "command_id", "correlation_id", "recorded_at", "source"]),
            "wire contract of events.metadata"
        );
        assert_eq!(map["command_id"], causation.command_id.to_string());
        assert_eq!(map["source"], "ui");
        assert_eq!(map["recorded_at"], metadata.recorded_at);
    }

    #[test]
    fn task_metadata_inherits_correlation_and_points_causation_at_the_task() {
        let correlation = Uuid::now_v7();
        let task = TaskAttempt::new(
            Uuid::new_v4(),
            TaskFormat::new("encore_record_ticket", 1).unwrap(),
            serde_json::json!({}),
            serde_json::json!({ "correlation_id": correlation }),
            1,
        )
        .unwrap();
        let metadata = CommandMetadata::from_task(&task);
        assert_eq!(metadata.causation.correlation_id, correlation);
        assert_eq!(metadata.causation.causation_id, task.task_id());
        assert_ne!(metadata.causation.command_id, correlation);
        assert_eq!(metadata.causation.source, "task:encore_record_ticket");
        assert_eq!(metadata.to_map()["source"], "task:encore_record_ticket");
    }
}
