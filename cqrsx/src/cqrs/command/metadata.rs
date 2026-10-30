//! The metadata of a command: who caused it ([`Causation`]) and when it was issued, stored with every event it
//! commits.
use crate::{cqrs::command::causation::Causation, task::attempt::TaskAttempt};
use chrono::{SecondsFormat, Utc};
use std::collections::HashMap;

/// Event metadata key of when the command was issued: RFC 3339 in UTC with microseconds, so every value has the same
/// width and sorts as text (`2026-10-17T09:12:03.123456Z`).
pub const RECORDED_AT: &str = "recorded_at";

/// Who issued a command, how it relates to earlier work ([`Causation`]: for a root the three ids are equal; a command
/// a task issues inherits the task's correlation id and points its causation id at the task), plus when
/// ([`RECORDED_AT`]). Its map ([`to_map`](Self::to_map)) travels through
/// [`execute_with_metadata`](crate::cqrs::command::CommandExecutor::execute_with_metadata) and is stored with every
/// event the command commits; the core stamps the causation on the tasks it plans.
///
/// ```
/// use cqrsx::cqrs::command::metadata::{CommandMetadata, RECORDED_AT};
///
/// let metadata = CommandMetadata::root("ui");
/// let map = metadata.to_map();
/// assert_eq!(map["source"], "ui");
/// assert_eq!(map["command_id"], map["correlation_id"]);
/// assert_eq!(map[RECORDED_AT].len(), "2026-10-17T09:12:03.123456Z".len());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandMetadata {
    pub causation: Causation,
    pub recorded_at: String,
}

impl CommandMetadata {
    fn now() -> String {
        Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
    }

    /// A command that starts a chain, issued by `source` (`"ui"` for the HTTP API).
    pub fn root(source: impl Into<String>) -> Self {
        Self {
            causation: Causation::root(source),
            recorded_at: Self::now(),
        }
    }

    /// A command issued while handling `attempt` ([`Causation::from_task`]).
    pub fn from_task(attempt: &TaskAttempt) -> Self {
        Self {
            causation: Causation::from_task(attempt),
            recorded_at: Self::now(),
        }
    }

    /// What every committed event stores: the four causation keys and [`RECORDED_AT`].
    pub fn to_map(&self) -> HashMap<String, String> {
        let mut map = self.causation.event_metadata();
        map.insert(RECORDED_AT.to_owned(), self.recorded_at.clone());
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::format::TaskFormat;
    use std::collections::BTreeSet;
    use uuid::Uuid;

    #[test]
    fn ui_metadata_uses_one_id_for_all_three_roles_and_fixed_width_time() {
        let metadata = CommandMetadata::root("ui");
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
        let task = TaskAttempt::try_new(
            Uuid::new_v4(),
            TaskFormat::try_new("encore_record_ticket", 1).unwrap(),
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
