//! Causation metadata: who caused a command, carried by its events and by the tasks it publishes.
//!
//! A chain starts with a [`root`](Causation::root) command whose three ids are equal. Every task
//! the command publishes carries the ids ([`stamp_task`](Causation::stamp_task)); a command issued
//! while handling such a task continues the chain ([`from_task`](Causation::from_task)): same
//! correlation id, caused by the task. Filtering events and tasks by the correlation id shows the
//! whole chain, across aggregates and services that share these keys.
use crate::task::{NewTask, attempt::TaskAttempt};
use serde_json::{Map, Value};
use std::collections::HashMap;
use uuid::Uuid;

/// Metadata key of the command's own id.
pub const COMMAND_ID: &str = "command_id";
/// Metadata key of the id shared by every command, event and task of one chain.
pub const CORRELATION_ID: &str = "correlation_id";
/// Metadata key of the direct cause: the command itself for a root, otherwise the task.
pub const CAUSATION_ID: &str = "causation_id";
/// Event metadata key of the origin: chosen by the application for a root, `task:<type>` otherwise.
pub const SOURCE: &str = "source";

/// Who caused a command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Causation {
    pub command_id: Uuid,
    pub correlation_id: Uuid,
    pub causation_id: Uuid,
    pub source: String,
}

impl Causation {
    /// A new chain: all three ids are one fresh, time-ordered (v7) command id.
    pub fn root(source: impl Into<String>) -> Self {
        let command_id = Uuid::now_v7();
        Self {
            command_id,
            correlation_id: command_id,
            causation_id: command_id,
            source: source.into(),
        }
    }

    /// A command issued while handling `attempt`: inherits the task's correlation id, is caused by
    /// the task and is sourced `task:<type>`. A task without a valid correlation id (made by hand,
    /// or by a producer outside the convention) starts its chain with its own task id.
    pub fn from_task(attempt: &TaskAttempt) -> Self {
        let correlation_id = attempt
            .metadata()
            .get(CORRELATION_ID)
            .and_then(Value::as_str)
            .and_then(|id| Uuid::parse_str(id).ok())
            .unwrap_or(attempt.task_id());
        Self {
            command_id: Uuid::now_v7(),
            correlation_id,
            causation_id: attempt.task_id(),
            source: format!("task:{}", attempt.format().task_type()),
        }
    }

    /// The four keys for [`execute_with_metadata`](crate::cqrs::command::CommandExecutor::execute_with_metadata);
    /// applications may add their own.
    pub fn event_metadata(&self) -> HashMap<String, String> {
        HashMap::from([
            (COMMAND_ID.to_owned(), self.command_id.to_string()),
            (CORRELATION_ID.to_owned(), self.correlation_id.to_string()),
            (CAUSATION_ID.to_owned(), self.causation_id.to_string()),
            (SOURCE.to_owned(), self.source.clone()),
        ])
    }

    /// `task` with the three ids in its metadata, replacing only those keys. Call it after
    /// [`NewTask::with_metadata`], which replaces the whole object. The source is not stamped:
    /// the task's format already says what it is.
    pub fn stamp_task(&self, mut task: NewTask) -> NewTask {
        task.merge_metadata(Map::from_iter([
            (COMMAND_ID.to_owned(), Value::String(self.command_id.to_string())),
            (CORRELATION_ID.to_owned(), Value::String(self.correlation_id.to_string())),
            (CAUSATION_ID.to_owned(), Value::String(self.causation_id.to_string())),
        ]));
        task
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::format::{TaskFormat, TaskPayload};
    use serde_json::json;

    #[derive(serde::Serialize)]
    struct Ship {}

    impl TaskPayload for Ship {
        const TASK_TYPE: &'static str = "ship_order";
        const TASK_VERSION: i32 = 1;
    }

    fn attempt(metadata: Value) -> TaskAttempt {
        TaskAttempt::new(Uuid::new_v4(), TaskFormat::of::<Ship>().unwrap(), json!({}), metadata, 2).unwrap()
    }

    #[test]
    fn a_root_uses_one_time_ordered_id_for_all_three_roles() {
        let root = Causation::root("ui");
        assert_eq!(root.command_id.get_version_num(), 7);
        assert_eq!((root.correlation_id, root.causation_id), (root.command_id, root.command_id));
        assert_eq!(root.source, "ui");
        assert_ne!(Causation::root("ui").command_id, root.command_id);
    }

    #[test]
    fn a_command_from_a_task_inherits_the_correlation_and_is_caused_by_the_task() {
        let correlation_id = Uuid::now_v7();
        let task = attempt(json!({ CORRELATION_ID: correlation_id.to_string(), "tenant": "t-1" }));
        let causation = Causation::from_task(&task);
        assert_eq!(causation.correlation_id, correlation_id);
        assert_eq!(causation.causation_id, task.task_id());
        assert_eq!(causation.source, "task:ship_order");
        assert_eq!(causation.command_id.get_version_num(), 7);
        assert_ne!(causation.command_id, correlation_id);
        assert_ne!(causation.command_id, task.task_id());
    }

    #[test]
    fn a_task_without_a_valid_correlation_id_starts_the_chain_with_its_own_id() {
        for metadata in [
            json!({}),
            json!({ CORRELATION_ID: 42 }),
            json!({ CORRELATION_ID: "not-a-uuid" }),
        ] {
            let task = attempt(metadata);
            let causation = Causation::from_task(&task);
            assert_eq!(
                (causation.correlation_id, causation.causation_id),
                (task.task_id(), task.task_id())
            );
        }
    }

    #[test]
    fn events_get_exactly_the_four_keys() {
        let causation = Causation::root("seed");
        let id = causation.command_id.to_string();
        assert_eq!(
            causation.event_metadata(),
            HashMap::from([
                ("command_id".to_owned(), id.clone()),
                ("correlation_id".to_owned(), id.clone()),
                ("causation_id".to_owned(), id),
                ("source".to_owned(), "seed".to_owned()),
            ])
        );
    }

    #[test]
    fn stamping_a_task_replaces_only_the_three_ids_and_keeps_application_keys() {
        let causation = Causation::from_task(&attempt(json!({})));
        let task = NewTask::new(Ship {})
            .unwrap()
            .with_metadata(json!({ "order_id": "o-7", "command_id": "stale", "source": "app" }))
            .unwrap();
        let id = task.id();
        let stamped = causation.stamp_task(task);
        assert_eq!(stamped.id(), id);
        assert_eq!(
            stamped.metadata(),
            &json!({
                "order_id": "o-7",
                "source": "app",
                "command_id": causation.command_id.to_string(),
                "correlation_id": causation.correlation_id.to_string(),
                "causation_id": causation.causation_id.to_string(),
            })
        );
    }
}
