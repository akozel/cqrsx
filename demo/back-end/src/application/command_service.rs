//! The command pipeline shared by both aggregates, around the cqrsx command port.
//!
//! Every use case ends in [`CommandService::run`]: box-office lock → `execute_with_metadata` (the
//! event and its outbox tasks in one commit) → outcome → activity log. Metadata (the cqrsx
//! causation keys `command_id`, `correlation_id`, `causation_id`, `source`, plus `recorded_at`)
//! travels through `execute_with_metadata` (decision D-04) and ends up in the `events.metadata`
//! column of every committed event.
use crate::application::{activity::ActivityHub, lock::BoxOfficeLock};
use chrono::{SecondsFormat, Utc};
use cqrsx::{
    cqrs::{
        aggregate::Aggregate,
        command::{CommandExecutor, CommandGatewayError, causation::Causation},
    },
    task::{NewTask, attempt::TaskAttempt},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{collections::HashMap, fmt::Display, marker::PhantomData, sync::Arc};

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

/// How a domain rejection is reported to the API and the activity log: the stable code from the
/// domain plus the parameters the UI texts interpolate.
pub trait Rejection {
    fn code(&self) -> &'static str;
    fn params(&self) -> Value;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Warning {
    pub code: &'static str,
    pub projection: String,
}

/// Outcome of a command after the gateway error has been interpreted (§6.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandOutcome<E> {
    Accepted {
        warnings: Vec<Warning>,
    },
    Rejected(E),
    Conflict,
    Unavailable,
    /// The box-office lock is held: closed for a moment while the read models are rebuilt.
    Locked,
}

impl<E> CommandOutcome<E> {
    pub fn is_accepted(&self) -> bool {
        matches!(self, CommandOutcome::Accepted { .. })
    }
}

/// Maps the library outcome to the demo outcome; a failed projection is reported under the
/// handler name `CommittedWithEventHandlerFailure` carries (the read-model collection name).
pub fn map_gateway_result<E>(result: Result<(), CommandGatewayError<E>>) -> CommandOutcome<E> {
    match result {
        Ok(()) => CommandOutcome::Accepted { warnings: vec![] },
        Err(CommandGatewayError::Domain(error)) => CommandOutcome::Rejected(error),
        Err(CommandGatewayError::Conflict) => CommandOutcome::Conflict,
        Err(CommandGatewayError::Unavailable) => CommandOutcome::Unavailable,
        Err(CommandGatewayError::CommittedWithEventHandlerFailure { handlers, .. }) => CommandOutcome::Accepted {
            warnings: handlers
                .into_iter()
                .map(|handler| Warning {
                    code: "projection_failed",
                    projection: handler.name,
                })
                .collect(),
        },
    }
}

/// What the HTTP layer and task handlers get back: the interpreted outcome plus the formats of the
/// tasks that were bound to the command (published only if it was accepted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Executed<E> {
    pub outcome: CommandOutcome<E>,
    pub tasks: Vec<String>,
}

impl<E> Executed<E> {
    pub fn locked() -> Self {
        Self {
            outcome: CommandOutcome::Locked,
            tasks: vec![],
        }
    }

    /// The outbox tasks could not be built, so the command was not executed (and is not logged).
    pub fn tasks_unavailable(error: impl Display) -> Self {
        tracing::error!("cannot build outbox tasks: {error}");
        Self {
            outcome: CommandOutcome::Unavailable,
            tasks: vec![],
        }
    }
}

/// What every `command.*` activity entry says about the command, whatever its outcome.
#[derive(Debug, Clone, Copy)]
pub struct CommandLabel<'a> {
    pub aggregate_id: &'a str,
    pub command_type: &'static str,
    pub fan_id: Option<&'a str>,
}

/// One long-lived service per aggregate around its command executor.
pub struct CommandService<A, E> {
    executor: E,
    activity: Arc<ActivityHub>,
    lock: Arc<BoxOfficeLock>,
    aggregate: PhantomData<fn() -> A>,
}

impl<A, E> CommandService<A, E>
where
    A: Aggregate,
    A::Command: Send,
    A::Error: Rejection,
    E: CommandExecutor<A>,
{
    pub fn new(executor: E, activity: Arc<ActivityHub>, lock: Arc<BoxOfficeLock>) -> Self {
        Self {
            executor,
            activity,
            lock,
            aggregate: PhantomData,
        }
    }

    pub fn lock(&self) -> &Arc<BoxOfficeLock> {
        &self.lock
    }

    pub fn is_locked(&self) -> bool {
        self.lock.is_locked()
    }

    /// Runs one command with the tasks that must be published atomically with its event. A
    /// rejected command writes nothing, so a task is never published without its event.
    pub async fn run(
        &self,
        label: CommandLabel<'_>,
        command: A::Command,
        metadata: CommandMetadata,
        tasks: Vec<NewTask>,
    ) -> Executed<A::Error> {
        if self.lock.is_locked() {
            return Executed::locked();
        }
        let task_formats: Vec<String> = tasks.iter().map(|t| t.format().to_string()).collect();
        let result = self
            .executor
            .execute_with_metadata(label.aggregate_id, command, metadata.to_map(), tasks)
            .await;
        let outcome = map_gateway_result(result);
        publish_outcome(&self.activity, A::TYPE, &label, &metadata, &task_formats, &outcome);
        Executed {
            outcome,
            tasks: task_formats,
        }
    }
}

fn publish_outcome<E: Rejection>(
    activity: &ActivityHub,
    aggregate_type: &str,
    label: &CommandLabel<'_>,
    metadata: &CommandMetadata,
    task_formats: &[String],
    outcome: &CommandOutcome<E>,
) {
    let base = json!({
        "aggregate_type": aggregate_type,
        "aggregate_id": label.aggregate_id,
        "command_id": metadata.causation.command_id,
        "correlation_id": metadata.causation.correlation_id,
        "source": metadata.causation.source,
        "command_type": label.command_type,
        "fan_id": label.fan_id,
    });
    let merge = |extra: Value| {
        let mut map = base.as_object().cloned().unwrap_or_default();
        if let Value::Object(extra) = extra {
            map.extend(extra);
        }
        Value::Object(map)
    };
    match outcome {
        CommandOutcome::Accepted { warnings } => {
            activity.publish(
                "command.accepted",
                merge(json!({ "tasks": task_formats, "warnings": warnings })),
            );
            for warning in warnings {
                activity.publish(
                    "projection.failed",
                    json!({ "projection": warning.projection, "view_id": label.aggregate_id, "message": "event handler failed after commit" }),
                );
            }
        }
        CommandOutcome::Rejected(error) => {
            activity.publish(
                "command.rejected",
                merge(json!({ "code": error.code(), "params": error.params() })),
            );
        }
        CommandOutcome::Conflict => {
            activity.publish(
                "command.failed",
                merge(json!({ "outcome": "conflict", "message": "concurrent change; retries exhausted" })),
            );
        }
        CommandOutcome::Unavailable => {
            activity.publish(
                "command.failed",
                merge(json!({ "outcome": "unavailable", "message": "storage unavailable" })),
            );
        }
        CommandOutcome::Locked => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::concert::ConcertError;
    use cqrsx::{cqrs::command::FailedHandler, task::format::TaskFormat};
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

    #[test]
    fn gateway_results_map_to_outcomes_and_projection_names() {
        assert_eq!(
            map_gateway_result::<ConcertError>(Ok(())),
            CommandOutcome::Accepted { warnings: vec![] }
        );
        assert_eq!(
            map_gateway_result(Err(CommandGatewayError::Domain(ConcertError::SoldOut))),
            CommandOutcome::Rejected(ConcertError::SoldOut)
        );
        assert_eq!(
            map_gateway_result::<ConcertError>(Err(CommandGatewayError::Conflict)),
            CommandOutcome::Conflict
        );
        assert_eq!(
            map_gateway_result::<ConcertError>(Err(CommandGatewayError::Unavailable)),
            CommandOutcome::Unavailable
        );
        let failed = |index: usize, name: &str| FailedHandler {
            index,
            name: name.into(),
        };
        assert_eq!(
            map_gateway_result::<ConcertError>(Err(CommandGatewayError::CommittedWithEventHandlerFailure {
                last_sequence: 7,
                handlers: vec![failed(0, "concert_halls"), failed(5, "fan_wallets")]
            })),
            CommandOutcome::Accepted {
                warnings: vec![
                    Warning {
                        code: "projection_failed",
                        projection: "concert_halls".into()
                    },
                    Warning {
                        code: "projection_failed",
                        projection: "fan_wallets".into()
                    },
                ]
            }
        );
    }
}
