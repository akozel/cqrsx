//! The command pipeline shared by both aggregates, around the cqrsx command port.
//!
//! Every use case ends in [`CommandService::run`]: `execute_with_metadata` (the event and the tasks the
//! [workflows](crate::application::workflows) plan from it, in one commit) → outcome → activity log. The read models follow from the journal (epic 03/04, D-17): an
//! accepted command means committed, the projections apply it a moment later. Metadata (the cqrsx
//! causation keys `command_id`, `correlation_id`, `causation_id`, `source`, plus `recorded_at`)
//! travels through `execute_with_metadata` (decision D-04) and ends up in the `events.metadata`
//! column of every committed event.
use crate::application::activity::ActivityHub;
use chrono::{SecondsFormat, Utc};
use cqrsx::{
    cqrs::{
        aggregate::Aggregate,
        command::{CommandExecutor, CommandGatewayError, causation::Causation},
    },
    task::attempt::TaskAttempt,
};
use serde_json::{Value, json};
use std::{collections::HashMap, marker::PhantomData, sync::Arc};

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

/// Outcome of a command after the gateway error has been interpreted (§6.6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandOutcome<E> {
    /// Committed: the events and their tasks are in the journal; the read models follow.
    Accepted,
    Rejected(E),
    Conflict,
    Unavailable,
}

impl<E> CommandOutcome<E> {
    pub fn is_accepted(&self) -> bool {
        matches!(self, CommandOutcome::Accepted)
    }
}

/// Maps the library outcome to the demo outcome.
pub fn map_gateway_result<T, E>(result: Result<T, CommandGatewayError<E>>) -> CommandOutcome<E> {
    match result {
        Ok(_) => CommandOutcome::Accepted,
        Err(CommandGatewayError::Domain(error)) => CommandOutcome::Rejected(error),
        Err(CommandGatewayError::Conflict) => CommandOutcome::Conflict,
        Err(CommandGatewayError::Unavailable) => CommandOutcome::Unavailable,
    }
}

/// What the HTTP layer and task handlers get back: the interpreted outcome plus the formats of the
/// tasks committed with the command's events (none unless it was accepted).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Executed<E> {
    pub outcome: CommandOutcome<E>,
    pub tasks: Vec<String>,
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
    aggregate: PhantomData<fn() -> A>,
}

impl<A, E> CommandService<A, E>
where
    A: Aggregate,
    A::Command: Send,
    A::Error: Rejection,
    E: CommandExecutor<A>,
{
    pub fn new(executor: E, activity: Arc<ActivityHub>) -> Self {
        Self {
            executor,
            activity,
            aggregate: PhantomData,
        }
    }

    /// Runs one command; the executor's planner plans its tasks from the decision, and they commit
    /// atomically with its event. A rejected command writes nothing, so a task is never published
    /// without its event.
    pub async fn run(&self, label: CommandLabel<'_>, command: A::Command, metadata: CommandMetadata) -> Executed<A::Error> {
        let result = self
            .executor
            .execute_with_metadata(label.aggregate_id, command, metadata.to_map())
            .await;
        let task_formats: Vec<String> = match &result {
            Ok(report) => report.tasks.iter().map(|task| task.format().to_string()).collect(),
            Err(_) => Vec::new(),
        };
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
        CommandOutcome::Accepted => {
            activity.publish("command.accepted", merge(json!({ "tasks": task_formats })));
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
                merge(json!({ "outcome": "unavailable", "message": "not run: storage unavailable or aggregate busy" })),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::concert::{Concert, ConcertCommand, ConcertError, commands::AnnounceConcert};
    use cqrsx::{
        cqrs::CommitReport,
        task::{NewTask, format::TaskFormat},
    };
    use std::collections::BTreeSet;
    use uuid::Uuid;

    /// Accepts every command and reports `tasks` as committed with it.
    struct Accepting {
        tasks: Vec<NewTask>,
    }

    impl CommandExecutor<Concert> for Accepting {
        async fn execute_with_metadata(
            &self,
            _: &str,
            _: ConcertCommand,
            _: HashMap<String, String>,
        ) -> Result<CommitReport<Concert>, CommandGatewayError<ConcertError>> {
            Ok(CommitReport {
                events: Vec::new(),
                tasks: self.tasks.clone(),
            })
        }
    }

    fn announce() -> ConcertCommand {
        ConcertCommand::AnnounceConcert(AnnounceConcert {
            artist: "A".into(),
            city: "C".into(),
            date: chrono::NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
        })
    }

    fn label() -> CommandLabel<'static> {
        CommandLabel {
            aggregate_id: "c-1",
            command_type: "AnnounceConcert",
            fan_id: None,
        }
    }

    #[tokio::test]
    async fn an_accepted_command_is_published_once_with_its_tasks_and_no_read_model_news() {
        let activity = Arc::new(ActivityHub::new(8, 8));
        let service = CommandService::<Concert, _>::new(Accepting { tasks: vec![] }, activity.clone());
        let executed = service.run(label(), announce(), CommandMetadata::ui()).await;
        assert_eq!(executed.outcome, CommandOutcome::Accepted);
        let (entries, _) = activity.subscribe();
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_eq!(entries[0].kind, "command.accepted");
        assert_eq!(entries[0].fields["tasks"], json!([]));
        assert!(
            !entries[0].fields.contains_key("warnings"),
            "the read models follow from the journal"
        );
    }

    #[tokio::test]
    async fn an_accepted_command_reports_the_tasks_its_commit_published() {
        let activity = Arc::new(ActivityHub::new(8, 8));
        let task = NewTask::from_payload(TaskFormat::new("encore_record_ticket", 1).unwrap(), json!({})).unwrap();
        let service = CommandService::<Concert, _>::new(Accepting { tasks: vec![task] }, activity.clone());
        let executed = service.run(label(), announce(), CommandMetadata::ui()).await;
        assert_eq!(executed.tasks, ["encore_record_ticket v1"]);
        let (entries, _) = activity.subscribe();
        assert_eq!(entries[0].fields["tasks"], json!(["encore_record_ticket v1"]));
    }

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
    fn gateway_results_map_to_outcomes() {
        assert_eq!(map_gateway_result::<_, ConcertError>(Ok(())), CommandOutcome::Accepted);
        assert_eq!(
            map_gateway_result::<(), _>(Err(CommandGatewayError::Domain(ConcertError::SoldOut))),
            CommandOutcome::Rejected(ConcertError::SoldOut)
        );
        assert_eq!(
            map_gateway_result::<(), ConcertError>(Err(CommandGatewayError::Conflict)),
            CommandOutcome::Conflict
        );
        assert_eq!(
            map_gateway_result::<(), ConcertError>(Err(CommandGatewayError::Unavailable)),
            CommandOutcome::Unavailable
        );
    }
}
