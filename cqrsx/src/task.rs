//! Durable task publication, handling, delivery and execution.
//!
//! Business sequencing belongs to aggregate transitions. A task represents one
//! replay-safe operation; delivery may be concurrent, out of order and repeated.
pub mod adapter;
pub mod attempt;
pub mod context;
pub mod dependencies;
pub mod dispatcher;
pub mod executor;
pub mod format;
pub mod handler;
pub mod handlers;
pub mod policy;
pub mod queue;
pub mod result;
#[cfg(feature = "worker")]
pub mod runtime;
#[cfg(test)]
pub(crate) mod test_support;
pub mod testing;

use crate::task::format::{TaskFormat, TaskFormatError, TaskPayload};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value, json};
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

/// The attempt budget of a task that sets none of its own: no [`with_max_attempts`](NewTask::with_max_attempts) and
/// no policy of its type (`task::policy::Policies`).
pub const DEFAULT_MAX_ATTEMPTS: i32 = 10;

#[derive(Clone, Debug, Default, PartialEq)]
pub enum TaskSchedule {
    #[default]
    Now,
    After(Duration),
    At(DateTime<Utc>),
}
impl TaskSchedule {
    fn validate(&self) -> Result<(), TaskValidationError> {
        match self {
            Self::After(delay) if delay.as_millis() > i64::MAX as u128 => Err(TaskValidationError::InvalidSchedule),
            _ => Ok(()),
        }
    }
}

/// A durable follow-up a command publishes with its events: a versioned payload, metadata, an initial
/// schedule and a claim limit. Its id is random (v4); the core replaces it with an id derived from the
/// command's causation when it plans the task (`Causation::task_id`).
#[derive(Clone, Debug, PartialEq)]
pub struct NewTask {
    id: Uuid,
    format: TaskFormat,
    payload: Value,
    metadata: Value,
    schedule: TaskSchedule,
    max_attempts: i32,
}
impl NewTask {
    /// A task of `payload`; the fields of `T::LABELS` move from its body to its metadata.
    pub fn new<T: TaskPayload>(payload: T) -> Result<Self, TaskValidationError> {
        let task = Self::from_payload(
            TaskFormat::of::<T>()?,
            serde_json::to_value(payload).map_err(|_| TaskValidationError::Serialization)?,
        );
        if T::LABELS.is_empty() {
            return task;
        }
        let mut task = task?;
        move_labels(T::LABELS, &mut task.payload, &mut task.metadata);
        Ok(task)
    }
    /// Dynamic producers must still supply an object and a validated format.
    pub fn from_payload(format: TaskFormat, payload: Value) -> Result<Self, TaskValidationError> {
        validate_payload(&payload)?;
        Ok(Self {
            id: Uuid::new_v4(),
            format,
            payload,
            metadata: json!({}),
            schedule: TaskSchedule::Now,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        })
    }
    pub fn scheduled(mut self, schedule: TaskSchedule) -> Result<Self, TaskValidationError> {
        schedule.validate()?;
        self.schedule = schedule;
        Ok(self)
    }
    pub fn with_metadata(mut self, metadata: Value) -> Result<Self, TaskValidationError> {
        validate_metadata(&metadata)?;
        self.metadata = metadata;
        Ok(self)
    }
    /// Insert `entries` into the metadata object, replacing equal keys. Metadata is always an
    /// object, so nothing can fail.
    pub(crate) fn merge_metadata(&mut self, entries: Map<String, Value>) {
        if let Value::Object(metadata) = &mut self.metadata {
            metadata.extend(entries);
        }
    }

    /// The same task under `id`: the core gives every planned task the id derived from its command.
    pub(crate) fn with_id(mut self, id: Uuid) -> Self {
        self.id = id;
        self
    }

    /// Total claims, including the initial claim and skipped invocations.
    pub fn with_max_attempts(mut self, max_attempts: i32) -> Result<Self, TaskValidationError> {
        if max_attempts <= 0 {
            return Err(TaskValidationError::InvalidAttemptLimit);
        }
        self.max_attempts = max_attempts;
        Ok(self)
    }
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn format(&self) -> &TaskFormat {
        &self.format
    }
    pub fn payload(&self) -> &Value {
        &self.payload
    }
    pub fn metadata(&self) -> &Value {
        &self.metadata
    }
    pub fn schedule(&self) -> &TaskSchedule {
        &self.schedule
    }
    pub fn max_attempts(&self) -> i32 {
        self.max_attempts
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TaskValidationError {
    #[error(transparent)]
    Format(#[from] TaskFormatError),
    #[error("task payload serialization failed")]
    Serialization,
    #[error("task payload must be a JSON object")]
    PayloadNotObject,
    #[error("task metadata must be a JSON object")]
    MetadataNotObject,
    #[error("task attempt limit must be positive")]
    InvalidAttemptLimit,
    #[error("delivery attempt must be positive")]
    InvalidAttempt,
    #[error("task schedule delay is not representable")]
    InvalidSchedule,
}
/// Moves the keys `labels` of the `payload` object to the `metadata` object; a key the payload lacks moves nothing.
/// Both are objects: every constructor validates them.
fn move_labels(labels: &[&str], payload: &mut Value, metadata: &mut Value) {
    for label in labels {
        if let Some(value) = payload.as_object_mut().and_then(|payload| payload.remove(*label)) {
            metadata[*label] = value;
        }
    }
}

/// A copy of the `payload` object with the keys `labels` it lacks taken back from the `metadata` object: the body a
/// labelled payload decodes from. Both are objects: every constructor validates them.
fn restore_labels(labels: &[&str], payload: &Value, metadata: &Value) -> Value {
    let mut restored = payload.clone();
    for label in labels {
        if restored.get(*label).is_none()
            && let Some(value) = metadata.get(*label)
        {
            restored[*label] = value.clone();
        }
    }
    restored
}

fn validate_payload(payload: &Value) -> Result<(), TaskValidationError> {
    if payload.is_object() {
        Ok(())
    } else {
        Err(TaskValidationError::PayloadNotObject)
    }
}
fn validate_metadata(metadata: &Value) -> Result<(), TaskValidationError> {
    if metadata.is_object() {
        Ok(())
    } else {
        Err(TaskValidationError::MetadataNotObject)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::attempt::TaskAttempt;
    use crate::task::format::TaskFormatError;
    use crate::task::test_support::DummyTask;
    use serde::{Serialize, Serializer};
    #[test]
    fn construction_validates_format_objects_attempts_and_schedule() {
        let task = NewTask::new(DummyTask { value: "runtime" }).unwrap();
        assert_eq!(task.payload(), &json!({"value": "runtime"}));
        assert_eq!(task.format().task_version(), 1);
        assert_eq!(task.format().to_string(), "dummy_task v1");
        assert_eq!(task.max_attempts(), 10);
        assert_eq!(task.metadata(), &json!({}));
        assert_eq!(task.schedule(), &TaskSchedule::Now);
        let id = task.id();
        let task = task
            .with_metadata(json!({"trace": "test"}))
            .unwrap()
            .with_max_attempts(2)
            .unwrap()
            .scheduled(TaskSchedule::After(Duration::ZERO))
            .unwrap();
        assert_eq!(task.id(), id);
        assert_eq!(task.max_attempts(), 2);
        assert!(task.clone().with_metadata(json!([])).is_err());
        assert!(task.clone().with_max_attempts(0).is_err());
        assert!(task.clone().scheduled(TaskSchedule::After(Duration::MAX)).is_err());
        assert!(task.scheduled(TaskSchedule::At(Utc::now())).is_ok());
        assert_eq!(TaskFormat::new("", 1), Err(TaskFormatError::EmptyType));
        assert_eq!(TaskFormat::new("valid", 0), Err(TaskFormatError::InvalidVersion));
        assert!(NewTask::from_payload(TaskFormat::new("valid", 1).unwrap(), json!([])).is_err());
        struct Broken;
        impl Serialize for Broken {
            fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("broken"))
            }
        }
        impl TaskPayload for Broken {
            const TASK_TYPE: &'static str = "broken";
            const TASK_VERSION: i32 = 1;
        }
        assert_eq!(NewTask::new(Broken), Err(TaskValidationError::Serialization));
    }

    #[test]
    fn a_task_without_a_budget_of_its_own_gets_the_default_of_ten_attempts() {
        assert_eq!(DEFAULT_MAX_ATTEMPTS, 10);
        assert_eq!(
            NewTask::new(DummyTask { value: "runtime" }).unwrap().max_attempts(),
            DEFAULT_MAX_ATTEMPTS
        );
        let format = TaskFormat::new("dummy_task", 1).unwrap();
        assert_eq!(
            NewTask::from_payload(format, json!({})).unwrap().max_attempts(),
            DEFAULT_MAX_ATTEMPTS
        );
    }

    /// A payload whose tenant is kept in the task's metadata; `absent` is a label its body never has.
    #[derive(Serialize)]
    struct Labelled {
        amount: u32,
        tenant: &'static str,
    }

    impl TaskPayload for Labelled {
        const TASK_TYPE: &'static str = "labelled";
        const TASK_VERSION: i32 = 1;
        const LABELS: &'static [&'static str] = &["tenant", "absent"];
    }

    #[test]
    fn labels_move_from_the_body_to_the_metadata_and_an_unlabelled_task_is_as_before() {
        let labelled = Labelled {
            amount: 80,
            tenant: "acme",
        };
        let task = NewTask::new(labelled).unwrap();
        assert_eq!(task.payload(), &json!({ "amount": 80 }));
        assert_eq!(
            task.metadata(),
            &json!({ "tenant": "acme" }),
            "a label the body lacks moves nothing"
        );
        let unlabelled = NewTask::new(DummyTask { value: "runtime" }).unwrap();
        assert_eq!(
            (unlabelled.payload(), unlabelled.metadata()),
            (&json!({ "value": "runtime" }), &json!({}))
        );
        let attempt = TaskAttempt::first(
            &Labelled {
                amount: 80,
                tenant: "acme",
            },
            json!({ "correlation_id": "c-1" }),
        )
        .unwrap();
        assert_eq!(attempt.payload(), &json!({ "amount": 80 }));
        assert_eq!(attempt.metadata(), &json!({ "correlation_id": "c-1", "tenant": "acme" }));
        let first = TaskAttempt::first(&DummyTask { value: "runtime" }, json!({ "correlation_id": "c-1" })).unwrap();
        assert_eq!(
            (first.payload(), first.metadata()),
            (&json!({ "value": "runtime" }), &json!({ "correlation_id": "c-1" }))
        );
    }

    #[test]
    fn labels_return_to_a_copy_of_the_body_unless_it_has_them() {
        let metadata = json!({ "tenant": "acme", "correlation_id": "c-1" });
        assert_eq!(
            restore_labels(Labelled::LABELS, &json!({ "amount": 80 }), &metadata),
            json!({ "amount": 80, "tenant": "acme" }),
            "only the labels come back, and a label the metadata lacks stays away"
        );
        assert_eq!(
            restore_labels(Labelled::LABELS, &json!({ "amount": 80, "tenant": "body" }), &metadata),
            json!({ "amount": 80, "tenant": "body" })
        );
    }

    #[test]
    fn with_id_changes_only_the_id() {
        let task = NewTask::new(DummyTask { value: "runtime" })
            .unwrap()
            .with_metadata(json!({"trace": "test"}))
            .unwrap()
            .with_max_attempts(3)
            .unwrap()
            .scheduled(TaskSchedule::After(Duration::from_secs(2)))
            .unwrap();
        let id = Uuid::now_v7();
        let renamed = task.clone().with_id(id);
        assert_eq!(renamed.id(), id);
        assert_eq!(
            (
                renamed.format(),
                renamed.payload(),
                renamed.metadata(),
                renamed.schedule(),
                renamed.max_attempts()
            ),
            (
                task.format(),
                task.payload(),
                task.metadata(),
                task.schedule(),
                task.max_attempts()
            )
        );
    }

    #[test]
    fn a_payload_with_an_invalid_declared_format_cannot_become_a_task() {
        #[derive(Serialize)]
        struct InvalidFormat;
        impl TaskPayload for InvalidFormat {
            const TASK_TYPE: &'static str = "invalid_format";
            const TASK_VERSION: i32 = 0;
        }
        assert_eq!(
            NewTask::new(InvalidFormat),
            Err(TaskValidationError::Format(TaskFormatError::InvalidVersion))
        );
    }
}
