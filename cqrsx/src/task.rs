//! A task as data: what a command publishes with its events and what a delivery attempt carries.
//!
//! Business sequencing belongs to aggregate transitions. A task represents one
//! repeat-safe operation; delivery may be concurrent, out of order and repeated. Running tasks is the work of the
//! `worker` module.
pub mod attempt;
pub mod format;
pub mod policy;

use crate::{diagnostics, task::format::TaskFormat};
use chrono::{DateTime, Utc};
pub use cqrsx_macros::Task;
use serde::{
    Serialize,
    de::{DeserializeOwned, value::MapDeserializer},
};
use serde_json::{Map, Value, json};
use std::{num::NonZeroU16, time::Duration};
use thiserror::Error;
use uuid::Uuid;

/// Versioned wire DTO. Retain supported versions while queued work exists.
///
/// The type and the version are stored with every queued task and route it to its handler, so they never
/// change for a payload's shape; `#[derive(Task)]` writes them from `#[task(name = "…", version = N)]`.
///
/// ```
/// use cqrsx::task::{NewTask, Task, format::TaskFormat};
/// use serde::Serialize;
///
/// #[derive(Serialize, Task)]
/// #[task(name = "send_e_ticket", version = 2)]
/// struct SendETicket {
///     seat: String,
/// }
///
/// assert_eq!((SendETicket::TYPE, SendETicket::VERSION), ("send_e_ticket", 2));
/// assert_eq!(TaskFormat::of::<SendETicket>().to_string(), "send_e_ticket v2");
/// let task = NewTask::try_new(SendETicket { seat: "B4".into() }).unwrap();
/// assert_eq!(task.payload()["seat"], "B4");
/// ```
///
/// A version is positive, as [`TaskFormat::try_new`] requires; the derive refuses another at compile time:
///
/// ```compile_fail
/// # use cqrsx::task::Task;
/// # use serde::Serialize;
/// #[derive(Serialize, Task)]
/// #[task(name = "send_e_ticket", version = 0)]
/// struct SendETicket {
///     seat: String,
/// }
/// ```
///
/// A field marked `#[label]` is kept in the task's metadata, under its name, instead of its body: what is said of the
/// task besides its business data (a tenant, say), never a copy of the body. [`NewTask::try_new`](crate::task::NewTask::try_new)
/// moves it there, and the JSON adapter puts it back before it decodes the payload, so the handler gets the whole
/// value:
///
/// ```
/// use cqrsx::task::{NewTask, Task};
/// use serde::Serialize;
/// use serde_json::json;
///
/// #[derive(Serialize, Task)]
/// #[task(name = "issue_refund", version = 1)]
/// struct IssueRefund {
///     amount: u32,
///     #[label]
///     tenant: String,
/// }
///
/// assert_eq!(IssueRefund::LABELS, ["tenant"]);
/// let task = NewTask::try_new(IssueRefund { amount: 80, tenant: "acme".into() }).unwrap();
/// assert_eq!(task.payload(), &json!({ "amount": 80 }));
/// assert_eq!(task.metadata(), &json!({ "tenant": "acme" }));
/// ```
///
/// A label moves under its field's name, so a serde attribute that renames or drops the key does not compile:
///
/// ```compile_fail
/// # use cqrsx::task::Task;
/// # use serde::Serialize;
/// #[derive(Serialize, Task)]
/// #[task(name = "issue_refund", version = 1)]
/// struct IssueRefund {
///     amount: u32,
///     #[label]
///     #[serde(rename = "tenant_id")]
///     tenant: String,
/// }
/// ```
///
/// A `Task` written by hand is checked when its format is taken ([`TaskFormat::of`]), so `cargo build` refuses
/// a version below 1 there:
///
/// ```compile_fail,E0080
/// use cqrsx::task::{Task, format::TaskFormat};
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct SendETicket {}
///
/// impl Task for SendETicket {
///     const TYPE: &'static str = "send_e_ticket";
///     const VERSION: i32 = 0;
/// }
///
/// fn main() {
///     println!("{}", TaskFormat::of::<SendETicket>());
/// }
/// ```
pub trait Task: Serialize {
    const TYPE: &'static str;
    const VERSION: i32;
    /// The fields kept in the task's metadata instead of its body, by name (`#[label]`); none by default.
    const LABELS: &'static [&'static str] = &[];
}

/// The attempt budget of a task that sets none of its own: no [`with_max_attempts`](NewTask::with_max_attempts) and
/// no policy of its type (`task::policy::Policies`).
pub const DEFAULT_MAX_ATTEMPTS: NonZeroU16 = NonZeroU16::new(10).unwrap();

/// When a task becomes eligible: at once, after a delay from the commit or at a time. Built by [`now`](Self::now),
/// [`after`](Self::after) (the one that can refuse) or [`at`](Self::at).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskSchedule {
    kind: ScheduleKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ScheduleKind {
    Now,
    After(Duration),
    At(DateTime<Utc>),
}

impl Default for TaskSchedule {
    fn default() -> Self {
        Self::now()
    }
}

impl TaskSchedule {
    /// Eligible at once.
    pub fn now() -> Self {
        Self { kind: ScheduleKind::Now }
    }

    /// Eligible `delay` after the commit. Refused when the delay in milliseconds does not fit a 64-bit integer.
    pub fn after(delay: Duration) -> Result<Self, TaskValidationError> {
        if delay.as_millis() > i64::MAX as u128 {
            return Err(TaskValidationError::DelayTooLong);
        }
        Ok(Self {
            kind: ScheduleKind::After(delay),
        })
    }

    /// Eligible at the time `at`.
    pub fn at(at: DateTime<Utc>) -> Self {
        Self {
            kind: ScheduleKind::At(at),
        }
    }

    /// The delay after the commit, for a schedule built by [`after`](Self::after).
    pub fn delay(&self) -> Option<Duration> {
        match self.kind {
            ScheduleKind::After(delay) => Some(delay),
            ScheduleKind::Now | ScheduleKind::At(_) => None,
        }
    }

    /// The time, for a schedule built by [`at`](Self::at).
    pub fn time(&self) -> Option<DateTime<Utc>> {
        match self.kind {
            ScheduleKind::At(at) => Some(at),
            ScheduleKind::Now | ScheduleKind::After(_) => None,
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
    /// The keys [`try_new`](Self::try_new) moved from the payload to the metadata, which `with_metadata` keeps.
    labels: &'static [&'static str],
    schedule: TaskSchedule,
    max_attempts: NonZeroU16,
}
impl NewTask {
    /// A task of `payload`; the fields of `T::LABELS` move from its body to its metadata. Refused when the payload
    /// does not serialize to a JSON object.
    pub fn try_new<T: Task>(payload: T) -> Result<Self, TaskValidationError> {
        let task = Self::from_payload(
            TaskFormat::of::<T>(),
            serde_json::to_value(payload).map_err(|_| TaskValidationError::Serialization)?,
        );
        if T::LABELS.is_empty() {
            return task;
        }
        let mut task = task?;
        move_labels(T::LABELS, &mut task.payload, &mut task.metadata);
        task.labels = T::LABELS;
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
            labels: &[],
            schedule: TaskSchedule::now(),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        })
    }
    #[must_use]
    pub fn with_schedule(mut self, schedule: TaskSchedule) -> Self {
        self.schedule = schedule;
        self
    }
    /// Replaces the metadata with the object `metadata`, but keeps the labels [`try_new`](Self::try_new) moved here from the
    /// payload: the handler decodes the payload with them, so a label is never lost, and a key of `metadata` that names
    /// a label keeps the label's value. A task without labels gets `metadata` as it is.
    #[must_use]
    pub fn with_metadata(mut self, mut metadata: Map<String, Value>) -> Self {
        for label in self.labels {
            if let Some(value) = self.metadata.get_mut(*label).map(Value::take) {
                metadata.insert((*label).to_owned(), value);
            }
        }
        self.metadata = Value::Object(metadata);
        self
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
    #[must_use]
    pub fn with_max_attempts(mut self, max_attempts: NonZeroU16) -> Self {
        self.max_attempts = max_attempts;
        self
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
    pub fn max_attempts(&self) -> NonZeroU16 {
        self.max_attempts
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TaskValidationError {
    #[error("task payload serialization failed")]
    Serialization,
    #[error("task payload must be a JSON object")]
    PayloadNotObject,
    #[error("task metadata must be a JSON object")]
    MetadataNotObject,
    #[error("delivery attempt must be positive")]
    InvalidAttempt,
    #[error("task schedule delay is too long")]
    DelayTooLong,
}
/// Moves the keys `labels` of the `payload` object to the `metadata` object; a key the payload lacks moves nothing.
/// Both are objects: every constructor validates them. Were one not (a bug, logged), nothing moves.
fn move_labels(labels: &[&str], payload: &mut Value, metadata: &mut Value) {
    let (Value::Object(payload), Value::Object(metadata)) = (payload, metadata) else {
        diagnostics::log_broken_invariant("task", "the payload and the metadata of a task are objects");
        return;
    };
    for label in labels {
        if let Some(value) = payload.remove(*label) {
            metadata.insert((*label).to_owned(), value);
        }
    }
}

/// Decodes `P` from the fields of the `payload` object and the keys `labels` it lacks, taken back from the `metadata`
/// object: the body a labelled payload decodes from, read in place, neither object copied. The payload is an object:
/// every constructor validates it. Were it not (a bug, logged), `P` decodes from the payload alone.
pub(crate) fn decode_with_labels<P: DeserializeOwned>(
    labels: &[&str],
    payload: &Value,
    metadata: &Value,
) -> Result<P, serde_json::Error> {
    let Value::Object(fields) = payload else {
        diagnostics::log_broken_invariant("task", "the payload of a task is an object");
        return P::deserialize(payload);
    };
    let missing = labels.iter().filter(|label| !fields.contains_key(**label));
    let restored = missing.filter_map(|label| metadata.get(*label).map(|value| (*label, value)));
    let entries = fields.iter().map(|(key, value)| (key.as_str(), value)).chain(restored);
    P::deserialize(MapDeserializer::<_, serde_json::Error>::new(entries))
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
    use crate::worker::test_support::DummyTask;
    use serde::{Deserialize, Serialize, Serializer};
    #[test]
    fn construction_validates_format_objects_attempts_and_schedule() {
        let task = NewTask::try_new(DummyTask { value: "runtime" }).unwrap();
        assert_eq!(task.payload(), &json!({"value": "runtime"}));
        assert_eq!(task.format().task_version(), 1);
        assert_eq!(task.format().to_string(), "dummy_task v1");
        assert_eq!(task.max_attempts().get(), 10);
        assert_eq!(task.metadata(), &json!({}));
        assert_eq!(task.schedule(), &TaskSchedule::now());
        let id = task.id();
        let task = task
            .with_metadata(serde_json::from_value(json!({"trace": "test"})).unwrap())
            .with_max_attempts(NonZeroU16::new(2).unwrap())
            .with_schedule(TaskSchedule::after(Duration::ZERO).unwrap());
        assert_eq!(task.id(), id);
        assert_eq!(task.max_attempts().get(), 2);
        assert_eq!(task.metadata(), &json!({"trace": "test"}));
        assert_eq!(task.schedule().delay(), Some(Duration::ZERO));
        assert_eq!(TaskSchedule::after(Duration::MAX), Err(TaskValidationError::DelayTooLong));
        assert_eq!(TaskSchedule::now().delay(), None);
        assert_eq!(TaskSchedule::now().time(), None);
        let moment = Utc::now();
        assert_eq!(
            (TaskSchedule::at(moment).time(), TaskSchedule::at(moment).delay()),
            (Some(moment), None)
        );
        assert_eq!(TaskSchedule::default(), TaskSchedule::now());
        assert_eq!(TaskFormat::try_new("", 1), Err(TaskFormatError::EmptyType));
        assert_eq!(TaskFormat::try_new("valid", 0), Err(TaskFormatError::InvalidVersion));
        assert!(NewTask::from_payload(TaskFormat::try_new("valid", 1).unwrap(), json!([])).is_err());
        struct Broken;
        impl Serialize for Broken {
            fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("broken"))
            }
        }
        impl Task for Broken {
            const TYPE: &'static str = "broken";
            const VERSION: i32 = 1;
        }
        assert_eq!(NewTask::try_new(Broken), Err(TaskValidationError::Serialization));
    }

    #[test]
    fn a_task_without_a_budget_of_its_own_gets_the_default_of_ten_attempts() {
        assert_eq!(DEFAULT_MAX_ATTEMPTS.get(), 10);
        assert_eq!(
            NewTask::try_new(DummyTask { value: "runtime" }).unwrap().max_attempts(),
            DEFAULT_MAX_ATTEMPTS
        );
        let format = TaskFormat::try_new("dummy_task", 1).unwrap();
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

    impl Task for Labelled {
        const TYPE: &'static str = "labelled";
        const VERSION: i32 = 1;
        const LABELS: &'static [&'static str] = &["tenant", "absent"];
    }

    #[test]
    fn labels_move_from_the_body_to_the_metadata_and_an_unlabelled_task_is_as_before() {
        let labelled = Labelled {
            amount: 80,
            tenant: "acme",
        };
        let task = NewTask::try_new(labelled).unwrap();
        assert_eq!(task.payload(), &json!({ "amount": 80 }));
        assert_eq!(
            task.metadata(),
            &json!({ "tenant": "acme" }),
            "a label the body lacks moves nothing"
        );
        let unlabelled = NewTask::try_new(DummyTask { value: "runtime" }).unwrap();
        assert_eq!(
            (unlabelled.payload(), unlabelled.metadata()),
            (&json!({ "value": "runtime" }), &json!({}))
        );
        let attempt = TaskAttempt::try_first(
            &Labelled {
                amount: 80,
                tenant: "acme",
            },
            json!({ "correlation_id": "c-1" }),
        )
        .unwrap();
        assert_eq!(attempt.payload(), &json!({ "amount": 80 }));
        assert_eq!(attempt.metadata(), &json!({ "correlation_id": "c-1", "tenant": "acme" }));
        let first = TaskAttempt::try_first(&DummyTask { value: "runtime" }, json!({ "correlation_id": "c-1" })).unwrap();
        assert_eq!(
            (first.payload(), first.metadata()),
            (&json!({ "value": "runtime" }), &json!({ "correlation_id": "c-1" }))
        );
    }

    /// `with_metadata` replaced the metadata whole and lost the labels: the handler could not decode the task any more.
    #[test]
    fn metadata_of_the_caller_keeps_the_labels_and_a_label_keeps_its_value() {
        let labelled = NewTask::try_new(Labelled {
            amount: 80,
            tenant: "acme",
        })
        .unwrap();
        assert_eq!(
            labelled
                .clone()
                .with_metadata(serde_json::from_value(json!({ "trace": "t-1" })).unwrap())
                .metadata(),
            &json!({ "trace": "t-1", "tenant": "acme" })
        );
        let renamed = labelled
            .with_metadata(serde_json::from_value(json!({ "tenant": "other", "trace": "t-1" })).unwrap())
            .with_metadata(serde_json::from_value(json!({ "trace": "t-2" })).unwrap());
        assert_eq!(
            renamed.metadata(),
            &json!({ "trace": "t-2", "tenant": "acme" }),
            "the label keeps its value, the caller's keys are replaced"
        );
        assert_eq!(
            decode_with_labels::<Value>(Labelled::LABELS, renamed.payload(), renamed.metadata()).unwrap(),
            json!({ "amount": 80, "tenant": "acme" }),
            "the body decodes as it was"
        );
        let unlabelled = NewTask::try_new(DummyTask { value: "runtime" }).unwrap();
        assert_eq!(
            unlabelled
                .with_metadata(serde_json::from_value(json!({ "tenant": "t-1" })).unwrap())
                .metadata(),
            &json!({ "tenant": "t-1" }),
            "without labels the metadata is the caller's"
        );
    }

    #[test]
    fn labels_return_to_the_body_unless_it_has_them() {
        let metadata = json!({ "tenant": "acme", "correlation_id": "c-1" });
        assert_eq!(
            decode_with_labels::<Value>(Labelled::LABELS, &json!({ "amount": 80 }), &metadata).unwrap(),
            json!({ "amount": 80, "tenant": "acme" }),
            "only the labels come back, and a label the metadata lacks stays away"
        );
        assert_eq!(
            decode_with_labels::<Value>(Labelled::LABELS, &json!({ "amount": 80, "tenant": "body" }), &metadata).unwrap(),
            json!({ "amount": 80, "tenant": "body" })
        );
        #[derive(Debug, PartialEq, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Strict {
            amount: u32,
            tenant: String,
        }
        assert_eq!(
            decode_with_labels::<Strict>(Labelled::LABELS, &json!({ "amount": 80 }), &metadata).unwrap(),
            Strict {
                amount: 80,
                tenant: "acme".to_owned()
            },
            "a struct decodes from the body and the label alone: the other metadata stays away"
        );
        let missing = decode_with_labels::<Strict>(Labelled::LABELS, &json!({ "amount": 80 }), &json!({})).unwrap_err();
        assert!(missing.to_string().contains("missing field `tenant`"), "{missing}");
    }

    #[tokio::test]
    async fn labels_of_a_task_that_is_not_an_object_stay_put_and_the_bug_is_logged() {
        let ((payload, restored), log) = crate::diagnostics::test_support::capture(async {
            let mut payload = json!({ "tenant": "acme" });
            move_labels(Labelled::LABELS, &mut payload, &mut json!(["metadata"]));
            let restored: Value =
                decode_with_labels(Labelled::LABELS, &json!(["payload"]), &json!({ "tenant": "acme" })).unwrap();
            (payload, restored)
        })
        .await;
        assert_eq!((payload, restored), (json!({ "tenant": "acme" }), json!(["payload"])));
        assert_eq!(log.matches("an invariant of cqrsx is broken").count(), 2, "{log}");
    }

    #[test]
    fn with_id_changes_only_the_id() {
        let task = NewTask::try_new(DummyTask { value: "runtime" })
            .unwrap()
            .with_metadata(serde_json::from_value(json!({"trace": "test"})).unwrap())
            .with_max_attempts(NonZeroU16::new(3).unwrap())
            .with_schedule(TaskSchedule::after(Duration::from_secs(2)).unwrap());
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
}
