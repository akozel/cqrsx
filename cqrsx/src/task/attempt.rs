//! Immutable handler-visible input without queue ownership credentials.
use super::{move_labels, validate_metadata, validate_payload};
use crate::task::{
    TaskValidationError,
    format::{TaskFormat, TaskPayload},
};
use serde_json::Value;
use uuid::Uuid;
/// Immutable input for one claim, carrying the stable task ID and claim number.
/// Lease credentials remain platform-owned.
#[derive(Clone, Debug)]
pub struct TaskAttempt {
    task_id: Uuid,
    format: TaskFormat,
    payload: Value,
    metadata: Value,
    number: i32,
}
impl TaskAttempt {
    /// One claim of a stored task, as the queue leased it: the payload and the metadata are objects, the claim number is
    /// positive.
    pub fn try_new(
        task_id: Uuid,
        format: TaskFormat,
        payload: Value,
        metadata: Value,
        number: i32,
    ) -> Result<Self, TaskValidationError> {
        validate_payload(&payload)?;
        validate_metadata(&metadata)?;
        if number <= 0 {
            return Err(TaskValidationError::InvalidAttempt);
        }
        Ok(Self {
            task_id,
            format,
            payload,
            metadata,
            number,
        })
    }
    /// The first claim of a new task carrying `payload` and `metadata`, with a random task ID;
    /// for invoking task handlers outside the runtime, e.g. in their tests. The fields of `P::LABELS` move from the
    /// payload to the metadata, as in a published task.
    pub fn try_first<P: TaskPayload>(payload: &P, metadata: Value) -> Result<Self, TaskValidationError> {
        let format = TaskFormat::of::<P>();
        let payload = serde_json::to_value(payload).map_err(|_| TaskValidationError::Serialization)?;
        let mut attempt = Self::try_new(Uuid::new_v4(), format, payload, metadata, 1)?;
        if !P::LABELS.is_empty() {
            move_labels(P::LABELS, &mut attempt.payload, &mut attempt.metadata);
        }
        Ok(attempt)
    }

    /// Stable identity of the durable task across claims and redelivery.
    pub fn task_id(&self) -> Uuid {
        self.task_id
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
    /// Positive claim number, including claims whose handler was never invoked.
    pub fn number(&self) -> i32 {
        self.number
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn invalid_attempt_data_is_rejected() {
        let format = TaskFormat::try_new("test", 1).unwrap();
        let build = |payload, metadata, attempt| TaskAttempt::try_new(Uuid::new_v4(), format.clone(), payload, metadata, attempt);
        assert!(build(json!([]), json!({}), 1).is_err());
        assert!(build(json!({}), json!([]), 1).is_err());
        assert!(build(json!({}), json!({}), 0).is_err());
    }

    #[test]
    fn a_first_attempt_carries_the_payload_format_and_metadata() {
        #[derive(serde::Serialize)]
        struct Greeting<'a> {
            name: &'a str,
        }
        impl TaskPayload for Greeting<'_> {
            const TASK_TYPE: &'static str = "greeting";
            const TASK_VERSION: i32 = 2;
        }
        let attempt = TaskAttempt::try_first(&Greeting { name: "fox" }, json!({ "correlation_id": "c-1" })).unwrap();
        assert_eq!(attempt.format(), &TaskFormat::try_new("greeting", 2).unwrap());
        assert_eq!(attempt.payload(), &json!({ "name": "fox" }));
        assert_eq!(attempt.metadata(), &json!({ "correlation_id": "c-1" }));
        assert_eq!(attempt.number(), 1);
        let other = TaskAttempt::try_first(&Greeting { name: "fox" }, json!({})).unwrap();
        assert_ne!(attempt.task_id(), other.task_id());
        assert_eq!(
            TaskAttempt::try_first(&Greeting { name: "fox" }, json!([])).unwrap_err(),
            TaskValidationError::MetadataNotObject
        );
    }

    #[test]
    fn a_first_attempt_rejects_what_no_task_could_carry() {
        #[derive(serde::Serialize)]
        struct Text(&'static str);
        impl TaskPayload for Text {
            const TASK_TYPE: &'static str = "text";
            const TASK_VERSION: i32 = 1;
        }
        struct Broken;
        impl serde::Serialize for Broken {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("broken"))
            }
        }
        impl TaskPayload for Broken {
            const TASK_TYPE: &'static str = "broken";
            const TASK_VERSION: i32 = 1;
        }
        assert_eq!(
            TaskAttempt::try_first(&Text("a string, not an object"), json!({})).unwrap_err(),
            TaskValidationError::PayloadNotObject
        );
        assert_eq!(
            TaskAttempt::try_first(&Broken, json!({})).unwrap_err(),
            TaskValidationError::Serialization
        );
    }
}
