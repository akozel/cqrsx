//! Immutable handler-visible input without queue ownership credentials.
use super::{validate_metadata, validate_payload};
use crate::task::{TaskValidationError, format::TaskFormat};
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
    pub fn new(
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
        let format = TaskFormat::new("test", 1).unwrap();
        let build = |payload, metadata, attempt| TaskAttempt::new(Uuid::new_v4(), format.clone(), payload, metadata, attempt);
        assert!(build(json!([]), json!({}), 1).is_err());
        assert!(build(json!({}), json!([]), 1).is_err());
        assert!(build(json!({}), json!({}), 0).is_err());
    }
}
