//! Error responses share one shape: `{ outcome, error: { code, params?, message } }`.
use crate::shared::application::StorageError;
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub outcome: &'static str,
    pub code: String,
    pub message: String,
    pub params: Value,
    pub extra: Value,
}

impl ApiError {
    pub fn new(status: StatusCode, outcome: &'static str, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status,
            outcome,
            code: code.into(),
            message: message.into(),
            params: json!({}),
            extra: json!({}),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid", "invalid_request", message)
    }

    pub fn not_found(code: &str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", code, message)
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable", "unavailable", message)
    }

    /// The read-model storage failed; the prefix tells it apart from the event store.
    pub fn read_model_unavailable(error: impl std::fmt::Display) -> Self {
        Self::unavailable(format!("read model storage unavailable: {error}"))
    }

    pub fn with_params(mut self, params: Value) -> Self {
        self.params = params;
        self
    }

    /// Fields next to `error` in the body: `extra` serialized as a JSON object.
    pub fn with_extra<T: Serialize>(mut self, extra: &T) -> Self {
        self.extra = serde_json::to_value(extra).unwrap_or_else(|error| {
            tracing::error!(%error, "an error answer's extra fields do not serialize; they are left out");
            Value::Null
        });
        self
    }
}

impl From<StorageError> for ApiError {
    fn from(error: StorageError) -> Self {
        ApiError::unavailable(error.0)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = json!({
            "outcome": self.outcome,
            "error": { "code": self.code, "params": self.params, "message": self.message },
        });
        if let (Value::Object(body), Value::Object(extra)) = (&mut body, self.extra) {
            body.extend(extra);
        }
        (self.status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    async fn body(error: ApiError) -> (StatusCode, Value) {
        let response = error.into_response();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[derive(Serialize)]
    struct Ids {
        command_id: u8,
    }

    #[tokio::test]
    async fn extra_fields_sit_next_to_the_error() {
        let (status, answer) = body(ApiError::invalid("no").with_extra(&Ids { command_id: 7 })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            answer,
            json!({ "outcome": "invalid", "command_id": 7,
                    "error": { "code": "invalid_request", "params": {}, "message": "no" } })
        );
    }

    #[tokio::test]
    async fn extra_fields_that_do_not_serialize_are_left_out() {
        // A JSON object has string keys only.
        let extra = HashMap::from([((1, 2), 3)]);
        let (_, answer) = body(ApiError::invalid("no").with_extra(&extra)).await;
        assert_eq!(
            answer,
            json!({ "outcome": "invalid", "error": { "code": "invalid_request", "params": {}, "message": "no" } })
        );
    }
}
