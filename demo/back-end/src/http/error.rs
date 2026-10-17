//! Error responses share one shape: `{ outcome, error: { code, params?, message } }`.
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
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

    pub fn locked() -> Self {
        Self::new(
            StatusCode::LOCKED,
            "locked",
            "rebuild_in_progress",
            "box office closed for a moment: rebuilding read models",
        )
    }

    pub fn with_params(mut self, params: Value) -> Self {
        self.params = params;
        self
    }

    pub fn with_extra(mut self, extra: Value) -> Self {
        self.extra = extra;
        self
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
