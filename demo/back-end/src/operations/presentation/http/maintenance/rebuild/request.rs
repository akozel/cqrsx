//! The body of `POST /api/projections/rebuild`: how slowly Encore! may go.
use crate::shared::presentation::http::error::ApiError;
use axum::{Json, extract::rejection::JsonRejection};
use serde::Deserialize;
use std::time::Duration;

/// `{ "pace_ms": 80 }` or `{}`; no body at all is `{}`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionRebuildRequest {
    /// The pause between the events of one key, in milliseconds (D-18): the UI sends the slow-motion delay or 0.
    #[serde(default)]
    pace_ms: u64,
}

impl ProjectionRebuildRequest {
    /// The request in a body: no body (no content type) means full speed; anything else must parse, or `400`.
    pub fn from_body(body: Result<Json<Self>, JsonRejection>) -> Result<Self, ApiError> {
        match body {
            Ok(Json(request)) => Ok(request),
            Err(JsonRejection::MissingJsonContentType(_)) => Ok(Self::default()),
            Err(error) => Err(ApiError::invalid(format!("invalid rebuild request: {error}"))),
        }
    }

    /// The pause between the events of one key.
    pub fn pace(&self) -> Duration {
        Duration::from_millis(self.pace_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        extract::FromRequest,
        http::{Request, header},
    };

    /// The request of a body as the router reads it.
    async fn parse(content_type: Option<&str>, body: &str) -> Result<ProjectionRebuildRequest, ApiError> {
        let mut request = Request::post("/");
        if let Some(content_type) = content_type {
            request = request.header(header::CONTENT_TYPE, content_type);
        }
        let body = Json::<ProjectionRebuildRequest>::from_request(request.body(Body::from(body.to_owned())).unwrap(), &()).await;
        ProjectionRebuildRequest::from_body(body)
    }

    #[tokio::test]
    async fn a_missing_body_is_full_speed_and_a_pace_is_milliseconds() {
        assert_eq!(parse(None, "").await.unwrap().pace(), Duration::ZERO);
        assert_eq!(parse(Some("application/json"), "{}").await.unwrap().pace(), Duration::ZERO);
        assert_eq!(
            parse(Some("application/json"), r#"{ "pace_ms": 80 }"#).await.unwrap().pace(),
            Duration::from_millis(80)
        );
    }

    #[tokio::test]
    async fn anything_else_is_a_bad_request() {
        for body in [
            r#"{ "speed": "slow" }"#,
            r#"{ "pace_ms": "slow" }"#,
            r#"{ "pace_ms": -1 }"#,
            "{not json",
        ] {
            let error = parse(Some("application/json"), body).await.unwrap_err();
            assert_eq!(error.status, axum::http::StatusCode::BAD_REQUEST, "{body}");
            assert!(
                error.message.starts_with("invalid rebuild request"),
                "{body}: {}",
                error.message
            );
        }
    }
}
