//! The answer to `POST /api/projections/rebuild`: Encore! started, or why it did not.
use crate::{
    operations::application::rebuild::{RebuildRefused, RebuildStarted},
    shared::presentation::http::error::ApiError,
};
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use uuid::Uuid;

/// `202 { outcome: "started", rebuild_id, pace_ms }`.
#[derive(Debug, Serialize)]
pub struct ProjectionRebuildResponse {
    outcome: &'static str,
    rebuild_id: Uuid,
    pace_ms: u64,
}

impl From<RebuildStarted> for ProjectionRebuildResponse {
    fn from(started: RebuildStarted) -> Self {
        Self {
            outcome: "started",
            rebuild_id: started.rebuild_id,
            pace_ms: u64::try_from(started.pace.as_millis()).unwrap_or(u64::MAX),
        }
    }
}

impl IntoResponse for ProjectionRebuildResponse {
    fn into_response(self) -> Response {
        (StatusCode::ACCEPTED, Json(self)).into_response()
    }
}

/// The answer to a refused rebuild request: `409` while one runs, `400` for a pace beyond the limit, `503` when the
/// read side cannot be asked.
impl From<RebuildRefused> for ApiError {
    fn from(refusal: RebuildRefused) -> Self {
        match refusal {
            RebuildRefused::AlreadyRunning => ApiError::new(
                StatusCode::CONFLICT,
                "conflict",
                "rebuild_in_progress",
                "a rebuild is already running",
            ),
            RebuildRefused::PaceTooLong { .. } => ApiError::invalid(refusal.to_string()),
            RebuildRefused::Unavailable(message) => ApiError::unavailable(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::time::Duration;

    async fn answer(response: Response) -> (StatusCode, Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn a_started_rebuild_is_accepted_with_its_id_and_pace() {
        let rebuild_id = Uuid::now_v7();
        let started = RebuildStarted {
            rebuild_id,
            pace: Duration::from_millis(80),
        };
        assert_eq!(
            answer(ProjectionRebuildResponse::from(started).into_response()).await,
            (
                StatusCode::ACCEPTED,
                json!({ "outcome": "started", "rebuild_id": rebuild_id, "pace_ms": 80 })
            )
        );
    }

    #[tokio::test]
    async fn every_refusal_has_its_status_and_code() {
        for (refusal, status, code, message) in [
            (
                RebuildRefused::AlreadyRunning,
                StatusCode::CONFLICT,
                "rebuild_in_progress",
                "a rebuild is already running",
            ),
            (
                RebuildRefused::PaceTooLong { max_ms: 1000 },
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "the pace must be at most 1000 ms",
            ),
            (
                RebuildRefused::Unavailable("down".into()),
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "down",
            ),
        ] {
            let (got, body) = answer(ApiError::from(refusal).into_response()).await;
            assert_eq!(got, status);
            assert_eq!(
                (&body["error"]["code"], &body["error"]["message"]),
                (&json!(code), &json!(message))
            );
        }
    }
}
