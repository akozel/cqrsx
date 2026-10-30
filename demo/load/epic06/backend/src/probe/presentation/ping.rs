//! `POST /api/probe/{probe_id}` — one ping from the load stand: no body, the probe in the path.
use crate::probe::domain::{InvalidProbeId, Ping, Probe, ProbeError, ProbeId};
use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use cqrsx::{cqrs::command::CommandGatewayError, engine::Cqrs};
use encore_backend::shared::presentation::error::ApiError;
use serde_json::json;
use std::sync::Arc;

/// The ping goes to the engine from the source `load`.
pub async fn handle(State(cqrs): State<Arc<Cqrs>>, Path(probe_id): Path<String>) -> Result<Response, ApiError> {
    let probe_id: ProbeId = probe_id
        .parse()
        .map_err(|error: InvalidProbeId| ApiError::invalid(error.to_string()))?;
    let result = cqrs.from("load").execute::<Probe>(&probe_id, Ping).await;
    Ok(answer(result.map(|report| report.tasks.len()), &probe_id))
}

/// `200` with the number of tasks the commit published, `409` after a conflict, `503` when the ping did not run; a
/// probe refuses nothing.
fn answer(result: Result<usize, CommandGatewayError<ProbeError>>, probe_id: &ProbeId) -> Response {
    match result {
        Ok(tasks) => (
            StatusCode::OK,
            Json(json!({ "outcome": "accepted", "probe_id": probe_id.as_str(), "tasks": tasks })),
        )
            .into_response(),
        Err(CommandGatewayError::Domain(refused)) => match refused {},
        Err(CommandGatewayError::Conflict) => ApiError::new(
            StatusCode::CONFLICT,
            "conflict",
            "conflict",
            "the probe changed concurrently; retries exhausted",
        )
        .into_response(),
        Err(CommandGatewayError::Unavailable) => {
            ApiError::unavailable("the ping did not run: the event store is unavailable or the probe is busy; retry")
                .into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    /// The status and the body of an answer.
    async fn body(response: Response) -> (StatusCode, Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn every_outcome_answers_with_its_status() {
        let probe_id: ProbeId = "p-1".parse().unwrap();
        assert_eq!(
            body(answer(Ok(2), &probe_id)).await,
            (
                StatusCode::OK,
                json!({ "outcome": "accepted", "probe_id": "p-1", "tasks": 2 })
            )
        );
        let (conflict, _) = body(answer(Err(CommandGatewayError::Conflict), &probe_id)).await;
        let (unavailable, _) = body(answer(Err(CommandGatewayError::Unavailable), &probe_id)).await;
        assert_eq!(
            (conflict, unavailable),
            (StatusCode::CONFLICT, StatusCode::SERVICE_UNAVAILABLE)
        );
    }
}
