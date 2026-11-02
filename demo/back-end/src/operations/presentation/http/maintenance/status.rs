//! `GET /api/projections/status` — every projection: its checkpoint, lag, owner and state, plus the Encore!
//! this node started and the last one it saw finish.
use crate::{
    operations::application::rebuild::{
        self, ProjectionRow, ProjectionState, ProjectionStatus, ReadSideService, ReadSideStatus, RebuildResult,
    },
    shared::presentation::http::error::ApiError,
};
use axum::{Json, extract::State};
use chrono::{DateTime, SecondsFormat, Utc};
use cqrsx_engine::Cqrs;
use serde::Serialize;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

/// `{ state, from_cache, projections, running, last_result }`: `state` is the worst of the projections, `from_cache`
/// says the checkpoint store did not answer in time and the rows are the ones seen last, `running` and `last_result`
/// are the Encore! this node started and the last one it saw end. A projection that is still `starting` has no row:
/// only its name, aggregate type and state.
#[derive(Debug, Serialize)]
pub struct ProjectionStatusResponse {
    state: &'static str,
    from_cache: bool,
    projections: Vec<Projection>,
    running: Option<Running>,
    last_result: Option<LastResult>,
}

#[derive(Debug, Serialize)]
struct Projection {
    projection: String,
    aggregate_type: String,
    state: &'static str,
    #[serde(flatten)]
    row: Option<Row>,
}

#[derive(Debug, Serialize)]
struct Row {
    position: String,
    delivered: u64,
    lag: u64,
    lag_capped: bool,
    owner: Option<String>,
    lease_until: Option<String>,
    failures: u32,
    last_error: Option<String>,
    rebuild: Rebuild,
}

#[derive(Debug, Serialize)]
struct Rebuild {
    requested: bool,
    cursor: Option<String>,
    rebuilt: u64,
    pace_ms: u64,
}

#[derive(Debug, Serialize)]
struct Running {
    rebuild_id: Uuid,
    pace_ms: u64,
    started_at: String,
    keys_total: u64,
}

#[derive(Debug, Serialize)]
struct LastResult {
    rebuild_id: Uuid,
    keys: u64,
    duration_ms: u64,
    finished_at: String,
}

fn state_name(state: ProjectionState) -> &'static str {
    match state {
        ProjectionState::Stalled => "stalled",
        ProjectionState::Unclaimed => "unclaimed",
        ProjectionState::Starting => "starting",
        ProjectionState::Rebuilding => "rebuilding",
        ProjectionState::Live => "live",
    }
}

fn millis(pace: Duration) -> u64 {
    u64::try_from(pace.as_millis()).unwrap_or(u64::MAX)
}

fn instant(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

impl From<ProjectionRow> for Row {
    fn from(row: ProjectionRow) -> Self {
        Self {
            position: row.position,
            delivered: row.delivered,
            lag: row.lag,
            lag_capped: row.lag_capped,
            owner: row.owner,
            lease_until: row.lease_until.map(instant),
            failures: row.failures,
            last_error: row.last_error,
            rebuild: Rebuild {
                requested: row.rebuild.requested,
                cursor: row.rebuild.cursor,
                rebuilt: row.rebuild.rebuilt,
                pace_ms: millis(row.rebuild.pace),
            },
        }
    }
}

impl From<ProjectionStatus> for Projection {
    fn from(status: ProjectionStatus) -> Self {
        Self {
            projection: status.projection,
            aggregate_type: status.aggregate_type,
            state: state_name(status.state),
            row: status.row.map(Row::from),
        }
    }
}

impl From<rebuild::Running> for Running {
    fn from(running: rebuild::Running) -> Self {
        Self {
            rebuild_id: running.rebuild_id,
            pace_ms: millis(running.pace),
            started_at: running.started_at,
            keys_total: running.keys_total,
        }
    }
}

impl From<RebuildResult> for LastResult {
    fn from(result: RebuildResult) -> Self {
        Self {
            rebuild_id: result.rebuild_id,
            keys: result.keys,
            duration_ms: result.duration_ms,
            finished_at: result.finished_at,
        }
    }
}

impl From<ReadSideStatus> for ProjectionStatusResponse {
    fn from(status: ReadSideStatus) -> Self {
        Self {
            state: state_name(status.state),
            from_cache: status.from_cache,
            projections: status.projections.into_iter().map(Projection::from).collect(),
            running: status.running.map(Running::from),
            last_result: status.last_result.map(LastResult::from),
        }
    }
}

pub async fn handle(State(cqrs): State<Arc<Cqrs>>) -> Result<Json<ProjectionStatusResponse>, ApiError> {
    Ok(Json(ReadSideService::of(&cqrs)?.status().await?.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::application::rebuild::RowRebuild;
    use chrono::TimeZone;
    use serde_json::json;

    fn row(owner: Option<&str>) -> ProjectionRow {
        ProjectionRow {
            position: "7".into(),
            delivered: 4,
            lag: 3,
            lag_capped: false,
            owner: owner.map(str::to_owned),
            lease_until: owner.map(|_| Utc.with_ymd_and_hms(2026, 11, 1, 10, 30, 5).unwrap()),
            failures: 1,
            last_error: Some("boom".into()),
            rebuild: RowRebuild {
                requested: true,
                cursor: Some("c-1".into()),
                rebuilt: 2,
                pace: Duration::from_millis(80),
            },
        }
    }

    #[test]
    fn a_status_with_rows_and_both_rebuilds_has_every_field() {
        let (running_id, finished_id) = (Uuid::now_v7(), Uuid::now_v7());
        let status = ReadSideStatus {
            state: ProjectionState::Stalled,
            from_cache: true,
            projections: vec![
                ProjectionStatus {
                    projection: "concert_halls".into(),
                    aggregate_type: "Concert".into(),
                    state: ProjectionState::Stalled,
                    row: Some(row(Some("node-a"))),
                },
                ProjectionStatus {
                    projection: "fan_wallets".into(),
                    aggregate_type: "Fan".into(),
                    state: ProjectionState::Starting,
                    row: None,
                },
            ],
            running: Some(rebuild::Running {
                rebuild_id: running_id,
                pace: Duration::from_millis(80),
                started_at: "2026-11-01T10:30:00.000Z".into(),
                keys_total: 5,
            }),
            last_result: Some(RebuildResult {
                rebuild_id: finished_id,
                keys: 3,
                duration_ms: 1200,
                finished_at: "2026-11-01T10:20:00.000Z".into(),
            }),
        };
        assert_eq!(
            serde_json::to_value(ProjectionStatusResponse::from(status)).unwrap(),
            json!({
                "state": "stalled",
                "from_cache": true,
                "projections": [
                    { "projection": "concert_halls", "aggregate_type": "Concert", "state": "stalled", "position": "7",
                      "delivered": 4, "lag": 3, "lag_capped": false, "owner": "node-a",
                      "lease_until": "2026-11-01T10:30:05.000Z", "failures": 1, "last_error": "boom",
                      "rebuild": { "requested": true, "cursor": "c-1", "rebuilt": 2, "pace_ms": 80 } },
                    { "projection": "fan_wallets", "aggregate_type": "Fan", "state": "starting" },
                ],
                "running": { "rebuild_id": running_id, "pace_ms": 80, "started_at": "2026-11-01T10:30:00.000Z", "keys_total": 5 },
                "last_result": { "rebuild_id": finished_id, "keys": 3, "duration_ms": 1200,
                                 "finished_at": "2026-11-01T10:20:00.000Z" },
            })
        );
    }

    #[test]
    fn a_row_without_an_owner_has_null_owner_and_lease() {
        let status = ReadSideStatus {
            state: ProjectionState::Unclaimed,
            from_cache: false,
            projections: vec![ProjectionStatus {
                projection: "concert_halls".into(),
                aggregate_type: "Concert".into(),
                state: ProjectionState::Unclaimed,
                row: Some(row(None)),
            }],
            running: None,
            last_result: None,
        };
        let body = serde_json::to_value(ProjectionStatusResponse::from(status)).unwrap();
        assert_eq!((&body["running"], &body["last_result"]), (&json!(null), &json!(null)));
        assert_eq!(
            (&body["projections"][0]["owner"], &body["projections"][0]["lease_until"]),
            (&json!(null), &json!(null))
        );
    }

    #[test]
    fn every_state_has_its_name() {
        let names = [
            ProjectionState::Stalled,
            ProjectionState::Unclaimed,
            ProjectionState::Starting,
            ProjectionState::Rebuilding,
            ProjectionState::Live,
        ]
        .map(state_name);
        assert_eq!(names, ["stalled", "unclaimed", "starting", "rebuilding", "live"]);
    }
}
