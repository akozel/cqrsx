//! `GET /api/projections/verify` \[P1\] — every document against the history of its aggregate (D-20):
//! documents behind the journal are counted apart, only a wrong one is a difference. Allowed at any time,
//! during Encore! too.
use crate::{
    operations::application::verify::{VerifyReport, VerifyService},
    shared::presentation::http::error::ApiError,
};
use axum::{Json, extract::State};
use cqrsx::view::verify::Difference;
use cqrsx_framework::Cqrs;
use serde::Serialize;
use std::{collections::BTreeMap, sync::Arc};

/// `{ ok, checked: { <read model>: keys }, behind: { documents, events }, differences }`: `ok` is true when no
/// document is wrong, however far behind the journal the documents are.
#[derive(Debug, Serialize)]
pub struct ProjectionVerifyResponse {
    ok: bool,
    checked: BTreeMap<String, usize>,
    behind: Behind,
    differences: Vec<Difference>,
}

#[derive(Debug, Serialize)]
struct Behind {
    documents: usize,
    events: usize,
}

impl From<VerifyReport> for ProjectionVerifyResponse {
    fn from(report: VerifyReport) -> Self {
        Self {
            ok: report.differences.is_empty(),
            checked: report.checked,
            behind: Behind {
                documents: report.behind_documents,
                events: report.behind_events,
            },
            differences: report.differences,
        }
    }
}

pub async fn handle(State(cqrs): State<Arc<Cqrs>>) -> Result<Json<ProjectionVerifyResponse>, ApiError> {
    Ok(Json(VerifyService::of(&cqrs).run().await?.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqrsx::view::verify::DifferenceKind;
    use serde_json::json;

    fn report(differences: Vec<Difference>) -> VerifyReport {
        VerifyReport {
            checked: BTreeMap::from([("concert_halls".to_owned(), 3), ("fan_wallets".to_owned(), 5)]),
            behind_documents: 3,
            behind_events: 4,
            differences,
        }
    }

    #[test]
    fn a_report_without_differences_is_ok_however_far_behind_it_is() {
        assert_eq!(
            serde_json::to_value(ProjectionVerifyResponse::from(report(vec![]))).unwrap(),
            json!({ "ok": true, "checked": { "concert_halls": 3, "fan_wallets": 5 },
                    "behind": { "documents": 3, "events": 4 }, "differences": [] })
        );
    }

    #[test]
    fn a_difference_fails_the_verification_and_is_listed() {
        let difference = |id: &str, kind| Difference {
            view: "concert_halls",
            id: id.into(),
            kind,
        };
        let body = serde_json::to_value(ProjectionVerifyResponse::from(report(vec![
            difference("c-1", DifferenceKind::Differs),
            difference("c-9", DifferenceKind::WithoutHistory),
        ])))
        .unwrap();
        assert_eq!(body["ok"], false);
        assert_eq!(
            body["differences"],
            json!([{ "view": "concert_halls", "id": "c-1", "kind": "differs" },
                   { "view": "concert_halls", "id": "c-9", "kind": "without_history" }])
        );
    }
}
