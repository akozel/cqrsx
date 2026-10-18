//! DEMO ONLY — the external mail service, hosted by the sidecar so that the back-end stays a plain
//! client of a mail API (`infrastructure::mail::http_mailer` over there). A moody provider:
//! `POST /mail` waits a random "network" latency, then rolls the dice — with
//! `SIDECAR_MAIL_FAILURE_RATE` it answers `503 { reason_code }` (a funny one), otherwise it stores
//! the e-mail in `mail_service.inbox` with an idempotent upsert keyed by the `Idempotency-Key`
//! header (the e-mail's business id) and answers `200 { inbox_id, duplicate }`. The back-end
//! delivers at-least-once, so a repeat must not create a second e-mail: it only bumps `deliveries`.
//! A malformed request is `400`, never a retry.
pub mod chaos;
pub mod inbox;

use crate::mail::chaos::Random;
use axum::{
    Json, Router,
    extract::{State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use chrono::{SecondsFormat, Utc};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Funny refusal reasons (`reason_code`); texts live in the UI i18n.
pub const REASONS: [&str; 4] = ["smtp_timeout", "server_on_fire", "rate_limited", "mailbox_full"];
/// The request header that carries the e-mail's business id; the body's `id` is the fallback.
pub const IDEMPOTENCY_KEY: &str = "Idempotency-Key";

#[derive(Clone, Debug)]
pub struct MailSettings {
    /// Probability that a delivery is refused.
    pub failure_rate: f64,
    /// Simulated network latency, `(min, max)`.
    pub latency: (Duration, Duration),
    /// An explicit seed makes the dice deterministic; `None` is random.
    pub seed: Option<u64>,
}

/// The e-mail as the back-end posts it. `task_id` and `attempt` are the client's references; they
/// are stored with the e-mail (and make at-least-once delivery visible in the X-ray).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MailRequest {
    pub id: String,
    pub kind: String,
    pub to_fan_id: String,
    pub to_name: String,
    pub subject: String,
    pub concert_id: String,
    pub seat: String,
    pub amount: i64,
    pub task_id: String,
    pub attempt: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Delivery {
    pub inbox_id: String,
    /// `true` when the e-mail already existed: the repeat only counted another delivery.
    pub duplicate: bool,
}

/// The storage of the mail service: `mail_service.inbox` at runtime, memory in tests.
pub trait MailStore: Send + Sync + 'static {
    /// Upserts the e-mail under `key`; a repeat only counts another delivery.
    fn deliver(&self, key: &str, mail: &MailRequest, now: &str) -> impl Future<Output = Result<Delivery, String>> + Send;
}

/// Why a delivery was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// DEMO ONLY: the dice said no.
    Chaos(&'static str),
    /// The inbox could not be written.
    Storage(String),
}

struct Dice {
    chaos: Random,
    latency: StdRng,
}

pub struct MailService<S> {
    store: S,
    settings: MailSettings,
    dice: Mutex<Dice>,
}

impl<S: MailStore> MailService<S> {
    pub fn new(store: S, settings: MailSettings) -> Self {
        let seed = settings.seed.unwrap_or_else(|| rand::rng().random());
        Self {
            store,
            dice: Mutex::new(Dice {
                chaos: Random::new(settings.failure_rate, Some(seed)),
                latency: StdRng::seed_from_u64(seed.wrapping_mul(7919)),
            }),
            settings,
        }
    }

    /// One roll: how long the "network" takes and whether this delivery is refused (and why).
    fn roll(&self) -> (Duration, Option<&'static str>) {
        let mut dice = self.dice.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let (min, max) = self.settings.latency;
        let latency = if max > min {
            dice.latency.random_range(min..=max)
        } else {
            min
        };
        let refused = dice
            .chaos
            .should_fail()
            .then(|| REASONS[dice.chaos.pick_reason(REASONS.len()).min(REASONS.len() - 1)]);
        (latency, refused)
    }

    /// The whole behaviour of one `POST /mail`, without the HTTP around it.
    pub async fn send(&self, key: &str, mail: &MailRequest) -> Result<Delivery, Refusal> {
        let (latency, refused) = self.roll();
        tokio::time::sleep(latency).await;
        if let Some(reason) = refused {
            return Err(Refusal::Chaos(reason));
        }
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        self.store.deliver(key, mail, &now).await.map_err(Refusal::Storage)
    }
}

/// `POST /mail`: the provider's one endpoint.
pub fn router<S: MailStore>(service: Arc<MailService<S>>) -> Router {
    Router::new().route("/mail", post(send::<S>)).with_state(service)
}

async fn send<S: MailStore>(
    State(service): State<Arc<MailService<S>>>,
    headers: HeaderMap,
    body: Result<Json<MailRequest>, JsonRejection>,
) -> Response {
    let Json(mail) = match body {
        Ok(body) => body,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": { "source": "mail", "message": format!("invalid e-mail: {error}") } })),
            )
                .into_response();
        }
    };
    let key = headers
        .get(IDEMPOTENCY_KEY)
        .and_then(|value| value.to_str().ok())
        .filter(|key| !key.is_empty())
        .unwrap_or(&mail.id)
        .to_owned();
    match service.send(&key, &mail).await {
        Ok(delivery) => (StatusCode::OK, Json(json!(delivery))).into_response(),
        Err(Refusal::Chaos(reason)) => (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "reason_code": reason }))).into_response(),
        Err(Refusal::Storage(message)) => {
            tracing::warn!("mail inbox unavailable: {message}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "reason_code": "inbox_unavailable", "message": message })),
            )
                .into_response()
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, header},
    };
    use http_body_util::BodyExt;
    use std::collections::BTreeMap;
    use tower::ServiceExt;

    /// The inbox in memory, with the same upsert semantics as MongoDB.
    #[derive(Default)]
    pub(crate) struct MemoryStore {
        /// key → (the first delivery, how many deliveries).
        pub(crate) mails: Mutex<BTreeMap<String, (MailRequest, u32)>>,
    }

    impl MailStore for MemoryStore {
        async fn deliver(&self, key: &str, mail: &MailRequest, _: &str) -> Result<Delivery, String> {
            let mut mails = self.mails.lock().map_err(|e| e.to_string())?;
            let duplicate = match mails.get_mut(key) {
                Some(entry) => {
                    entry.1 += 1;
                    true
                }
                None => {
                    mails.insert(key.to_owned(), (mail.clone(), 1));
                    false
                }
            };
            Ok(Delivery {
                inbox_id: key.to_owned(),
                duplicate,
            })
        }
    }

    struct BrokenStore;

    impl MailStore for BrokenStore {
        async fn deliver(&self, _: &str, _: &MailRequest, _: &str) -> Result<Delivery, String> {
            Err("inbox write failed: boom".into())
        }
    }

    fn settings(failure_rate: f64) -> MailSettings {
        MailSettings {
            failure_rate,
            latency: (Duration::ZERO, Duration::ZERO),
            seed: Some(42),
        }
    }

    pub(crate) fn mail(attempt: i32) -> MailRequest {
        MailRequest {
            id: "e_ticket:b-1".into(),
            kind: "e_ticket".into(),
            to_fan_id: "fox".into(),
            to_name: "Fox".into(),
            subject: "🎫".into(),
            concert_id: "radiohead-2026-11-21".into(),
            seat: "B4".into(),
            amount: 80,
            task_id: "t-1".into(),
            attempt,
        }
    }

    async fn post(router: &Router, key: Option<&str>, body: String) -> (StatusCode, serde_json::Value) {
        let mut request = Request::post("/mail").header(header::CONTENT_TYPE, "application/json");
        if let Some(key) = key {
            request = request.header(IDEMPOTENCY_KEY, key);
        }
        let response = router.clone().oneshot(request.body(Body::from(body)).unwrap()).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn a_reliable_service_stores_once_and_counts_repeats_as_duplicates() {
        let service = MailService::new(MemoryStore::default(), settings(0.0));
        let first = service.send("e_ticket:b-1", &mail(2)).await.unwrap();
        let again = service.send("e_ticket:b-1", &mail(3)).await.unwrap();
        assert_eq!((first.duplicate, again.duplicate), (false, true));
        assert_eq!(first.inbox_id, "e_ticket:b-1");
        let mails = service.store.mails.lock().unwrap();
        let (stored, deliveries) = &mails["e_ticket:b-1"];
        assert_eq!(
            (stored.attempt, *deliveries),
            (2, 2),
            "the first delivery is kept, both are counted"
        );
    }

    #[tokio::test]
    async fn a_hopeless_service_refuses_with_a_known_reason_and_writes_nothing() {
        let service = MailService::new(MemoryStore::default(), settings(1.0));
        for _ in 0..5 {
            match service.send("e_ticket:b-1", &mail(1)).await {
                Err(Refusal::Chaos(reason)) => assert!(REASONS.contains(&reason)),
                other => panic!("expected a refusal, got {other:?}"),
            }
        }
        assert!(service.store.mails.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_unavailable_inbox_is_a_refusal_too() {
        let service = MailService::new(BrokenStore, settings(0.0));
        assert_eq!(
            service.send("e_ticket:b-1", &mail(1)).await,
            Err(Refusal::Storage("inbox write failed: boom".into()))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_network_latency_is_waited_for() {
        let service = MailService::new(
            MemoryStore::default(),
            MailSettings {
                failure_rate: 0.0,
                latency: (Duration::from_millis(600), Duration::from_millis(1800)),
                seed: Some(7),
            },
        );
        let started = tokio::time::Instant::now();
        service.send("e_ticket:b-1", &mail(1)).await.unwrap();
        let waited = started.elapsed();
        assert!(
            (Duration::from_millis(600)..=Duration::from_millis(1800)).contains(&waited),
            "{waited:?}"
        );
    }

    #[tokio::test]
    async fn the_http_api_answers_200_503_and_400() {
        let reliable = router(Arc::new(MailService::new(MemoryStore::default(), settings(0.0))));
        let body = serde_json::to_string(&mail(1)).unwrap();
        let (status, answer) = post(&reliable, Some("e_ticket:b-1"), body.clone()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(answer, json!({ "inbox_id": "e_ticket:b-1", "duplicate": false }));
        let (status, answer) = post(&reliable, None, body.clone()).await;
        assert_eq!(
            (status, answer["duplicate"].as_bool()),
            (StatusCode::OK, Some(true)),
            "the body id is the fallback key"
        );
        let (status, answer) = post(&reliable, Some("refund:c-1"), body.clone()).await;
        assert_eq!(
            (status, answer["inbox_id"].as_str()),
            (StatusCode::OK, Some("refund:c-1")),
            "the header wins"
        );
        let (status, answer) = post(&reliable, None, "{not json".into()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(answer["error"]["message"].as_str().unwrap().starts_with("invalid e-mail"));

        let hopeless = router(Arc::new(MailService::new(MemoryStore::default(), settings(1.0))));
        let (status, answer) = post(&hopeless, Some("e_ticket:b-1"), body.clone()).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(REASONS.contains(&answer["reason_code"].as_str().unwrap()));

        let broken = router(Arc::new(MailService::new(BrokenStore, settings(0.0))));
        let (status, answer) = post(&broken, Some("e_ticket:b-1"), body).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(answer["reason_code"], "inbox_unavailable");
    }
}
