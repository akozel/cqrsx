//! `HttpMailer`: the `Mailer` port over the mail service's HTTP API — the whole mail code of the
//! box office.
//!
//! One attempt is one `POST {base_url}/mail` with the e-mail as JSON and its business id as
//! `Idempotency-Key`. Delivery is at-least-once (a lease can expire, the process can crash after
//! the effect and before settlement), so the provider deduplicates by that key and answers
//! `duplicate: true` on a repeat. The answer decides the task outcome:
//!
//! | Answer | Outcome |
//! |---|---|
//! | `2xx { inbox_id, duplicate }` | done |
//! | `503 { reason_code }` | retry after the pause of the task type's policy (`MAIL`: 1, 2, 4, 8, 8 s by default) |
//! | `429`, other `5xx` | retry, reason `mail_unavailable` |
//! | timeout, connection refused | retry, reason `mail_unreachable` |
//! | other `4xx` | permanent failure: the request itself is wrong |
//!
//! Every attempt is reported to the activity log (`task.attempt.started` / `succeeded` / `failed`); a retry with the
//! pause the task runtime applies to it (`ctx.retry_pause()`).
use crate::shared::application::{
    activity::ActivityHub,
    mail::{Mail, MailService},
};
use async_trait::async_trait;
use cqrsx::{
    command::causation::CORRELATION_ID,
    task::{attempt::TaskAttempt, context::TaskContext, result::TaskResult},
};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub struct MailClientSettings {
    /// `http://host:port`, without a trailing slash.
    pub base_url: String,
    /// Per-request timeout; must end before the task runtime's handler timeout.
    pub timeout: Duration,
}

impl MailClientSettings {
    /// The timeout of a request under a handler that may run `handler_timeout`: a second less, at least a second.
    pub fn request_timeout(handler_timeout: Duration) -> Duration {
        handler_timeout
            .saturating_sub(Duration::from_secs(1))
            .max(Duration::from_secs(1))
    }
}

/// `POST /mail` body. `task_id` and `attempt` are client references the provider stores with the
/// e-mail (they make at-least-once delivery visible in the demo's X-ray).
#[derive(Debug, Serialize)]
struct MailRequest<'a> {
    id: &'a str,
    kind: &'a str,
    to_fan_id: &'a str,
    to_name: &'a str,
    subject: &'a str,
    concert_id: &'a str,
    seat: &'a str,
    amount: u32,
    task_id: Uuid,
    attempt: i32,
}

#[derive(Debug, Deserialize)]
struct Delivered {
    inbox_id: String,
    #[serde(default)]
    duplicate: bool,
}

#[derive(Debug, Deserialize)]
struct Refused {
    reason_code: String,
}

enum Failure {
    Retry(String),
    Permanent(String),
}

pub struct HttpMailer {
    client: reqwest::Client,
    settings: MailClientSettings,
}

impl HttpMailer {
    /// The HTTP client every mailer shares (one connection pool); the timeout is the settings'.
    pub fn client(settings: &MailClientSettings) -> Result<reqwest::Client, reqwest::Error> {
        reqwest::Client::builder().timeout(settings.timeout).build()
    }

    pub fn new(client: reqwest::Client, settings: MailClientSettings) -> Self {
        Self { client, settings }
    }

    async fn post(&self, attempt: &TaskAttempt, mail: &Mail) -> Result<Delivered, Failure> {
        let body = MailRequest {
            id: &mail.id,
            kind: mail.kind,
            to_fan_id: &mail.to_fan_id,
            to_name: &mail.to_name,
            subject: &mail.subject,
            concert_id: &mail.concert_id,
            seat: &mail.seat,
            amount: mail.amount,
            task_id: attempt.task_id(),
            attempt: attempt.number(),
        };
        let response = self
            .client
            .post(format!("{}/mail", self.settings.base_url))
            .header("Idempotency-Key", &mail.id)
            .json(&body)
            .send()
            .await;
        match response {
            Ok(response) if response.status().is_success() => Ok(response.json().await.unwrap_or(Delivered {
                inbox_id: mail.id.clone(),
                duplicate: false,
            })),
            Ok(response) if response.status() == StatusCode::SERVICE_UNAVAILABLE => Err(Failure::Retry(
                response
                    .json::<Refused>()
                    .await
                    .map(|refused| refused.reason_code)
                    .unwrap_or_else(|_| "mail_unavailable".to_owned()),
            )),
            Ok(response) if response.status() == StatusCode::TOO_MANY_REQUESTS || response.status().is_server_error() => {
                Err(Failure::Retry("mail_unavailable".to_owned()))
            }
            Ok(response) => Err(Failure::Permanent(format!("mail service answered {}", response.status()))),
            Err(error) => {
                tracing::warn!("mail service unreachable: {error}");
                Err(Failure::Retry("mail_unreachable".to_owned()))
            }
        }
    }
}

/// `base` plus the outcome-specific fields of one activity entry.
fn with(base: &Map<String, Value>, extra: Value) -> Value {
    let mut map = base.clone();
    if let Value::Object(extra) = extra {
        map.extend(extra);
    }
    Value::Object(map)
}

#[async_trait]
impl MailService for HttpMailer {
    async fn send(&self, ctx: &TaskContext, activity: &ActivityHub, mail: Mail) -> TaskResult {
        let attempt = ctx.attempt();
        let number = attempt.number();
        let base: Map<String, Value> = [
            ("task_id", json!(attempt.task_id())),
            ("format", json!(attempt.format().to_string())),
            ("attempt", json!(number)),
            (
                "correlation_id",
                attempt.metadata().get(CORRELATION_ID).cloned().unwrap_or_default(),
            ),
            ("inbox_id", json!(mail.id)),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect();
        activity.publish("task.attempt.started", Value::Object(base.clone()));
        match self.post(attempt, &mail).await {
            Ok(delivered) => {
                activity.publish(
                    "task.attempt.succeeded",
                    with(
                        &base,
                        json!({ "inbox_id": delivered.inbox_id, "duplicate": delivered.duplicate }),
                    ),
                );
                TaskResult::Done
            }
            Err(Failure::Retry(reason)) => {
                let retry_after = ctx.retry_pause();
                activity.publish(
                    "task.attempt.failed",
                    with(
                        &base,
                        json!({ "reason_code": reason, "retry": true, "retry_after_ms": retry_after.as_millis() as u64 }),
                    ),
                );
                TaskResult::retry(reason)
            }
            Err(Failure::Permanent(reason)) => {
                activity.publish(
                    "task.attempt.failed",
                    with(
                        &base,
                        json!({ "reason_code": "mail_rejected", "retry": false, "message": reason }),
                    ),
                );
                TaskResult::fail(reason)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, extract::State, http::HeaderMap, routing::post};
    use cqrsx::task::{format::TaskFormat, policy::Retry};
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
    };

    /// The pauses of the `MAIL` policy under the demo defaults: what the task runtime tells an attempt.
    const MAIL_PAUSE: Retry = Retry::backoff(Duration::from_secs(1), Duration::from_secs(8));

    /// The mail service, scripted: one `(status, body, delay)` per request, oldest first; when the
    /// script runs out, every request is a delivery. Every request is recorded.
    #[derive(Default)]
    struct Script {
        answers: Mutex<VecDeque<(u16, Value, Duration)>>,
        requests: Mutex<Vec<(HeaderMap, Value)>>,
    }

    async fn serve(script: Arc<Script>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route(
                "/mail",
                post(
                    |State(script): State<Arc<Script>>, headers: HeaderMap, Json(body): Json<Value>| async move {
                        script.requests.lock().unwrap().push((headers, body));
                        let (status, body, delay) = script.answers.lock().unwrap().pop_front().unwrap_or((
                            200,
                            json!({ "inbox_id": "e_ticket:b-1", "duplicate": false }),
                            Duration::ZERO,
                        ));
                        tokio::time::sleep(delay).await;
                        (axum::http::StatusCode::from_u16(status).unwrap(), Json(body))
                    },
                ),
            )
            .with_state(script);
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    }

    fn settings(base_url: String, timeout: Duration) -> MailClientSettings {
        MailClientSettings { base_url, timeout }
    }

    fn mailer(settings: MailClientSettings) -> (HttpMailer, ActivityHub) {
        let activity = ActivityHub::new(32, 32);
        let client = HttpMailer::client(&settings).unwrap();
        (HttpMailer::new(client, settings), activity)
    }

    /// Attempt `number` of an e-ticket task, told the pause of the `MAIL` policy.
    fn attempt(number: i32) -> TaskContext {
        let attempt = TaskAttempt::try_new(
            Uuid::new_v4(),
            TaskFormat::try_new("encore_send_e_ticket", 1).unwrap(),
            json!({}),
            json!({ "correlation_id": "c-1" }),
            number,
        )
        .unwrap();
        TaskContext::new(attempt, MAIL_PAUSE.pause(number))
    }

    fn mail() -> Mail {
        Mail {
            id: "e_ticket:b-1".into(),
            kind: "e_ticket",
            to_fan_id: "fox".into(),
            to_name: "Fox".into(),
            subject: "🎫".into(),
            concert_id: "radiohead-2026-11-21".into(),
            seat: "B4".into(),
            amount: 80,
        }
    }

    fn entries(activity: &ActivityHub) -> Vec<(String, Value)> {
        activity
            .subscribe()
            .0
            .iter()
            .map(|e| (e.kind.clone(), Value::Object(e.fields.clone())))
            .collect()
    }

    #[tokio::test]
    async fn a_delivered_mail_is_done_and_a_repeat_is_reported_as_a_duplicate() {
        let script = Arc::new(Script::default());
        script.answers.lock().unwrap().extend([
            (200, json!({ "inbox_id": "e_ticket:b-1", "duplicate": false }), Duration::ZERO),
            (200, json!({ "inbox_id": "e_ticket:b-1", "duplicate": true }), Duration::ZERO),
            (200, json!({}), Duration::ZERO), // a provider that answers nothing useful still delivered
        ]);
        let url = serve(script.clone()).await;
        let (mailer, activity) = mailer(settings(url, Duration::from_secs(2)));
        let first = attempt(1);
        assert_eq!(mailer.send(&first, &activity, mail()).await, TaskResult::Done);
        assert_eq!(mailer.send(&attempt(2), &activity, mail()).await, TaskResult::Done);
        assert_eq!(mailer.send(&attempt(3), &activity, mail()).await, TaskResult::Done);

        let requests = script.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        let (headers, body) = &requests[0];
        assert_eq!(headers.get("idempotency-key").unwrap().to_str().unwrap(), "e_ticket:b-1");
        assert_eq!(body["id"], "e_ticket:b-1");
        assert_eq!(body["kind"], "e_ticket");
        assert_eq!(body["to_fan_id"], "fox");
        assert_eq!(body["to_name"], "Fox");
        assert_eq!(body["seat"], "B4");
        assert_eq!(body["amount"], 80);
        assert_eq!(body["attempt"], 1);
        assert_eq!(body["task_id"], first.task_id().to_string());
        assert_eq!(requests[1].1["attempt"], 2);

        let seen = entries(&activity);
        assert_eq!(
            seen.iter().map(|(kind, _)| kind.as_str()).collect::<Vec<_>>(),
            [
                "task.attempt.started",
                "task.attempt.succeeded",
                "task.attempt.started",
                "task.attempt.succeeded",
                "task.attempt.started",
                "task.attempt.succeeded"
            ]
        );
        assert_eq!(seen[0].1["correlation_id"], "c-1");
        assert_eq!(seen[0].1["format"], "encore_send_e_ticket v1");
        assert_eq!(seen[1].1["duplicate"], false);
        assert_eq!(seen[3].1["duplicate"], true);
        assert_eq!(seen[5].1["inbox_id"], "e_ticket:b-1");
        assert_eq!(seen[5].1["duplicate"], false);
    }

    #[tokio::test]
    async fn a_refusal_is_retried_with_the_provider_reason_and_a_doubling_backoff() {
        let script = Arc::new(Script::default());
        script.answers.lock().unwrap().extend([
            (503, json!({ "reason_code": "server_on_fire" }), Duration::ZERO),
            (503, json!("no reason given"), Duration::ZERO),
            (500, json!({}), Duration::ZERO),
            (429, json!({}), Duration::ZERO),
            (502, json!({}), Duration::ZERO),
        ]);
        let url = serve(script).await;
        let (mailer, activity) = mailer(settings(url, Duration::from_secs(2)));
        let expected = [
            ("server_on_fire", 1),
            ("mail_unavailable", 2),
            ("mail_unavailable", 4),
            ("mail_unavailable", 8),
            ("mail_unavailable", 8),
        ];
        for (number, (reason, _)) in (1..).zip(expected) {
            match mailer.send(&attempt(number), &activity, mail()).await {
                TaskResult::Retry { reason: actual } => {
                    assert_eq!(actual, reason, "attempt {number}");
                }
                other => panic!("attempt {number}: expected a retry, got {other:?}"),
            }
        }
        let failed: Vec<_> = entries(&activity)
            .into_iter()
            .filter(|(kind, _)| kind == "task.attempt.failed")
            .collect();
        assert_eq!(failed.len(), 5);
        assert_eq!(failed[0].1["reason_code"], "server_on_fire");
        assert_eq!(failed[0].1["retry"], true);
        assert_eq!(failed[0].1["retry_after_ms"], 1000);
        assert_eq!(failed[4].1["retry_after_ms"], 8000);
        // Every entry shows the pause the runtime told its attempt: the doubling backoff of the mail policy.
        for ((number, (_, seconds)), (_, entry)) in (1..).zip(expected).zip(&failed) {
            assert_eq!(entry["retry_after_ms"], seconds * 1000, "attempt {number}");
        }
    }

    #[tokio::test]
    async fn a_rejected_request_is_a_permanent_failure() {
        let script = Arc::new(Script::default());
        script
            .answers
            .lock()
            .unwrap()
            .push_back((400, json!({ "error": "bad address" }), Duration::ZERO));
        let url = serve(script).await;
        let (mailer, activity) = mailer(settings(url, Duration::from_secs(2)));
        match mailer.send(&attempt(1), &activity, mail()).await {
            TaskResult::Fail { reason } => assert_eq!(reason, "mail service answered 400 Bad Request"),
            other => panic!("expected a permanent failure, got {other:?}"),
        }
        let (_, failed) = entries(&activity)
            .into_iter()
            .find(|(kind, _)| kind == "task.attempt.failed")
            .unwrap();
        assert_eq!(failed["retry"], false);
        assert_eq!(failed["reason_code"], "mail_rejected");
        assert_eq!(failed["message"], "mail service answered 400 Bad Request");
    }

    #[tokio::test]
    async fn an_unreachable_or_slow_service_is_retried() {
        // Nothing listens on the port any more.
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", closed.local_addr().unwrap());
        drop(closed);
        let (unreachable, refused) = mailer(settings(url, Duration::from_secs(2)));
        assert!(matches!(
            unreachable.send(&attempt(1), &refused, mail()).await,
            TaskResult::Retry { reason } if reason == "mail_unreachable"
        ));
        assert_eq!(entries(&refused)[1].1["retry_after_ms"], 1000);
        // The service answers, but later than the client is willing to wait.
        let script = Arc::new(Script::default());
        script
            .answers
            .lock()
            .unwrap()
            .push_back((200, json!({}), Duration::from_millis(500)));
        let url = serve(script).await;
        let (impatient, late) = mailer(settings(url, Duration::from_millis(50)));
        assert!(matches!(
            impatient.send(&attempt(3), &late, mail()).await,
            TaskResult::Retry { reason } if reason == "mail_unreachable"
        ));
        assert_eq!(entries(&late)[1].1["retry_after_ms"], 4000);
    }
}
