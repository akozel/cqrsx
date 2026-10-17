//! DEMO ONLY — the moody mail server shared by the two mail task handlers.
//!
//! Each attempt: announce itself, sleep a random "network" latency, roll the dice (20 % → retry
//! with a funny reason and a short backoff), otherwise write the e-mail into the external inbox
//! with an idempotent upsert keyed by the business id. Delivery is at-least-once (a lease can
//! expire, the process can crash after the effect and before settlement), so a repeat must not
//! create a second e-mail: `deliveries > 1` means "duplicate suppressed".
use crate::{application::activity::ActivityHub, infrastructure::mail::chaos::ChaosPolicy};
use chrono::{SecondsFormat, Utc};
use cqrsx::task::{attempt::TaskAttempt, handler::TaskHandlerError};
use mongodb::{
    Collection,
    bson::{Document, doc},
};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde_json::json;
use std::{future::Future, sync::Arc, time::Duration};
use uuid::Uuid;

/// Funny failure reasons (`reason_code`); texts live in the UI i18n.
pub const REASONS: [&str; 4] = ["smtp_timeout", "server_on_fire", "rate_limited", "mailbox_full"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mail {
    /// Business key: `e_ticket:<booking_id>` or `refund:<cancellation_id>`.
    pub id: String,
    pub kind: &'static str,
    pub to_fan_id: String,
    pub to_name: String,
    pub subject: String,
    pub concert_id: String,
    pub seat: String,
    pub amount: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub inbox_id: String,
    /// `true` when the e-mail already existed (the upsert only bumped `deliveries`).
    pub duplicate: bool,
}

/// The "external mail service" storage. Native async fn; only the MongoDB implementation exists
/// at runtime (an in-memory one lives in tests).
pub trait Inbox: Send + Sync {
    fn deliver(&self, mail: Mail, task_id: Uuid, attempt: i32) -> impl Future<Output = Result<Delivery, String>> + Send;
}

/// `mail_service.inbox` — a separate database on purpose: it is an external effect, not a read
/// model, and wipe/replay never touch it.
pub struct MongoInbox {
    collection: Collection<Document>,
}

impl MongoInbox {
    pub fn new(collection: Collection<Document>) -> Self {
        Self { collection }
    }
}

impl Inbox for MongoInbox {
    async fn deliver(&self, mail: Mail, task_id: Uuid, attempt: i32) -> Result<Delivery, String> {
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let result = self
            .collection
            .update_one(
                doc! { "_id": &mail.id },
                doc! {
                    "$setOnInsert": {
                        "kind": mail.kind,
                        "to_fan_id": &mail.to_fan_id,
                        "to_name": &mail.to_name,
                        "subject": &mail.subject,
                        "concert_id": &mail.concert_id,
                        "seat": &mail.seat,
                        "amount": mail.amount,
                        "task_id": task_id.to_string(),
                        "delivered_on_attempt": attempt,
                        "first_delivered_at": &now,
                    },
                    "$inc": { "deliveries": 1 },
                },
            )
            .upsert(true)
            .await
            .map_err(|e| format!("inbox write failed: {e}"))?;
        Ok(Delivery {
            inbox_id: mail.id,
            duplicate: result.upserted_id.is_none(),
        })
    }
}

#[derive(Clone)]
pub struct MailSettings {
    pub latency: (Duration, Duration),
    pub retry_base: Duration,
    pub retry_max: Duration,
}

pub struct FlakyMailServer<I: Inbox, C: ChaosPolicy> {
    inbox: Arc<I>,
    chaos: C,
    settings: MailSettings,
    activity: Arc<ActivityHub>,
    latency_rng: StdRng,
}

impl<I: Inbox, C: ChaosPolicy> FlakyMailServer<I, C> {
    pub fn new(inbox: Arc<I>, chaos: C, settings: MailSettings, activity: Arc<ActivityHub>, seed: u64) -> Self {
        Self {
            inbox,
            chaos,
            settings,
            activity,
            latency_rng: StdRng::seed_from_u64(seed),
        }
    }

    /// Backoff requested from the queue: `min(base · 2^(n−1), max)` → 1, 2, 4, 8, 8 s by default.
    pub fn retry_after(&self, attempt: i32) -> Duration {
        let exponent = u32::try_from(attempt.max(1) - 1).unwrap_or(u32::MAX).min(16);
        self.settings
            .retry_base
            .saturating_mul(2u32.saturating_pow(exponent))
            .min(self.settings.retry_max)
    }

    pub async fn send(&mut self, attempt: &TaskAttempt, mail: Mail) -> Result<(), TaskHandlerError> {
        let number = attempt.number();
        let format = attempt.format().to_string();
        let correlation_id = attempt.metadata().get("correlation_id").cloned().unwrap_or_default();
        self.activity.publish(
            "task.attempt.started",
            json!({ "task_id": attempt.task_id(), "format": format, "attempt": number, "correlation_id": correlation_id,
                    "inbox_id": mail.id }),
        );
        let (min, max) = self.settings.latency;
        let latency = if max > min {
            self.latency_rng.random_range(min..=max)
        } else {
            min
        };
        tokio::time::sleep(latency).await;
        // DEMO ONLY: the dice roll.
        if self.chaos.should_fail() {
            let reason = REASONS[self.chaos.pick_reason(REASONS.len()).min(REASONS.len() - 1)];
            let retry_after = self.retry_after(number);
            self.activity.publish(
                "task.attempt.failed",
                json!({ "task_id": attempt.task_id(), "format": format, "attempt": number, "reason_code": reason,
                        "retry": true, "retry_after_ms": retry_after.as_millis() as u64, "inbox_id": mail.id }),
            );
            return Err(TaskHandlerError::Retry {
                reason: reason.to_owned(),
                retry_after: Some(retry_after),
            });
        }
        match self.inbox.deliver(mail, attempt.task_id(), number).await {
            Ok(delivery) => {
                self.activity.publish(
                    "task.attempt.succeeded",
                    json!({ "task_id": attempt.task_id(), "format": format, "attempt": number,
                            "inbox_id": delivery.inbox_id, "duplicate": delivery.duplicate }),
                );
                Ok(())
            }
            Err(message) => {
                tracing::warn!("mail inbox unavailable: {message}");
                let retry_after = self.retry_after(number);
                self.activity.publish(
                    "task.attempt.failed",
                    json!({ "task_id": attempt.task_id(), "format": format, "attempt": number, "reason_code": "inbox_unavailable",
                            "retry": true, "retry_after_ms": retry_after.as_millis() as u64 }),
                );
                Err(TaskHandlerError::Retry {
                    reason: "inbox_unavailable".to_owned(),
                    retry_after: Some(retry_after),
                })
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::infrastructure::mail::chaos::{Always, Never};
    use cqrsx::task::format::TaskFormat;
    use std::{collections::BTreeMap, sync::Mutex};

    /// Test-only inbox with the same upsert semantics as MongoDB.
    #[derive(Default)]
    pub(crate) struct MemoryInbox {
        pub(crate) mails: Mutex<BTreeMap<String, (Mail, i32, u32)>>,
    }

    impl Inbox for MemoryInbox {
        async fn deliver(&self, mail: Mail, _: Uuid, attempt: i32) -> Result<Delivery, String> {
            let mut mails = self.mails.lock().map_err(|e| e.to_string())?;
            let id = mail.id.clone();
            let duplicate = match mails.get_mut(&id) {
                Some(entry) => {
                    entry.2 += 1;
                    true
                }
                None => {
                    mails.insert(id.clone(), (mail, attempt, 1));
                    false
                }
            };
            Ok(Delivery { inbox_id: id, duplicate })
        }
    }

    pub(crate) fn settings() -> MailSettings {
        MailSettings {
            latency: (Duration::ZERO, Duration::ZERO),
            retry_base: Duration::from_secs(1),
            retry_max: Duration::from_secs(8),
        }
    }

    pub(crate) fn attempt(number: i32) -> TaskAttempt {
        TaskAttempt::new(
            Uuid::new_v4(),
            TaskFormat::new("encore_send_e_ticket", 1).unwrap(),
            json!({}),
            json!({ "correlation_id": "c-1" }),
            number,
        )
        .unwrap()
    }

    pub(crate) fn mail() -> Mail {
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

    #[tokio::test]
    async fn always_failing_server_retries_with_backoff_and_writes_nothing() {
        let inbox = Arc::new(MemoryInbox::default());
        let activity = Arc::new(ActivityHub::new(16, 16));
        let mut server = FlakyMailServer::new(inbox.clone(), Always, settings(), activity.clone(), 1);
        for (number, expected) in [(1, 1), (2, 2), (3, 4), (4, 8), (5, 8), (6, 8)] {
            let result = server.send(&attempt(number), mail()).await;
            match result {
                Err(TaskHandlerError::Retry { reason, retry_after }) => {
                    assert!(REASONS.contains(&reason.as_str()));
                    assert_eq!(retry_after, Some(Duration::from_secs(expected)));
                }
                other => panic!("expected retry, got {other:?}"),
            }
        }
        assert!(inbox.mails.lock().unwrap().is_empty());
        let (backlog, _) = activity.subscribe();
        assert_eq!(backlog.iter().filter(|e| e.kind == "task.attempt.failed").count(), 6);
    }

    #[tokio::test]
    async fn reliable_server_delivers_once_and_a_repeat_is_a_suppressed_duplicate() {
        let inbox = Arc::new(MemoryInbox::default());
        let activity = Arc::new(ActivityHub::new(16, 16));
        let mut server = FlakyMailServer::new(inbox.clone(), Never, settings(), activity.clone(), 1);
        server.send(&attempt(2), mail()).await.unwrap();
        server.send(&attempt(3), mail()).await.unwrap();
        let mails = inbox.mails.lock().unwrap();
        assert_eq!(mails.len(), 1);
        let (_, delivered_on, deliveries) = &mails["e_ticket:b-1"];
        assert_eq!((*delivered_on, *deliveries), (2, 2));
        let (backlog, _) = activity.subscribe();
        let succeeded: Vec<_> = backlog.iter().filter(|e| e.kind == "task.attempt.succeeded").collect();
        assert_eq!(succeeded.len(), 2);
        assert_eq!(succeeded[0].fields["duplicate"], false);
        assert_eq!(succeeded[1].fields["duplicate"], true);
    }
}
