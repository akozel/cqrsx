//! `mail_service.inbox` — the storage of the simulated external mail service. A separate database
//! on purpose: it is an external effect, not a read model, and wipe/replay never touch it.
use crate::{
    application::{
        StorageError,
        ports::{DeliveredMail, Inbox, Mail},
    },
    domain::FanId,
    infrastructure::mail::flaky_mail_server::{Delivery, MailStore},
};
use async_trait::async_trait;
use chrono::{SecondsFormat, Utc};
use futures::TryStreamExt;
use mongodb::{
    Collection,
    bson::{Document, doc},
};
use uuid::Uuid;

pub struct MongoInbox {
    collection: Collection<Document>,
}

impl MongoInbox {
    pub fn new(collection: Collection<Document>) -> Self {
        Self { collection }
    }
}

/// An idempotent upsert keyed by the business id: a repeat only increments `deliveries`.
impl MailStore for MongoInbox {
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

#[async_trait]
impl Inbox for MongoInbox {
    async fn of_fan(&self, fan_id: &FanId) -> Result<Vec<DeliveredMail>, StorageError> {
        let documents: Vec<Document> = self
            .collection
            .find(doc! { "to_fan_id": fan_id.as_str() })
            .sort(doc! { "first_delivered_at": 1 })
            .await
            .map_err(StorageError::new)?
            .try_collect()
            .await
            .map_err(StorageError::new)?;
        documents
            .into_iter()
            .map(|mut document| {
                if let Some(id) = document.remove("_id") {
                    document.insert("inbox_id", id);
                }
                mongodb::bson::from_document(document).map_err(StorageError::new)
            })
            .collect()
    }
}
