//! `mail_service.inbox` — where the mail service keeps the delivered e-mails. One document per
//! business id: `{ _id, kind, to_fan_id, to_name, subject, concert_id, seat, amount, task_id,
//! delivered_on_attempt, first_delivered_at, deliveries }`. The X-ray shows it as it is.
use crate::mail::{Delivery, MailRequest, MailStore};
use mongodb::{
    Collection,
    bson::{Document, doc},
};

pub struct MongoInbox {
    collection: Collection<Document>,
}

impl MongoInbox {
    pub fn new(collection: Collection<Document>) -> Self {
        Self { collection }
    }
}

/// The upsert of one delivery: the e-mail is written once (`$setOnInsert`, with the attempt that
/// got through), every delivery is counted (`$inc`). Returns `(filter, update)`.
pub fn upsert(key: &str, mail: &MailRequest, now: &str) -> (Document, Document) {
    (
        doc! { "_id": key },
        doc! {
            "$setOnInsert": {
                "kind": &mail.kind,
                "to_fan_id": &mail.to_fan_id,
                "to_name": &mail.to_name,
                "subject": &mail.subject,
                "concert_id": &mail.concert_id,
                "seat": &mail.seat,
                "amount": mail.amount,
                "task_id": &mail.task_id,
                "delivered_on_attempt": mail.attempt,
                "first_delivered_at": now,
            },
            "$inc": { "deliveries": 1 },
        },
    )
}

impl MailStore for MongoInbox {
    async fn deliver(&self, key: &str, mail: &MailRequest, now: &str) -> Result<Delivery, String> {
        let (filter, update) = upsert(key, mail, now);
        let result = self
            .collection
            .update_one(filter, update)
            .upsert(true)
            .await
            .map_err(|e| format!("inbox write failed: {e}"))?;
        Ok(Delivery {
            inbox_id: key.to_owned(),
            duplicate: result.upserted_id.is_none(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mail::tests::mail;

    /// The document shape is a contract with the X-ray (`MailboxPanel`) and the wallet (`delivered_on_attempt`).
    #[test]
    fn the_upsert_writes_the_mail_once_and_counts_every_delivery() {
        let (filter, update) = upsert("e_ticket:b-1", &mail(2), "2026-10-17T09:00:00.000Z");
        assert_eq!(filter, doc! { "_id": "e_ticket:b-1" });
        let on_insert = update.get_document("$setOnInsert").unwrap();
        assert_eq!(on_insert.get_str("kind").unwrap(), "e_ticket");
        assert_eq!(on_insert.get_str("to_fan_id").unwrap(), "fox");
        assert_eq!(on_insert.get_i64("amount").unwrap(), 80);
        assert_eq!(on_insert.get_str("task_id").unwrap(), "t-1");
        assert_eq!(on_insert.get_i32("delivered_on_attempt").unwrap(), 2);
        assert_eq!(on_insert.get_str("first_delivered_at").unwrap(), "2026-10-17T09:00:00.000Z");
        assert_eq!(update.get_document("$inc").unwrap(), &doc! { "deliveries": 1 });
    }
}
