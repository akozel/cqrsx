//! Read-only MongoDB inspection with a whitelist of collections.
use futures::TryStreamExt;
use mongodb::{
    Client,
    bson::{Bson, Document, doc},
};
use serde::Serialize;
use serde_json::Value;

/// `(database, collection)` pairs the sidecar is allowed to show.
pub const WHITELIST: [(&str, &str); 3] = [
    ("encore_read", "concert_halls"),
    ("encore_read", "fan_wallets"),
    ("mail_service", "inbox"),
];

pub fn allowed(database: &str, collection: &str) -> bool {
    WHITELIST.contains(&(database, collection))
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CollectionView {
    pub source: &'static str,
    pub database: String,
    pub collection: String,
    pub query: String,
    pub total: u64,
    pub docs: Vec<Value>,
}

#[derive(Clone)]
pub struct MongoInspector {
    client: Client,
    read_db: String,
    mail_db: String,
}

impl MongoInspector {
    pub fn new(client: Client, read_db: String, mail_db: String) -> Self {
        Self {
            client,
            read_db,
            mail_db,
        }
    }

    /// Translates the logical names from the whitelist to the configured database names.
    fn database_name(&self, logical: &str) -> String {
        match logical {
            "encore_read" => self.read_db.clone(),
            "mail_service" => self.mail_db.clone(),
            other => other.to_owned(),
        }
    }

    pub async fn collection(
        &self,
        database: &str,
        collection: &str,
        limit: i64,
    ) -> Result<CollectionView, mongodb::error::Error> {
        let coll = self
            .client
            .database(&self.database_name(database))
            .collection::<Document>(collection);
        let total = coll.count_documents(doc! {}).await?;
        let docs: Vec<Document> = coll
            .find(doc! {})
            .sort(doc! { "_id": 1 })
            .limit(limit)
            .await?
            .try_collect()
            .await?;
        Ok(CollectionView {
            source: "mongodb",
            database: database.to_owned(),
            collection: collection.to_owned(),
            query: format!("db.getSiblingDB('{database}').{collection}.find({{}}).sort({{ _id: 1 }}).limit({limit})"),
            total,
            docs: docs.into_iter().map(|d| Bson::Document(d).into_relaxed_extjson()).collect(),
        })
    }

    /// The shared client (for the demo lifecycle, which drops the whitelisted collections).
    pub fn client(&self) -> Client {
        self.client.clone()
    }

    pub async fn ping(&self) -> Result<(), mongodb::error::Error> {
        self.client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whitelist_rejects_everything_else() {
        assert!(allowed("encore_read", "concert_halls"));
        assert!(allowed("encore_read", "fan_wallets"));
        assert!(allowed("mail_service", "inbox"));
        assert!(!allowed("admin", "system.users"));
        assert!(!allowed("encore_read", "inbox"));
        assert!(!allowed("mail_service", "concert_halls"));
    }
}
