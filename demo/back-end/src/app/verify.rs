//! 🔍 Verify [P1]: rebuild both read models into temporary collections with the very same
//! `ViewProjection`s and compare the canonical documents (without `version`) with the live ones.
//! No in-memory fakes at runtime (rule §12.4): the temporary collections live in MongoDB.
use crate::{
    app::read_side::{CONCERT_HALLS, FAN_WALLETS, ReadSide},
    domain::{concert::Concert, fan::Fan},
    projections::{concert_hall::ConcertHallView, fan_wallet::FanWalletView, mongo_view_repository::MongoViewRepository},
};
use cqrsx::{
    cqrs::{aggregate::Aggregate, event_handler::replay::EventReplay, projection::ViewProjection},
    postgres::event_repository::PostgresEventRepository,
};
use mongodb::{
    Database,
    bson::{Bson, Document},
};
use serde_json::{Value, json};
use sqlx::PgPool;
use std::{collections::BTreeMap, sync::Arc};

pub struct VerifyService {
    pool: PgPool,
    read_db: Database,
    read: Arc<ReadSide>,
}

fn canonical(document: Document) -> Value {
    let mut json = Bson::Document(document).into_relaxed_extjson();
    if let Value::Object(map) = &mut json {
        map.remove("version");
    }
    json
}

fn index(documents: Vec<Document>) -> BTreeMap<String, Value> {
    documents
        .into_iter()
        .map(|d| (d.get_str("_id").unwrap_or_default().to_owned(), canonical(d)))
        .collect()
}

impl VerifyService {
    pub fn new(pool: PgPool, read_db: Database, read: Arc<ReadSide>) -> Self {
        Self { pool, read_db, read }
    }

    async fn rebuild_into<A, V>(&self, name: &'static str) -> Result<Vec<Document>, String>
    where
        A: Aggregate + 'static,
        V: cqrsx::cqrs::projection::View<A> + 'static,
    {
        let temp = Arc::new(MongoViewRepository::<V, A>::new(&self.read_db, name));
        temp.wipe().await.map_err(|e| e.to_string())?;
        let replay = EventReplay::new(
            PostgresEventRepository::new(self.pool.clone()),
            ViewProjection::new(temp.clone()),
        );
        let replayed = replay
            .replay_all()
            .await
            .map_err(|e| format!("{} replay failed: {e}", A::TYPE));
        let documents = temp.documents().await.map_err(|e| e.to_string());
        // Always clean up, even after a failure.
        let _ = temp.wipe().await;
        replayed?;
        documents
    }

    pub async fn run(&self) -> Result<Value, String> {
        let halls_expected = index(self.rebuild_into::<Concert, ConcertHallView>("concert_halls_verify").await?);
        let wallets_expected = index(self.rebuild_into::<Fan, FanWalletView>("fan_wallets_verify").await?);
        let halls_actual = index(self.read.halls.documents().await.map_err(|e| e.to_string())?);
        let wallets_actual = index(self.read.wallets.documents().await.map_err(|e| e.to_string())?);
        let mut differences = Vec::new();
        for (collection, expected, actual) in [
            (CONCERT_HALLS, &halls_expected, &halls_actual),
            (FAN_WALLETS, &wallets_expected, &wallets_actual),
        ] {
            let ids: std::collections::BTreeSet<&String> = expected.keys().chain(actual.keys()).collect();
            for id in ids {
                match (expected.get(id), actual.get(id)) {
                    (Some(e), Some(a)) if e == a => {}
                    (Some(_), None) => differences.push(json!({ "collection": collection, "id": id, "kind": "missing" })),
                    (None, Some(_)) => differences.push(json!({ "collection": collection, "id": id, "kind": "unexpected" })),
                    _ => differences.push(json!({ "collection": collection, "id": id, "kind": "differs" })),
                }
            }
        }
        Ok(json!({
            "ok": differences.is_empty(),
            "checked": { CONCERT_HALLS: halls_expected.len(), FAN_WALLETS: wallets_expected.len() },
            "differences": differences,
        }))
    }
}
