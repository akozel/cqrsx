//! `ViewRepository` (write side of a projection) and `ReadModelStore` (queries, wipe) over a MongoDB
//! collection: `{ _id: view_id, version: i64, last_sequence: i64, view: <V as BSON> }`, where
//! `last_sequence` is the last event of the aggregate applied to the document (the library's contiguity
//! rule: the document takes only the next one).
//! Optimistic locking mirrors the library's `PostgresViewRepository`: `version == 0` inserts (a
//! duplicate key is a conflict), otherwise
//! `update_one({ _id, version }, { $set: { view, version + 1, last_sequence } })` and `matched_count == 0`
//! is a conflict. `ViewProjection` retries conflicts (up to 16 times).
use crate::application::{
    StorageError,
    ports::{ReadModelStore, Stored},
};
use async_trait::async_trait;
use cqrsx::cqrs::{
    aggregate::Aggregate,
    projection::{
        View,
        repository::{ViewContext, ViewRepository},
    },
    store::PersistenceError,
};
use mongodb::{
    Collection, Database,
    bson::{Bson, Document, doc},
    error::{ErrorKind, WriteFailure},
};
use serde::{Deserialize, de::DeserializeOwned};
use std::marker::PhantomData;

pub struct MongoViewRepository<V, A> {
    collection: Collection<Document>,
    phantom: PhantomData<fn() -> (V, A)>,
}

/// A document before its view is decoded: the version for optimistic locking and the last applied event.
#[derive(Deserialize)]
struct RawView {
    version: i64,
    last_sequence: i64,
    view: Bson,
}

fn connection_error(error: mongodb::error::Error) -> PersistenceError {
    PersistenceError::ConnectionError(Box::new(error))
}

/// BSON has no unsigned integers: `last_sequence` is stored as `i64`.
fn stored_sequence(last_sequence: usize) -> Result<i64, PersistenceError> {
    i64::try_from(last_sequence).map_err(|e| PersistenceError::UnknownError(Box::new(e)))
}

/// A negative `last_sequence` can only be a damaged document.
fn applied_sequence(stored: i64) -> Result<usize, PersistenceError> {
    usize::try_from(stored).map_err(|e| PersistenceError::DeserializationError(Box::new(e)))
}

fn is_duplicate_key(error: &mongodb::error::Error) -> bool {
    matches!(*error.kind, ErrorKind::Write(WriteFailure::WriteError(ref e)) if e.code == 11000)
}

impl<V, A> MongoViewRepository<V, A> {
    pub fn new(database: &Database, name: &str) -> Self {
        Self {
            collection: database.collection(name),
            phantom: PhantomData,
        }
    }

    async fn document(&self, view_id: &str) -> Result<Option<Document>, PersistenceError> {
        self.collection
            .find_one(doc! { "_id": view_id })
            .await
            .map_err(connection_error)
    }

    async fn load_raw(&self, view_id: &str) -> Result<Option<RawView>, PersistenceError> {
        let Some(document) = self.document(view_id).await? else {
            return Ok(None);
        };
        let stored: RawView =
            mongodb::bson::from_document(document).map_err(|e| PersistenceError::DeserializationError(Box::new(e)))?;
        Ok(Some(stored))
    }
}

impl<V, A> ViewRepository<V, A> for MongoViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError> {
        Ok(self.load_with_context(view_id).await?.map(|(view, _)| view))
    }

    async fn load_with_context(&self, view_id: &str) -> Result<Option<(V, ViewContext)>, PersistenceError> {
        let Some(stored) = self.load_raw(view_id).await? else {
            return Ok(None);
        };
        let view: V = mongodb::bson::from_bson(stored.view).map_err(|e| PersistenceError::DeserializationError(Box::new(e)))?;
        let last_sequence = applied_sequence(stored.last_sequence)?;
        Ok(Some((
            view,
            ViewContext::new(view_id.to_owned(), stored.version, last_sequence),
        )))
    }

    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        let view = mongodb::bson::to_bson(&view).map_err(|e| PersistenceError::UnknownError(Box::new(e)))?;
        let next = context
            .version
            .checked_add(1)
            .ok_or_else(|| PersistenceError::UnknownError("view version overflow".into()))?;
        let last_sequence = stored_sequence(context.last_sequence)?;
        if context.version == 0 {
            let result = self
                .collection
                .insert_one(doc! {
                    "_id": &context.view_instance_id,
                    "version": next,
                    "last_sequence": last_sequence,
                    "view": view,
                })
                .await;
            return match result {
                Ok(_) => Ok(()),
                Err(error) if is_duplicate_key(&error) => Err(PersistenceError::OptimisticLockError),
                Err(error) => Err(connection_error(error)),
            };
        }
        let result = self
            .collection
            .update_one(
                doc! { "_id": &context.view_instance_id, "version": context.version },
                doc! { "$set": { "view": view, "version": next, "last_sequence": last_sequence } },
            )
            .await
            .map_err(connection_error)?;
        if result.matched_count == 0 {
            return Err(PersistenceError::OptimisticLockError);
        }
        Ok(())
    }
}

/// A stored document as the read API returns it: `_id` becomes the view id.
#[derive(Deserialize)]
struct StoredDocument<V> {
    #[serde(rename = "_id")]
    id: String,
    version: i64,
    view: V,
}

fn decode<V: DeserializeOwned>(document: Document) -> Result<Stored<V>, StorageError> {
    let StoredDocument { id, version, view } = mongodb::bson::from_document(document).map_err(StorageError::new)?;
    Ok(Stored { id, version, view })
}

impl<V: DeserializeOwned, A> MongoViewRepository<V, A> {
    /// The documents matching `filter`, ordered by `_id`, decoded.
    async fn find_sorted(&self, filter: Document) -> Result<Vec<Stored<V>>, StorageError> {
        use futures::TryStreamExt;
        let documents: Vec<Document> = self
            .collection
            .find(filter)
            .sort(doc! { "_id": 1 })
            .await
            .map_err(StorageError::new)?
            .try_collect()
            .await
            .map_err(StorageError::new)?;
        documents.into_iter().map(decode).collect()
    }
}

#[async_trait]
impl<V, A> ReadModelStore<V> for MongoViewRepository<V, A>
where
    V: DeserializeOwned + Send + 'static,
{
    async fn load(&self, id: &str) -> Result<Option<Stored<V>>, StorageError> {
        self.document(id).await.map_err(StorageError::new)?.map(decode).transpose()
    }

    async fn list(&self) -> Result<Vec<Stored<V>>, StorageError> {
        self.find_sorted(doc! {}).await
    }

    /// A range scan on the `_id` index: no other index is needed to read a fan's ledgers.
    async fn list_range(&self, lower: &str, upper: &str) -> Result<Vec<Stored<V>>, StorageError> {
        self.find_sorted(doc! { "_id": { "$gte": lower, "$lt": upper } }).await
    }

    /// Wipe = drop the collection.
    async fn wipe(&self) -> Result<(), StorageError> {
        self.collection.drop().await.map_err(StorageError::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::{
        concert::views::concert_hall::{ConcertHallView, ConcertSummary, SeatEntry, SeatState},
        fan::views::fan_wallet::{FanWalletView, TicketEntry, TicketEntryState},
    };
    use crate::domain::shared::venue::Zone;
    use mongodb::bson::{from_bson, from_document, to_bson};
    use std::collections::BTreeMap;

    /// The stored envelope: `version` and `last_sequence` next to the view, both required.
    #[test]
    fn the_document_envelope_carries_the_version_and_the_last_applied_event() {
        let view = to_bson(&ConcertHallView::default()).unwrap();
        let raw: RawView = from_document(doc! { "_id": "c-1", "version": 3, "last_sequence": 7, "view": view.clone() }).unwrap();
        assert_eq!((raw.version, raw.last_sequence), (3, 7));
        assert_eq!(from_bson::<ConcertHallView>(raw.view).unwrap(), ConcertHallView::default());
        assert!(
            from_document::<RawView>(doc! { "_id": "c-1", "version": 3, "view": view }).is_err(),
            "a document without last_sequence is not a read model of this version"
        );
        assert_eq!(stored_sequence(7).unwrap(), 7);
        assert_eq!(applied_sequence(7).unwrap(), 7);
        assert!(matches!(applied_sequence(-1), Err(PersistenceError::DeserializationError(_))));
        #[cfg(target_pointer_width = "64")]
        assert!(matches!(stored_sequence(usize::MAX), Err(PersistenceError::UnknownError(_))));
    }

    /// The views are stored as BSON: seat and ticket keys stay strings, options stay optional.
    #[test]
    fn both_views_survive_the_bson_round_trip() {
        let hall = ConcertHallView {
            concert: Some(ConcertSummary {
                artist: "A".into(),
                city: "C".into(),
                date: "2026-11-21".into(),
            }),
            capacity: 40,
            seats: BTreeMap::from([(
                "B4".to_owned(),
                SeatEntry {
                    state: SeatState::Booked,
                    fan_id: Some("fox".into()),
                    fan_name: Some("Fox".into()),
                    zone: Some(Zone::Standard),
                    price: Some(80),
                    booking_id: Some("b-1".into()),
                    sequence: 2,
                    changed_at: "2026-10-17T09:00:02.000000Z".into(),
                },
            )]),
            sold: 1,
            revenue: 80,
            ..ConcertHallView::default()
        };
        assert_eq!(from_bson::<ConcertHallView>(to_bson(&hall).unwrap()).unwrap(), hall);
        let wallet = FanWalletView {
            fan_id: "fox".into(),
            fan_name: "Fox".into(),
            tickets: BTreeMap::from([(
                "b-1".to_owned(),
                TicketEntry {
                    concert_id: "c-1".into(),
                    seat: "B4".into(),
                    price: 80,
                    state: TicketEntryState::Cancelled,
                    refund: Some(80),
                    sequence: 2,
                    changed_at: "2026-10-17T09:00:02.000000Z".into(),
                },
            )]),
            ..FanWalletView::default()
        };
        assert_eq!(from_bson::<FanWalletView>(to_bson(&wallet).unwrap()).unwrap(), wallet);
    }
}
