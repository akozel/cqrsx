//! A read model in a MongoDB collection: cqrsx writes its documents (`MongoViewRepository`, one document per
//! view: `{ _id: view_id, version, last_sequence, view }`), the demo queries the collection by that shape
//! and drops it to wipe the read model.
use crate::application::{
    StorageError,
    ports::{ReadModelStore, Stored},
};
use async_trait::async_trait;
use cqrsx::{
    cqrs::{
        aggregate::Aggregate,
        projection::{
            View,
            repository::{ViewContext, ViewRepository},
        },
        store::PersistenceError,
    },
    mongodb::view_repository::MongoViewRepository,
};
use mongodb::{
    Collection, Database,
    bson::{Document, doc},
};
use serde::{Deserialize, de::DeserializeOwned};

/// One read model: the projection writes through the library's repository, the queries and the wipe read
/// and drop the same collection.
pub struct MongoReadModel<V, A> {
    views: MongoViewRepository<V, A>,
    collection: Collection<Document>,
}

impl<V, A> MongoReadModel<V, A> {
    pub fn new(database: &Database, name: &str) -> Self {
        Self {
            views: MongoViewRepository::new(database, name),
            collection: database.collection(name),
        }
    }
}

impl<V: View<A>, A: Aggregate> ViewRepository<V, A> for MongoReadModel<V, A> {
    async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError> {
        self.views.load(view_id).await
    }

    async fn load_with_context(&self, view_id: &str) -> Result<Option<(V, ViewContext)>, PersistenceError> {
        self.views.load_with_context(view_id).await
    }

    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        self.views.update_view(view, context).await
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

impl<V: DeserializeOwned, A> MongoReadModel<V, A> {
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
impl<V, A> ReadModelStore<V> for MongoReadModel<V, A>
where
    V: DeserializeOwned + Send + 'static,
    A: 'static,
{
    async fn load(&self, id: &str) -> Result<Option<Stored<V>>, StorageError> {
        self.collection
            .find_one(doc! { "_id": id })
            .await
            .map_err(StorageError::new)?
            .map(decode)
            .transpose()
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
    use mongodb::bson::{from_bson, to_bson};
    use std::collections::BTreeMap;

    /// The read API takes `_id`, `version` and `view` from a document of the library's shape.
    #[test]
    fn a_document_of_the_library_shape_reads_as_a_stored_view() {
        let view = to_bson(&ConcertHallView::default()).unwrap();
        let stored: Stored<ConcertHallView> =
            decode(doc! { "_id": "c-1", "version": 3_i64, "last_sequence": 7_i64, "view": view.clone() }).unwrap();
        assert_eq!(
            stored,
            Stored {
                id: "c-1".into(),
                version: 3,
                view: ConcertHallView::default()
            }
        );
        assert!(decode::<ConcertHallView>(doc! { "_id": "c-1", "view": view }).is_err());
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
