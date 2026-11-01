//! Read-model documents in a MongoDB collection.
use crate::{is_duplicate_key, persistence_error};
use ::mongodb::{
    Collection, Database,
    bson::{self, Bson, Document, doc},
};
use cqrsx::{
    domain::aggregate::Aggregate,
    journal::PersistenceError,
    view::{
        View,
        repository::{StoredView, ViewMeta, ViewRepository},
    },
};
use serde::Deserialize;
use std::{fmt, marker::PhantomData};

/// Stores serialized read-model views in a MongoDB collection, one document per view:
///
/// ```text
/// { _id: <view id>, revision: <Int64>, last_sequence: <Int64>, payload: <the view as BSON> }
/// ```
///
/// The collection is the view's name ([`View::NAME`]). `revision` is the optimistic revision (1 after the first
/// write) and `last_sequence` the last applied event of the view's aggregate (see `ViewMeta`). A new document is
/// inserted, and a duplicate `_id` is a conflict; an existing one is replaced only at the revision it was read with.
/// The queries read by `_id`: one document (`find_one`), every document and a range of ids (`find` sorted by `_id`,
/// the order of the ids' bytes), so the collection needs no index beyond `_id`. The application drops the
/// collection to wipe the read model.
///
/// ```no_run
/// # mod example {
/// # use cqrsx::{domain::{aggregate::Aggregate, command::Decide, event::{Events, EventEnvelope}}, view::View};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// # pub struct Counted;
/// # impl Events for Counted {
/// #     fn event_type(&self) -> &'static str { "counted" }
/// #     fn event_version(&self) -> &'static str { "1" }
/// # }
/// # #[derive(Debug, thiserror::Error)]
/// # #[error("never")]
/// # pub struct Never;
/// # #[derive(Debug, Default, Clone, Serialize, Deserialize)]
/// # pub struct Counter;
/// # impl Aggregate for Counter {
/// #     const TYPE: &'static str = "counter";
/// #     type Id = String;
/// #     type Command = ();
/// #     type Event = Counted;
/// #     type Rejection = Never;
/// #     type Rules = ();
/// #     fn apply(&mut self, _: Counted) {}
/// # }
/// # impl Decide<Counter> for () {
/// #     fn decide(self, _: &Counter, _: &()) -> Result<Counted, Never> { Ok(Counted) }
/// # }
/// # #[derive(Debug, Default, Serialize, Deserialize)]
/// # pub struct Total { pub count: usize }
/// # impl View<Counter> for Total {
/// #     const NAME: &'static str = "totals";
/// #     fn apply(&mut self, _: &EventEnvelope<Counter>) { self.count += 1; }
/// # }
/// # }
/// # use example::{Counter, Total};
/// use cqrsx::view::projection::ViewProjection;
/// use cqrsx_mongodb::view_repository::MongoViewRepository;
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = mongodb::Client::with_uri_str("mongodb://localhost:27017").await?;
/// let totals = Arc::new(MongoViewRepository::<Total, Counter>::new(&client.database("read"))); // collection `totals`
/// let projection = ViewProjection::new(totals);
/// # Ok(()) }
/// ```
pub struct MongoViewRepository<V, A> {
    collection: Collection<Document>,
    _phantom: PhantomData<fn() -> (V, A)>,
}

// By hand: the collection's own `Debug` prints the whole client; its name is enough.
impl<V, A> fmt::Debug for MongoViewRepository<V, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MongoViewRepository")
            .field("collection", &self.collection.name())
            .finish_non_exhaustive()
    }
}

impl<V: View<A>, A: Aggregate> MongoViewRepository<V, A> {
    /// The views live in the collection [`View::NAME`] of `database`.
    pub fn new(database: &Database) -> Self {
        Self {
            collection: database.collection(V::NAME),
            _phantom: PhantomData,
        }
    }
}

/// The envelope of a stored view; every field is required.
#[derive(Debug, Deserialize)]
struct Stored {
    #[serde(rename = "_id")]
    id: String,
    revision: i64,
    last_sequence: i64,
    payload: Bson,
}

fn stored_sequence(last_sequence: usize) -> Result<i64, PersistenceError> {
    i64::try_from(last_sequence).map_err(|error| PersistenceError::Unexpected(Box::new(error)))
}

fn applied_sequence(stored: i64) -> Result<usize, PersistenceError> {
    usize::try_from(stored).map_err(|error| PersistenceError::Deserialization(Box::new(error)))
}

fn unreadable(error: bson::de::Error) -> PersistenceError {
    PersistenceError::Deserialization(Box::new(error))
}

/// A stored document, decoded.
fn decode<V: View<A>, A: Aggregate>(document: Document) -> Result<StoredView<V>, PersistenceError> {
    let stored: Stored = bson::from_document(document).map_err(unreadable)?;
    let last_sequence = applied_sequence(stored.last_sequence)?;
    Ok(StoredView {
        id: stored.id,
        revision: stored.revision,
        last_sequence,
        view: bson::from_bson(stored.payload).map_err(unreadable)?,
    })
}

impl<V: View<A>, A: Aggregate> MongoViewRepository<V, A> {
    /// The view with its revision and last applied event.
    async fn stored(&self, view_id: &str) -> Result<Option<StoredView<V>>, PersistenceError> {
        let Some(document) = self
            .collection
            .find_one(doc! { "_id": view_id })
            .await
            .map_err(persistence_error)?
        else {
            return Ok(None);
        };
        decode::<V, A>(document).map(Some)
    }

    /// The documents matching `filter`, ordered by `_id`, decoded.
    async fn sorted(&self, filter: Document) -> Result<Vec<StoredView<V>>, PersistenceError> {
        let mut cursor = self
            .collection
            .find(filter)
            .sort(doc! { "_id": 1 })
            .await
            .map_err(persistence_error)?;
        let mut documents = Vec::new();
        while cursor.advance().await.map_err(persistence_error)? {
            let document = cursor
                .deserialize_current()
                .map_err(|error| PersistenceError::Deserialization(Box::new(error)))?;
            documents.push(decode::<V, A>(document)?);
        }
        Ok(documents)
    }
}

impl<V, A> ViewRepository<V, A> for MongoViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError> {
        Ok(self.stored(view_id).await?.map(|stored| stored.view))
    }

    async fn load_stored(&self, view_id: &str) -> Result<Option<StoredView<V>>, PersistenceError> {
        self.stored(view_id).await
    }

    async fn update_view(&self, view: V, context: ViewMeta) -> Result<(), PersistenceError> {
        let view = bson::to_bson(&view).map_err(|error| PersistenceError::Unexpected(Box::new(error)))?;
        let revision = context
            .revision
            .checked_add(1)
            .ok_or_else(|| PersistenceError::Unexpected("view revision overflow".into()))?;
        let last_sequence = stored_sequence(context.last_sequence)?;
        if context.revision == 0 {
            let document = doc! { "_id": &context.id, "revision": revision, "last_sequence": last_sequence, "payload": view };
            return match self.collection.insert_one(document).await {
                Ok(_) => Ok(()),
                Err(error) if is_duplicate_key(&error) => Err(PersistenceError::Conflict),
                Err(error) => Err(persistence_error(error)),
            };
        }
        let updated = self
            .collection
            .update_one(
                doc! { "_id": &context.id, "revision": context.revision },
                doc! { "$set": { "payload": view, "revision": revision, "last_sequence": last_sequence } },
            )
            .await
            .map_err(persistence_error)?;
        if updated.matched_count == 0 {
            return Err(PersistenceError::Conflict);
        }
        Ok(())
    }

    async fn list(&self) -> Result<Vec<StoredView<V>>, PersistenceError> {
        self.sorted(doc! {}).await
    }

    /// A range scan on the `_id` index.
    async fn range(&self, lower: &str, upper: &str) -> Result<Vec<StoredView<V>>, PersistenceError> {
        self.sorted(doc! { "_id": { "$gte": lower, "$lt": upper } }).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::Ledger;
    use cqrsx::domain::event::EventEnvelope;
    use serde::Serialize;

    /// A view BSON cannot hold: an unsigned value beyond `Int64`.
    #[derive(Debug, Default, Serialize, Deserialize)]
    struct Huge {
        value: u64,
    }

    impl View<Ledger> for Huge {
        const NAME: &'static str = "huge";
        fn apply(&mut self, _: &EventEnvelope<Ledger>) {}
    }

    #[tokio::test]
    async fn an_unreachable_server_is_a_connection_error_and_a_view_bson_cannot_hold_is_unexpected() {
        let views = MongoViewRepository::<Huge, Ledger>::new(&crate::test_support::unreachable().await);
        assert_eq!(format!("{views:?}"), r#"MongoViewRepository { collection: "huge", .. }"#);
        assert!(matches!(views.load("a").await, Err(PersistenceError::Connection(_))));
        assert!(matches!(views.load_stored("a").await, Err(PersistenceError::Connection(_))));
        for version in [0, 1] {
            assert!(matches!(
                views
                    .update_view(Huge::default(), ViewMeta::new("a".into(), version, 1))
                    .await,
                Err(PersistenceError::Connection(_))
            ));
        }
        // Refused before any statement.
        assert!(matches!(
            views
                .update_view(Huge { value: u64::MAX }, ViewMeta::new("a".into(), 1, 2))
                .await,
            Err(PersistenceError::Unexpected(_))
        ));
        assert!(matches!(
            views
                .update_view(Huge::default(), ViewMeta::new("a".into(), i64::MAX, 2))
                .await,
            Err(PersistenceError::Unexpected(_))
        ));
    }

    #[tokio::test]
    async fn an_unreachable_server_fails_a_listing_and_a_range_as_a_connection_error() {
        let views = MongoViewRepository::<Huge, Ledger>::new(&crate::test_support::unreachable().await);
        assert!(matches!(views.list().await, Err(PersistenceError::Connection(_))));
        assert!(matches!(
            views.range("fox:", "fox;").await,
            Err(PersistenceError::Connection(_))
        ));
    }

    #[test]
    fn the_envelope_needs_the_revision_the_last_applied_event_and_the_payload() {
        let stored: Stored =
            bson::from_document(doc! { "_id": "c-1", "revision": 3_i64, "last_sequence": 7_i64, "payload": { "count": 7 } })
                .unwrap();
        assert_eq!((stored.revision, stored.last_sequence), (3, 7));
        assert_eq!(stored.payload, Bson::Document(doc! { "count": 7 }));
        for incomplete in [
            doc! { "revision": 1_i64, "payload": {} },
            doc! { "last_sequence": 1_i64, "payload": {} },
            doc! { "revision": 1_i64, "last_sequence": 1_i64 },
        ] {
            assert!(bson::from_document::<Stored>(incomplete).is_err());
        }
    }

    #[test]
    fn sequences_round_trip_and_values_out_of_range_are_explicit() {
        assert_eq!(stored_sequence(7).unwrap(), 7);
        assert_eq!(applied_sequence(7).unwrap(), 7);
        assert!(matches!(applied_sequence(-1), Err(PersistenceError::Deserialization(_))));
        #[cfg(target_pointer_width = "64")]
        assert!(matches!(stored_sequence(usize::MAX), Err(PersistenceError::Unexpected(_))));
        let error = bson::from_document::<Stored>(doc! {}).unwrap_err();
        assert!(matches!(unreadable(error), PersistenceError::Deserialization(_)));
    }
}
