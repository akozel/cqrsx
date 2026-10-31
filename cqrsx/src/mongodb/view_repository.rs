//! Read-model documents in a MongoDB collection.
use crate::{
    domain::aggregate::Aggregate,
    journal::PersistenceError,
    mongodb::{is_duplicate_key, persistence},
    view::{
        View,
        repository::{StoredView, ViewContext, ViewRepository},
    },
};
use ::mongodb::{
    Collection, Database,
    bson::{self, Bson, Document, doc},
};
use serde::Deserialize;
use std::{fmt, marker::PhantomData};

/// Stores serialized read-model views in a MongoDB collection, one document per view:
///
/// ```text
/// { _id: <view id>, version: <Int64>, last_sequence: <Int64>, view: <the view as BSON> }
/// ```
///
/// The collection is the view's name ([`View::NAME`]). `version` is the optimistic version (1 after the first
/// write) and `last_sequence` the last applied event of the view's aggregate (see `ViewContext`). A new document is
/// inserted, and a duplicate `_id` is a conflict; an existing one is replaced only at the version it was read with.
/// The queries read by `_id`: one document (`find_one`), every document and a range of ids (`find` sorted by `_id`,
/// the order of the ids' bytes), so the collection needs no index beyond `_id`. The application drops the
/// collection to wipe the read model.
///
/// ```no_run
/// # mod example {
/// # use cqrsx::{domain::{aggregate::Aggregate, command::Decide, event::{DomainEvent, EventEnvelope}}, view::View};
/// # use serde::{Deserialize, Serialize};
/// # #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// # pub struct Counted;
/// # impl DomainEvent for Counted {
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
/// #     type Error = Never;
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
/// #     fn update(&mut self, _: &EventEnvelope<Counter>) { self.count += 1; }
/// # }
/// # }
/// # use example::{Counter, Total};
/// use cqrsx::{view::projection::ViewProjection, mongodb::view_repository::MongoViewRepository};
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
    version: i64,
    last_sequence: i64,
    view: Bson,
}

fn stored_sequence(last_sequence: usize) -> Result<i64, PersistenceError> {
    i64::try_from(last_sequence).map_err(|error| PersistenceError::UnknownError(Box::new(error)))
}

fn applied_sequence(stored: i64) -> Result<usize, PersistenceError> {
    usize::try_from(stored).map_err(|error| PersistenceError::DeserializationError(Box::new(error)))
}

fn unreadable(error: bson::de::Error) -> PersistenceError {
    PersistenceError::DeserializationError(Box::new(error))
}

/// A stored document, decoded.
fn decode<V: View<A>, A: Aggregate>(document: Document) -> Result<StoredView<V>, PersistenceError> {
    let stored: Stored = bson::from_document(document).map_err(unreadable)?;
    let last_sequence = applied_sequence(stored.last_sequence)?;
    Ok(StoredView {
        id: stored.id,
        version: stored.version,
        last_sequence,
        view: bson::from_bson(stored.view).map_err(unreadable)?,
    })
}

impl<V: View<A>, A: Aggregate> MongoViewRepository<V, A> {
    /// The view with its version and last applied event.
    async fn stored(&self, view_id: &str) -> Result<Option<StoredView<V>>, PersistenceError> {
        let Some(document) = self.collection.find_one(doc! { "_id": view_id }).await.map_err(persistence)? else {
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
            .map_err(persistence)?;
        let mut documents = Vec::new();
        while cursor.advance().await.map_err(persistence)? {
            let document = cursor
                .deserialize_current()
                .map_err(|error| PersistenceError::DeserializationError(Box::new(error)))?;
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

    async fn load_with_context(&self, view_id: &str) -> Result<Option<StoredView<V>>, PersistenceError> {
        self.stored(view_id).await
    }

    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        let view = bson::to_bson(&view).map_err(|error| PersistenceError::UnknownError(Box::new(error)))?;
        let version = context
            .version
            .checked_add(1)
            .ok_or_else(|| PersistenceError::UnknownError("view version overflow".into()))?;
        let last_sequence = stored_sequence(context.last_sequence)?;
        if context.version == 0 {
            let document =
                doc! { "_id": &context.view_instance_id, "version": version, "last_sequence": last_sequence, "view": view };
            return match self.collection.insert_one(document).await {
                Ok(_) => Ok(()),
                Err(error) if is_duplicate_key(&error) => Err(PersistenceError::OptimisticLockError),
                Err(error) => Err(persistence(error)),
            };
        }
        let updated = self
            .collection
            .update_one(
                doc! { "_id": &context.view_instance_id, "version": context.version },
                doc! { "$set": { "view": view, "version": version, "last_sequence": last_sequence } },
            )
            .await
            .map_err(persistence)?;
        if updated.matched_count == 0 {
            return Err(PersistenceError::OptimisticLockError);
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
    use crate::domain::{aggregate::tests::Ledger, event::EventEnvelope};
    use serde::Serialize;

    /// A view BSON cannot hold: an unsigned value beyond `Int64`.
    #[derive(Debug, Default, Serialize, Deserialize)]
    struct Huge {
        value: u64,
    }

    impl View<Ledger> for Huge {
        const NAME: &'static str = "huge";
        fn update(&mut self, _: &EventEnvelope<Ledger>) {}
    }

    #[tokio::test]
    async fn an_unreachable_server_is_a_connection_error_and_a_view_bson_cannot_hold_is_unexpected() {
        let views = MongoViewRepository::<Huge, Ledger>::new(&crate::mongodb::test_support::unreachable().await);
        assert_eq!(format!("{views:?}"), r#"MongoViewRepository { collection: "huge", .. }"#);
        assert!(matches!(views.load("a").await, Err(PersistenceError::ConnectionError(_))));
        assert!(matches!(
            views.load_with_context("a").await,
            Err(PersistenceError::ConnectionError(_))
        ));
        for version in [0, 1] {
            assert!(matches!(
                views
                    .update_view(Huge::default(), ViewContext::new("a".into(), version, 1))
                    .await,
                Err(PersistenceError::ConnectionError(_))
            ));
        }
        // Refused before any statement.
        assert!(matches!(
            views
                .update_view(Huge { value: u64::MAX }, ViewContext::new("a".into(), 1, 2))
                .await,
            Err(PersistenceError::UnknownError(_))
        ));
        assert!(matches!(
            views
                .update_view(Huge::default(), ViewContext::new("a".into(), i64::MAX, 2))
                .await,
            Err(PersistenceError::UnknownError(_))
        ));
    }

    #[tokio::test]
    async fn an_unreachable_server_fails_a_listing_and_a_range_as_a_connection_error() {
        let views = MongoViewRepository::<Huge, Ledger>::new(&crate::mongodb::test_support::unreachable().await);
        assert!(matches!(views.list().await, Err(PersistenceError::ConnectionError(_))));
        assert!(matches!(
            views.range("fox:", "fox;").await,
            Err(PersistenceError::ConnectionError(_))
        ));
    }

    #[test]
    fn the_envelope_needs_the_version_the_last_applied_event_and_the_view() {
        let stored: Stored =
            bson::from_document(doc! { "_id": "c-1", "version": 3_i64, "last_sequence": 7_i64, "view": { "count": 7 } }).unwrap();
        assert_eq!((stored.version, stored.last_sequence), (3, 7));
        assert_eq!(stored.view, Bson::Document(doc! { "count": 7 }));
        for incomplete in [
            doc! { "version": 1_i64, "view": {} },
            doc! { "last_sequence": 1_i64, "view": {} },
            doc! { "version": 1_i64, "last_sequence": 1_i64 },
        ] {
            assert!(bson::from_document::<Stored>(incomplete).is_err());
        }
    }

    #[test]
    fn sequences_round_trip_and_values_out_of_range_are_explicit() {
        assert_eq!(stored_sequence(7).unwrap(), 7);
        assert_eq!(applied_sequence(7).unwrap(), 7);
        assert!(matches!(applied_sequence(-1), Err(PersistenceError::DeserializationError(_))));
        #[cfg(target_pointer_width = "64")]
        assert!(matches!(stored_sequence(usize::MAX), Err(PersistenceError::UnknownError(_))));
        let error = bson::from_document::<Stored>(doc! {}).unwrap_err();
        assert!(matches!(unreadable(error), PersistenceError::DeserializationError(_)));
    }
}
