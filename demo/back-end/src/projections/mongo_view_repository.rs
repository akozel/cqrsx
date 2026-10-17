//! `ViewRepository` over a MongoDB collection: `{ _id: view_id, version: i64, view: <V as BSON> }`.
//! Optimistic locking mirrors the library's `PostgresViewRepository`: `version == 0` inserts (a
//! duplicate key is a conflict), otherwise `update_one({ _id, version }, { $set: { view, version + 1 } })`
//! and `matched_count == 0` is a conflict. `ViewProjection` retries conflicts (up to 16 times).
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
use serde::Deserialize;
use std::marker::PhantomData;

pub struct MongoViewRepository<V, A> {
    collection: Collection<Document>,
    phantom: PhantomData<fn() -> (V, A)>,
}

#[derive(Deserialize)]
struct Stored {
    version: i64,
    view: Bson,
}

fn connection_error(error: mongodb::error::Error) -> PersistenceError {
    PersistenceError::ConnectionError(Box::new(error))
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

    /// Wipe = drop the collection. The ledger is untouched; Encore! rebuilds it.
    pub async fn wipe(&self) -> Result<(), PersistenceError> {
        self.collection.drop().await.map_err(connection_error)
    }

    /// Raw documents as stored (`_id`, `version`, `view`), for the read API.
    pub async fn documents(&self) -> Result<Vec<Document>, PersistenceError> {
        use futures::TryStreamExt;
        self.collection
            .find(doc! {})
            .sort(doc! { "_id": 1 })
            .await
            .map_err(connection_error)?
            .try_collect()
            .await
            .map_err(connection_error)
    }

    pub async fn document(&self, view_id: &str) -> Result<Option<Document>, PersistenceError> {
        self.collection
            .find_one(doc! { "_id": view_id })
            .await
            .map_err(connection_error)
    }

    async fn load_stored(&self, view_id: &str) -> Result<Option<Stored>, PersistenceError> {
        let Some(document) = self.document(view_id).await? else {
            return Ok(None);
        };
        let stored: Stored =
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
        let Some(stored) = self.load_stored(view_id).await? else {
            return Ok(None);
        };
        let view: V = mongodb::bson::from_bson(stored.view).map_err(|e| PersistenceError::DeserializationError(Box::new(e)))?;
        Ok(Some((view, ViewContext::new(view_id.to_owned(), stored.version))))
    }

    async fn update_view(&self, view: V, context: ViewContext) -> Result<(), PersistenceError> {
        let view = mongodb::bson::to_bson(&view).map_err(|e| PersistenceError::UnknownError(Box::new(e)))?;
        let next = context
            .version
            .checked_add(1)
            .ok_or_else(|| PersistenceError::UnknownError("view version overflow".into()))?;
        if context.version == 0 {
            let result = self
                .collection
                .insert_one(doc! { "_id": &context.view_instance_id, "version": next, "view": view })
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
                doc! { "$set": { "view": view, "version": next } },
            )
            .await
            .map_err(connection_error)?;
        if result.matched_count == 0 {
            return Err(PersistenceError::OptimisticLockError);
        }
        Ok(())
    }
}
