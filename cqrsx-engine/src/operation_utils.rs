//! The operations of the feature `operation-utils`: what changes the read side or reads without a bound, and the
//! mechanism that runs tasks. Their criterion, and what stays public without the feature, is the API rule of the
//! engine in [`doc/components.md`](../../doc/components.md#naming-and-integration-rules).
use crate::{
    Cqrs, Journal,
    views::{JournalTotals, ViewQuery, Views, ViewsError},
};
use cqrsx::{
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::{AggregateStore, PersistenceError, feed::Position, store::EventSourcedStore},
    projection::checkpoint::RebuildReply,
    view::verify::Verification,
    worker::{adapter::boxed::BoxedTaskAdapter, dispatcher::TaskDispatcher},
};
#[cfg(feature = "postgres")]
use cqrsx_postgres::event_repository::PostgresEventRepository;
use std::{any::type_name, fmt::Display, time::Duration};

impl Cqrs {
    /// The task handlers of every module in one dispatcher, with their dependencies and the executors of every
    /// aggregate type; `None` without a task type.
    pub fn dispatcher(&self) -> Option<&TaskDispatcher<BoxedTaskAdapter>> {
        self.dispatcher.as_deref()
    }

    /// The committed events of the aggregate `id` of type `A`, from its journal alone, without a snapshot (on
    /// PostgreSQL through the pool of whole-history reads, without a statement timeout).
    pub async fn history<A: Aggregate>(&self, id: &A::Id) -> Result<Vec<EventEnvelope<A>>, PersistenceError> {
        let id = id.to_string();
        match &self.journal {
            #[cfg(feature = "test-utils")]
            Journal::Memory => {
                EventSourcedStore::<_, A>::new(self.memory.journal.clone())
                    .load_events(&id)
                    .await
            }
            #[cfg(feature = "postgres")]
            Journal::Postgres(pools) => {
                EventSourcedStore::<_, A>::new(PostgresEventRepository::new(pools.history.clone()))
                    .load_events(&id)
                    .await
            }
        }
    }
}

impl<V: 'static> ViewQuery<'_, V> {
    /// Asks the processor that owns the view's checkpoint, on any node, to rebuild the document of the aggregate `id`
    /// (its `Display`) from its history; live delivery goes on meanwhile. Returns once the request is recorded, not
    /// when the document is replaced. [`ViewsError::NotStarted`] when the processor has not registered its checkpoint
    /// yet (the request can be repeated later); [`ViewsError::Unknown`] when the engine does not have the view.
    pub async fn rebuild(&self, id: &impl Display) -> Result<(), ViewsError> {
        let queries = self.queries.ok_or_else(|| ViewsError::Unknown(type_name::<V>().to_owned()))?;
        Ok(queries
            .checkpoints()
            .request_rebuild_key(&queries.key(), &id.to_string())
            .await?)
    }
}

impl Views<'_> {
    /// Asks the processor of the view `name` to rebuild every document, `pace` between the events of a key.
    pub async fn request_rebuild(&self, name: &str, pace: Duration) -> Result<RebuildReply, ViewsError> {
        let view = self.cqrs.views.named(name)?;
        Ok(view.checkpoints().request_rebuild(&view.key(), pace).await?)
    }

    /// Asks the processor of the view `name` to rebuild the document of the aggregate `id`.
    pub async fn request_rebuild_key(&self, name: &str, id: &str) -> Result<(), ViewsError> {
        let view = self.cqrs.views.named(name)?;
        Ok(view.checkpoints().request_rebuild_key(&view.key(), id).await?)
    }

    /// Every document of the view `name` against the fold of the part of its history it applied (D-20).
    pub async fn verify(&self, name: &str) -> Result<Verification, ViewsError> {
        Ok(self.cqrs.views.named(name)?.verify().await?)
    }

    /// The events of `aggregate_type` above `after`, counted up to `cap`: how far a view of the type whose checkpoint
    /// stands at `after` is behind.
    pub async fn count_after(&self, aggregate_type: &str, after: Position, cap: u64) -> Result<u64, ViewsError> {
        let views = &self.cqrs.views.operations;
        let view = views.iter().find(|view| view.aggregate_type() == aggregate_type);
        let view = view.ok_or_else(|| ViewsError::UnknownAggregateType(aggregate_type.to_owned()))?;
        Ok(view.count_after(after, cap).await?)
    }

    /// The size of the journals of the views' aggregate types: their events and aggregates, each type once.
    pub async fn totals(&self) -> Result<JournalTotals, ViewsError> {
        let mut counted = Vec::new();
        let mut totals = JournalTotals::default();
        for view in &self.cqrs.views.operations {
            let aggregate_type = view.aggregate_type();
            if counted.contains(&aggregate_type) {
                continue;
            }
            counted.push(aggregate_type);
            let of_type = view.totals().await?;
            totals.events += of_type.events;
            totals.aggregates += of_type.aggregates;
        }
        Ok(totals)
    }
}
