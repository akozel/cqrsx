//! The operations of the feature `operation-utils`: what changes the read side or reads without a bound, and the
//! mechanism that runs tasks. Their criterion, and what stays public without the feature, is the API rule of the
//! framework in [`doc/components.md`](../../doc/components.md#naming-and-integration-rules).
use crate::{
    Cqrsx,
    views::{Answer, CheckpointAnswer, JournalTotals, Registry, ViewQuery, Views, ViewsError},
};
use cqrsx::{
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::{PersistenceError, feed::Position, repository::EventRepository},
    projection::checkpoint::{CheckpointKey, CheckpointStore, RebuildReply},
    view::{
        View,
        repository::ViewRepository,
        verify::{Verification, verify},
    },
    worker::{adapter::boxed::BoxedTaskAdapter, dispatcher::TaskDispatcher},
};
use std::{
    any::{TypeId, type_name},
    fmt::Display,
    future::Future,
    marker::PhantomData,
    sync::Arc,
    time::Duration,
};

/// What a journal counts of one aggregate type beyond [`Counts`](crate::views::Counts): its size.
pub(crate) trait Totals: Send + Sync {
    fn totals<A: Aggregate>(&self) -> impl Future<Output = Result<JournalTotals, PersistenceError>> + Send;
}

/// The maintenance of one view, its types erased: the operations that change its read side or read its journal
/// without a bound.
pub(crate) trait Maintenance: Send + Sync {
    fn name(&self) -> &'static str;
    fn aggregate_type(&self) -> &'static str;
    fn request_rebuild<'a>(&'a self, pace: Duration) -> CheckpointAnswer<'a, RebuildReply>;
    fn request_rebuild_key<'a>(&'a self, id: &'a str) -> CheckpointAnswer<'a, ()>;
    fn verify(&self) -> Answer<'_, Verification>;
    fn totals(&self) -> Answer<'_, JournalTotals>;
}

/// A view's maintenance: its documents, the checkpoints of its processor and the journal of whole histories its
/// documents are verified against and its size counted on (`history`).
pub(crate) struct Maintained<V, A, D, C, H> {
    documents: Arc<D>,
    checkpoints: Arc<C>,
    history: H,
    types: PhantomData<fn() -> (V, A)>,
}

impl<V, A, D, C, H> Maintained<V, A, D, C, H> {
    pub(crate) fn new(documents: Arc<D>, checkpoints: Arc<C>, history: H) -> Self {
        Self {
            documents,
            checkpoints,
            history,
            types: PhantomData,
        }
    }
}

impl<V, A, D, C, H> Maintenance for Maintained<V, A, D, C, H>
where
    V: View<A> + PartialEq + 'static,
    A: Aggregate + 'static,
    D: ViewRepository<V, A> + 'static,
    C: CheckpointStore + 'static,
    H: EventRepository + Totals + 'static,
{
    fn name(&self) -> &'static str {
        V::NAME
    }

    fn aggregate_type(&self) -> &'static str {
        A::TYPE
    }

    fn request_rebuild<'a>(&'a self, pace: Duration) -> CheckpointAnswer<'a, RebuildReply> {
        Box::pin(async move {
            let key = CheckpointKey::of::<V, A>();
            CheckpointStore::request_rebuild(&*self.checkpoints, &key, pace).await
        })
    }

    fn request_rebuild_key<'a>(&'a self, id: &'a str) -> CheckpointAnswer<'a, ()> {
        Box::pin(async move {
            let key = CheckpointKey::of::<V, A>();
            CheckpointStore::request_rebuild_key(&*self.checkpoints, &key, id).await
        })
    }

    fn verify(&self) -> Answer<'_, Verification> {
        Box::pin(verify::<V, A>(&self.history, &*self.documents))
    }

    fn totals(&self) -> Answer<'_, JournalTotals> {
        Box::pin(self.history.totals::<A>())
    }
}

impl Registry {
    /// The maintenance of the view `name`.
    fn maintained(&self, name: &str) -> Result<&dyn Maintenance, ViewsError> {
        let view = self
            .maintenance
            .iter()
            .map(|(_, view)| &**view)
            .find(|view| view.name() == name);
        view.ok_or_else(|| ViewsError::Unknown(name.to_owned()))
    }

    /// The maintenance of the view `V`.
    fn maintained_of<V: 'static>(&self) -> Result<&dyn Maintenance, ViewsError> {
        let view = self.maintenance.iter().find(|(id, _)| *id == TypeId::of::<V>());
        view.map(|(_, view)| &**view)
            .ok_or_else(|| ViewsError::Unknown(type_name::<V>().to_owned()))
    }
}

impl Cqrsx {
    /// The task handlers of every module in one dispatcher, with their dependencies and the executors of every
    /// aggregate type; `None` without a task type.
    pub fn dispatcher(&self) -> Option<&TaskDispatcher<BoxedTaskAdapter>> {
        self.dispatcher.as_deref()
    }

    /// The committed events of the aggregate `id` of type `A`, from its journal alone, without a snapshot (on
    /// PostgreSQL through the pool of whole-history reads, without a statement timeout).
    pub async fn history<A: Aggregate>(&self, id: &A::Id) -> Result<Vec<EventEnvelope<A>>, PersistenceError> {
        self.journal.history::<A>(&id.to_string()).await
    }
}

impl<V: 'static> ViewQuery<'_, V> {
    /// Asks the processor that owns the view's checkpoint, on any node, to rebuild the document of the aggregate `id`
    /// (its `Display`) from its history; live delivery goes on meanwhile. Returns once the request is recorded, not
    /// when the document is replaced. [`ViewsError::NotStarted`] when the processor has not registered its checkpoint
    /// yet (the request can be repeated later); [`ViewsError::Unknown`] when the framework does not have the view.
    pub async fn rebuild(&self, id: &impl Display) -> Result<(), ViewsError> {
        let view = self.registry.maintained_of::<V>()?;
        Ok(view.request_rebuild_key(&id.to_string()).await?)
    }
}

impl Views<'_> {
    /// Asks the processor of the view `name` to rebuild every document, `pace` between the events of a key.
    pub async fn request_rebuild(&self, name: &str, pace: Duration) -> Result<RebuildReply, ViewsError> {
        Ok(self.cqrsx.views.maintained(name)?.request_rebuild(pace).await?)
    }

    /// Asks the processor of the view `name` to rebuild the document of the aggregate `id`.
    pub async fn request_rebuild_key(&self, name: &str, id: &str) -> Result<(), ViewsError> {
        Ok(self.cqrsx.views.maintained(name)?.request_rebuild_key(id).await?)
    }

    /// Every document of the view `name` against the fold of the part of its history it applied (D-20).
    pub async fn verify(&self, name: &str) -> Result<Verification, ViewsError> {
        Ok(self.cqrsx.views.maintained(name)?.verify().await?)
    }

    /// The events of `aggregate_type` above `after`, counted up to `cap`: how far a view of the type whose checkpoint
    /// stands at `after` is behind.
    pub async fn count_after(&self, aggregate_type: &str, after: Position, cap: u64) -> Result<u64, ViewsError> {
        let views = &self.cqrsx.views.operations;
        let view = views.iter().find(|view| view.aggregate_type() == aggregate_type);
        let view = view.ok_or_else(|| ViewsError::UnknownAggregateType(aggregate_type.to_owned()))?;
        Ok(view.count_after(after, cap).await?)
    }

    /// The size of the journals of the views' aggregate types: their events and aggregates, each type once.
    pub async fn totals(&self) -> Result<JournalTotals, ViewsError> {
        let mut counted = Vec::new();
        let mut totals = JournalTotals::default();
        for (_, view) in &self.cqrsx.views.maintenance {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    #[tokio::test]
    async fn views_verify_and_take_rebuild_requests() {
        let (cqrsx, _) = framework().await;
        book_every(&cqrsx, &[("s-1", "A1", "fox"), ("s-1", "A2", "panda"), ("s-2", "A1", "fox")]).await;
        let views = cqrsx.views();
        cqrsx.catch_up().await;
        let verified = views.verify("halls").await.unwrap();
        assert_eq!((verified.checked, verified.differences.len()), (2, 0));
        assert_eq!(
            views.request_rebuild("halls", Duration::ZERO).await.unwrap(),
            RebuildReply::Accepted
        );
        assert_eq!(
            views.request_rebuild("halls", Duration::ZERO).await.unwrap(),
            RebuildReply::AlreadyRunning
        );
        views.request_rebuild_key("halls", "s-1").await.unwrap();
        while views.status().await.unwrap()[0].rebuilt < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            views.verify("halls").await.unwrap().differences.len(),
            0,
            "the rebuild folds the history"
        );
        for refused in [
            views.request_rebuild("seats", Duration::ZERO).await.map(|_| ()),
            views.request_rebuild_key("seats", "s-1").await,
            views.verify("seats").await.map(|_| ()),
        ] {
            assert_eq!(refused.unwrap_err().to_string(), "the view `seats` is not registered");
        }
    }

    #[tokio::test]
    async fn a_view_rebuilds_the_document_of_one_aggregate_on_request() {
        let (cqrsx, _) = framework().await;
        book_every(&cqrsx, &[("s-1", "A1", "fox"), ("s-2", "B2", "panda")]).await;
        let halls = cqrsx.view::<Hall>();
        let early = halls.rebuild(&"s-1").await.unwrap_err();
        assert!(matches!(early, ViewsError::NotStarted), "{early:?}");
        assert_eq!(
            early.to_string(),
            "the view's processor has not started: its checkpoint is not registered yet"
        );
        let by_name = [
            cqrsx.views().request_rebuild("halls", Duration::ZERO).await.map(|_| ()),
            cqrsx.views().request_rebuild_key("halls", "s-1").await,
        ];
        assert!(
            by_name.iter().all(|result| matches!(result, Err(ViewsError::NotStarted))),
            "{by_name:?}"
        );
        let unknown = cqrsx.view::<String>().rebuild(&"s-1").await.unwrap_err();
        assert_eq!(unknown.to_string(), "the view `alloc::string::String` is not registered");

        cqrsx.catch_up().await;
        let revision = async |id: &str| halls.get(&id).await.unwrap().unwrap().revision;
        let (first, second) = (revision("s-1").await, revision("s-2").await);
        halls.rebuild(&String::from("s-1")).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while revision("s-1").await == first {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the document is rebuilt");
        assert_eq!(revision("s-2").await, second, "the other document is not touched");
        assert_eq!(cqrsx.views().verify("halls").await.unwrap().differences.len(), 0);
    }

    #[tokio::test]
    async fn the_views_count_their_journal_and_its_size() {
        let (cqrsx, _) = framework().await;
        book_every(&cqrsx, &[("s-1", "A1", "fox"), ("s-1", "A2", "panda"), ("s-2", "A1", "fox")]).await;
        let views = cqrsx.views();
        assert_eq!(views.count_after("show", Position::START, 10).await.unwrap(), 3);
        assert_eq!(
            views.count_after("show", Position::START, 2).await.unwrap(),
            2,
            "up to the cap"
        );
        assert_eq!(views.count_after("wallet", Position::START, 10).await.unwrap(), 0);
        assert_eq!(
            views.count_after("seat", Position::START, 10).await.unwrap_err().to_string(),
            "no view of the aggregate type `seat` is registered"
        );
        assert_eq!(
            views.totals().await.unwrap(),
            JournalTotals {
                events: 3,
                aggregates: 2
            },
            "the shows, the wallets have no event yet"
        );
        cqrsx.run_tasks().await;
        assert_eq!(
            views.totals().await.unwrap(),
            JournalTotals {
                events: 5,
                aggregates: 4
            },
            "every aggregate type of a view once: fox's second A1 is recorded already"
        );
    }
}
