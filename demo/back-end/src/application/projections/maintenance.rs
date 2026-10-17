//! Maintenance of one read model over the whole ledger: wipe, rebuild by replay (Encore!) and
//! verify against a fresh rebuild. Replay is safe because it only calls `View::update`: no
//! commands, no tasks, no e-mails.
use crate::application::{
    StorageError,
    ports::{ReadModelStore, Stored},
    projections::progress::{ProgressCallback, ProgressHandler},
};
use async_trait::async_trait;
use cqrsx::cqrs::{
    aggregate::Aggregate,
    event_handler::replay::EventReplay,
    projection::{View, ViewProjection, repository::ViewRepository},
    store::repository::PersistedEventRepository,
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    sync::Arc,
    time::Duration,
};
use uuid::Uuid;

/// A document that differs between the live read model and a fresh rebuild.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Difference {
    pub collection: &'static str,
    pub id: String,
    /// `missing` (only in the rebuild), `unexpected` (only live) or `differs`.
    pub kind: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verification {
    pub checked: usize,
    pub differences: Vec<Difference>,
}

/// What the rebuild and verify operations need from every read model, whatever its view type.
#[async_trait]
pub trait MaintainedReadModel: Send + Sync {
    /// The collection name the API and the activity log report.
    fn name(&self) -> &'static str;
    /// The aggregate type whose events feed it.
    fn aggregate_type(&self) -> &'static str;
    async fn wipe(&self) -> Result<(), StorageError>;
    /// Replays every event of the aggregate type into the live read model; `delay` slows each
    /// event down (slow motion), `on_event` reports progress.
    async fn rebuild(&self, delay: Option<Duration>, on_event: Box<ProgressCallback>) -> Result<(), StorageError>;
    /// Rebuilds into a scratch copy with the very same projection and compares it with the live
    /// documents (the version is ignored). The scratch copy is always dropped.
    async fn verify(&self, run_id: Uuid) -> Result<Verification, StorageError>;
}

pub type HistoryFactory<H> = Arc<dyn Fn() -> H + Send + Sync>;
pub type ScratchFactory<R> = Arc<dyn Fn(Uuid) -> Arc<R> + Send + Sync>;

/// [`MaintainedReadModel`] for a view `V` of aggregate `A` stored in repositories of type `R`;
/// `H` reads the saved history.
pub struct ProjectionMaintenance<H, R, V, A> {
    name: &'static str,
    history: HistoryFactory<H>,
    live: Arc<R>,
    scratch: ScratchFactory<R>,
    phantom: PhantomData<fn() -> (V, A)>,
}

impl<H, R, V, A> ProjectionMaintenance<H, R, V, A> {
    pub fn new(name: &'static str, history: HistoryFactory<H>, live: Arc<R>, scratch: ScratchFactory<R>) -> Self {
        Self {
            name,
            history,
            live,
            scratch,
            phantom: PhantomData,
        }
    }
}

impl<H, R, V, A> ProjectionMaintenance<H, R, V, A>
where
    H: PersistedEventRepository + 'static,
    R: ViewRepository<V, A> + ReadModelStore<V> + 'static,
    V: View<A> + PartialEq + 'static,
    A: Aggregate + 'static,
{
    async fn replay_into(
        &self,
        repository: Arc<R>,
        delay: Option<Duration>,
        on_event: Box<ProgressCallback>,
    ) -> Result<(), StorageError> {
        EventReplay::new(
            (self.history)(),
            ProgressHandler::new(ViewProjection::new(repository), delay, on_event),
        )
        .replay_all()
        .await
        .map_err(|e| StorageError(format!("{} replay failed: {e}", A::TYPE)))
    }
}

fn by_id<V>(documents: Vec<Stored<V>>) -> BTreeMap<String, V> {
    documents.into_iter().map(|stored| (stored.id, stored.view)).collect()
}

#[async_trait]
impl<H, R, V, A> MaintainedReadModel for ProjectionMaintenance<H, R, V, A>
where
    H: PersistedEventRepository + 'static,
    R: ViewRepository<V, A> + ReadModelStore<V> + 'static,
    V: View<A> + PartialEq + 'static,
    A: Aggregate + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn aggregate_type(&self) -> &'static str {
        A::TYPE
    }

    async fn wipe(&self) -> Result<(), StorageError> {
        self.live.wipe().await
    }

    async fn rebuild(&self, delay: Option<Duration>, on_event: Box<ProgressCallback>) -> Result<(), StorageError> {
        self.replay_into(self.live.clone(), delay, on_event).await
    }

    async fn verify(&self, run_id: Uuid) -> Result<Verification, StorageError> {
        let scratch = (self.scratch)(run_id);
        scratch.wipe().await?;
        let replayed = self.replay_into(scratch.clone(), None, Box::new(|_, _, _| {})).await;
        let rebuilt = scratch.list().await;
        // Always clean up, even after a failure.
        let _ = scratch.wipe().await;
        replayed?;
        let expected = by_id(rebuilt?);
        let actual = by_id(self.live.list().await?);
        let ids: BTreeSet<&String> = expected.keys().chain(actual.keys()).collect();
        let differences = ids
            .into_iter()
            .filter_map(|id| {
                let kind = match (expected.get(id), actual.get(id)) {
                    (Some(e), Some(a)) if e == a => return None,
                    (Some(_), None) => "missing",
                    (None, Some(_)) => "unexpected",
                    _ => "differs",
                };
                Some(Difference {
                    collection: self.name,
                    id: id.clone(),
                    kind,
                })
            })
            .collect();
        Ok(Verification {
            checked: expected.len(),
            differences,
        })
    }
}
