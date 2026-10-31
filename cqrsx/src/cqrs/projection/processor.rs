//! The projection processor: feeds a projection from the journal of its aggregate type instead of from the
//! commands that committed the events, so that no committed event is lost on the way to the read model.
//!
//! A processor owns one checkpoint row per projection and aggregate type ([`CheckpointStore`]) under a
//! lease, reads the journal in pages from the checkpoint ([`PersistedEventRepository::feed_after`]), hands
//! the events of every key to the projection through the key's worker (the ordered delivery, one worker per
//! busy key, batches per handler call) and moves the checkpoint behind the pages whose receipts are in. A
//! page may be read while the receipts of earlier pages are still in flight (a window as deep as the key
//! buffer's capacity less one); the checkpoint follows the completed prefix of the window, and inside a
//! failed page it stops before the transaction of the failed key. A failed round is recorded on the row and
//! followed by a pause that doubles up to a maximum. The processor registers its row before every claim, so a
//! row dropped together with the read side comes back at the start of the journal and the views are rebuilt.
//!
//! Rebuilds run through the same key workers: the owner rebuilds every key of the journal page by page when
//! a rebuild of the projection is requested ([`CheckpointStore::request_rebuild`], the cursor is kept on
//! the row), the keys requested one by one ([`CheckpointStore::request_rebuild_key`]), and a key whose
//! document refused a live event as a gap (healed in place, the page waits for the rebuild). A rebuild reads
//! the key's history, folds it and replaces the document ([`RebuildableProjection::rebuild`]); with a pace
//! the first event replaces the document and the rest apply one by one. Writes are bounded by the
//! processor's concurrency. The command executor wakes the processors of its aggregate type after every
//! commit ([`CommitWakeup`]); on another node the poll finds the commit.
//!
//! ```
//! # mod example {
//! # use cqrsx::{domain::{aggregate::Aggregate, command::Decide, event::{DomainEvent, EventEnvelope}}, cqrs::projection::View};
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
//! # pub struct Counted;
//! # impl DomainEvent for Counted {
//! #     fn event_type(&self) -> &'static str { "counted" }
//! #     fn event_version(&self) -> &'static str { "1" }
//! # }
//! # #[derive(Debug, thiserror::Error)]
//! # #[error("never")]
//! # pub struct Never;
//! # #[derive(Debug, Default, Clone, Serialize, Deserialize)]
//! # pub struct Counter;
//! # impl Aggregate for Counter {
//! #     const TYPE: &'static str = "counter";
//! #     type Id = String;
//! #     type Command = ();
//! #     type Event = Counted;
//! #     type Error = Never;
//! #     type Rules = ();
//! #     fn apply(&mut self, _: Counted) {}
//! # }
//! # impl Decide<Counter> for () {
//! #     fn decide(self, _: &Counter, _: &()) -> Result<Counted, Never> { Ok(Counted) }
//! # }
//! # #[derive(Debug, Default, Serialize, Deserialize)]
//! # pub struct Total { pub count: usize }
//! # impl View<Counter> for Total {
//! #     const NAME: &'static str = "totals";
//! #     fn update(&mut self, _: &EventEnvelope<Counter>) { self.count += 1; }
//! # }
//! # }
//! # use example::{Counted, Counter, Total};
//! # use cqrsx::domain::event::EventEnvelope;
//! use cqrsx::{
//!     cqrs::{
//!         projection::{
//!             ViewProjection,
//!             checkpoint::CheckpointStore,
//!             processor::{ProjectionProcessor, config::ProcessorConfig},
//!             repository::ViewRepository,
//!         },
//!         testing::{
//!             mem_checkpoint_store::MemCheckpointStore,
//!             mem_event_repository::MemEventRepository,
//!             mem_view_repository::MemViewRepository,
//!         },
//!     },
//!     journal::{repository::PersistedEventRepository, serialized_event::SerializedEvent},
//! };
//! use std::{sync::Arc, time::Duration};
//! use tokio_util::sync::CancellationToken;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let event = |sequence| SerializedEvent::try_from(&EventEnvelope::<Counter> {
//! #     aggregate_id: "c-1".into(), sequence, payload: Counted, metadata: Default::default() }).unwrap();
//! let journal = Arc::new(MemEventRepository::default());
//! let checkpoints = Arc::new(MemCheckpointStore::default());
//! let totals = Arc::new(MemViewRepository::<Total, Counter>::default());
//! journal.persist::<Counter>(&[event(1), event(2), event(3)], None).await?;
//!
//! let processor = ProjectionProcessor::new(
//!     "totals",
//!     journal.clone(),
//!     checkpoints.clone(),
//!     Arc::new(ViewProjection::new(totals.clone())),
//!     ProcessorConfig::default(),
//! );
//! let shutdown = CancellationToken::new();
//! let running = tokio::spawn(processor.run(shutdown.clone()));
//!
//! // The processor applies the journal in the background; the document and the checkpoint follow.
//! let mut total = None;
//! for _ in 0..100 {
//!     total = totals.load("c-1").await?.filter(|total| total.count == 3);
//!     if total.is_some() { break; }
//!     tokio::time::sleep(Duration::from_millis(10)).await;
//! }
//! assert_eq!(total.map(|total| total.count), Some(3));
//! shutdown.cancel();
//! running.await?;
//! let status = checkpoints.status(Some("totals")).await?;
//! assert_eq!(status[0].position, journal.head::<Counter>().await?);
//! assert_eq!(status[0].owner, None, "released on stop");
//! # Ok(()) }
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(example()).unwrap();
//! ```
pub mod config;
mod delivery;
mod lease;
pub mod observer;
mod owner;
mod rebuild;

use crate::{
    cqrs::{
        event_handler::EventHandler,
        projection::{
            RebuildableProjection,
            checkpoint::{CheckpointError, CheckpointKey, CheckpointStore, Claimed},
            processor::{
                config::ProcessorConfig,
                delivery::OrderedDelivery,
                observer::{LeaseEvent, ProjectionObserver},
                owner::{Exit, Owner},
                rebuild::{Bounded, HistoryRebuilder},
            },
        },
    },
    diagnostics,
    domain::aggregate::Aggregate,
    journal::{repository::PersistedEventRepository, upcaster::EventUpcaster, wakeup::CommitWakeup},
};
use std::{fmt, sync::Arc};
use tokio::{sync::Semaphore, time::Instant};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// What a processor was doing when it failed, as its log lines name it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Operation {
    /// A page of the journal.
    Feed,
    /// The journal's head, looked at while idle.
    Head,
    /// A statement of the checkpoint store.
    Checkpoints,
    /// A page delivered to the projection.
    Page,
    /// A rebuild of keys.
    Rebuild,
    /// An extension of the lease.
    Lease,
}

impl Operation {
    /// The operation's name in the log lines.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Feed => "feed",
            Self::Head => "head",
            Self::Checkpoints => "checkpoints",
            Self::Page => "page",
            Self::Rebuild => "rebuild",
            Self::Lease => "lease",
        }
    }
}

/// Feeds one projection from the journal of aggregate type `A`: see the [module](self) documentation.
/// Built once per projection and process, run until its shutdown token is cancelled.
#[must_use = "a processor feeds its projection only while `run` runs"]
pub struct ProjectionProcessor<A: Aggregate, R, C> {
    name: String,
    owner: String,
    repository: Arc<R>,
    checkpoints: Arc<C>,
    projection: Arc<dyn RebuildableProjection<A>>,
    upcasters: Vec<Box<dyn EventUpcaster>>,
    observer: Arc<dyn ProjectionObserver>,
    wakeup: CommitWakeup,
    config: ProcessorConfig,
}

// By hand: the journal, the checkpoint store, the projection and the observer need not be `Debug`.
impl<A: Aggregate, R, C> fmt::Debug for ProjectionProcessor<A, R, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectionProcessor")
            .field("name", &self.name)
            .field("aggregate_type", &A::TYPE)
            .field("owner", &self.owner)
            .field("upcasters", &self.upcasters.len())
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// The observer of a processor nobody observes.
struct NoObserver;

impl ProjectionObserver for NoObserver {}

impl<A, R, C> ProjectionProcessor<A, R, C>
where
    A: Aggregate + 'static,
    R: PersistedEventRepository + 'static,
    C: CheckpointStore + 'static,
{
    /// `name` names the projection on its checkpoint row (one row per name and aggregate type, so two
    /// projections over one collection need two names and two processors). `repository` is the journal,
    /// `checkpoints` the store of the row; `projection` handles the live events and rebuilds the keys.
    pub fn new(
        name: impl Into<String>,
        repository: Arc<R>,
        checkpoints: Arc<C>,
        projection: Arc<dyn RebuildableProjection<A>>,
        config: ProcessorConfig,
    ) -> Self {
        Self {
            name: name.into(),
            owner: Uuid::new_v4().to_string(),
            repository,
            checkpoints,
            projection,
            upcasters: Vec::new(),
            observer: Arc::new(NoObserver),
            wakeup: CommitWakeup::new(),
            config,
        }
    }

    /// Upcasters applied to every event read from the journal, live and on a rebuild, in order.
    pub fn with_upcasters(mut self, upcasters: Vec<Box<dyn EventUpcaster>>) -> Self {
        self.upcasters = upcasters;
        self
    }

    pub fn with_observer(mut self, observer: Arc<dyn ProjectionObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// The wake-up the aggregate type's command executor fires after a commit.
    pub fn with_wakeup(mut self, wakeup: CommitWakeup) -> Self {
        self.wakeup = wakeup;
        self
    }

    /// The name written to the row as its owner while this processor holds it (default: a fresh UUID).
    pub fn with_owner(mut self, owner: impl Into<String>) -> Self {
        self.owner = owner.into();
        self
    }

    #[cfg(test)]
    pub(crate) fn settings(&self) -> (&str, &ProcessorConfig) {
        (&self.owner, &self.config)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key(&self) -> CheckpointKey {
        CheckpointKey::new(self.name.clone(), A::TYPE)
    }

    /// Runs until `shutdown` is cancelled: registers the row and claims it when it is free, works while the
    /// lease holds and claims again after losing it. The registration repeats before every claim, so a row
    /// dropped with the read side comes back at the start of the journal and every document is rebuilt.
    /// Storage errors are logged and retried with a pause (every poll interval while waiting for the row,
    /// doubling while owning it); nothing is fatal. On shutdown the owner finishes what is in flight (up to
    /// one lease), moves the checkpoint and releases the row at once.
    pub async fn run(self, shutdown: CancellationToken) {
        let permits = Arc::new(Semaphore::new(self.config.concurrency().min(Semaphore::MAX_PERMITS)));
        let upcasters = Arc::new(self.upcasters);
        let rebuilder = Arc::new(HistoryRebuilder::new(
            self.repository.clone(),
            upcasters.clone(),
            self.projection.clone(),
            permits.clone(),
        ));
        let handler: Arc<dyn EventHandler<A>> = Arc::new(Bounded::new(self.projection.clone(), permits));
        let delivery = OrderedDelivery::new(&self.name, handler, rebuilder, self.config.delivery_limits());
        let context = Context {
            key: CheckpointKey::new(self.name, A::TYPE),
            owner: self.owner,
            handler: self.projection.name(),
            repository: self.repository,
            checkpoints: self.checkpoints,
            config: self.config,
            observer: self.observer,
            upcasters,
            delivery,
            wakeup: self.wakeup,
            shutdown,
        };
        context.run().await;
    }
}

/// What every ownership of the row shares.
pub(super) struct Context<A: Aggregate, R, C> {
    pub(super) key: CheckpointKey,
    pub(super) owner: String,
    /// The projection's name, for failure reports.
    pub(super) handler: String,
    pub(super) repository: Arc<R>,
    pub(super) checkpoints: Arc<C>,
    pub(super) config: ProcessorConfig,
    pub(super) observer: Arc<dyn ProjectionObserver>,
    pub(super) upcasters: Arc<Vec<Box<dyn EventUpcaster>>>,
    pub(super) delivery: OrderedDelivery<A>,
    pub(super) wakeup: CommitWakeup,
    pub(super) shutdown: CancellationToken,
}

impl<A, R, C> Context<A, R, C>
where
    A: Aggregate + 'static,
    R: PersistedEventRepository + 'static,
    C: CheckpointStore + 'static,
{
    async fn run(&self) {
        let (projection, aggregate_type) = (self.key.projection(), self.key.aggregate_type());
        loop {
            if self.shutdown.is_cancelled() {
                return;
            }
            match self.claim().await {
                Ok(Some((claimed, anchor))) => {
                    diagnostics::log_projection_event(projection, aggregate_type, "lease acquired");
                    self.observer.lease(LeaseEvent::Acquired);
                    match Owner::new(self, claimed, anchor).run().await {
                        Exit::Stopped => {
                            diagnostics::log_projection_event(projection, aggregate_type, "lease released");
                            self.observer.lease(LeaseEvent::Released);
                            return;
                        }
                        Exit::Lost => {
                            diagnostics::log_projection_event(projection, aggregate_type, "lease lost");
                            self.observer.lease(LeaseEvent::Lost);
                            continue; // the row may be free again at once
                        }
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    diagnostics::log_projection_failure(projection, aggregate_type, Operation::Checkpoints, error.category(), 0)
                }
            }
            tokio::select! {
                _ = self.shutdown.cancelled() => return,
                _ = tokio::time::sleep(self.config.poll_interval()) => {}
            }
        }
    }

    /// Registers the row and claims it when it is free; the moment before the claim anchors the lease. The
    /// registration repeats before every claim: a row dropped with the read side comes back at the start of
    /// the journal, and its next owner rebuilds every document from there.
    async fn claim(&self) -> Result<Option<(Claimed, Instant)>, CheckpointError> {
        self.checkpoints.register(&self.key).await?;
        let anchor = Instant::now();
        let claimed = self.checkpoints.claim(&self.key, &self.owner, self.config.lease()).await?;
        Ok(claimed.map(|claimed| (claimed, anchor)))
    }
}

#[cfg(test)]
mod tests {
    use super::Operation;

    #[test]
    fn operations_are_named_by_the_words_of_the_log_lines() {
        let named: Vec<_> = [
            Operation::Feed,
            Operation::Head,
            Operation::Checkpoints,
            Operation::Page,
            Operation::Rebuild,
            Operation::Lease,
        ]
        .map(Operation::as_str)
        .into();
        assert_eq!(named, ["feed", "head", "checkpoints", "page", "rebuild", "lease"]);
    }
}
