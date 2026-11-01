//! A harness for the processor's fault model: in-memory stores behind wrappers with gates (a call blocks
//! until the test lets it through), scripted failures and lost replies, a recording observer, and nodes
//! that can be stopped or crashed. A crash is `JoinHandle::abort`: the key lanes of the crashed node
//! finish their current job, like a slow owner that lost its lease, so every check holds for both outcomes.
use crate::support::ledger::{Command, Delta, Ledger, Services};
use cqrsx::{
    command::{executor::StoreCommandExecutor, locks::LockLimits, retry::AggregateConflictRetryPolicy},
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::{
        EventStore, PersistenceError,
        feed::{FeedEntry, Position},
        repository::EventRepository,
        serialized_event::{EventFormat, SerializedEvent},
        snapshot::SerializedSnapshot,
        store::JournalEventStore,
        wakeup::CommitWakeup,
    },
    memory::{
        checkpoint_store::MemoryCheckpointStore, event_repository::MemoryEventRepository, view_repository::MemoryViewRepository,
    },
    projection::{
        checkpoint::{
            CheckpointError, CheckpointKey, CheckpointStatus, CheckpointStore, Claimed, LeaseToken, PendingRebuilds, RebuildReply,
        },
        processor::{
            ProjectionProcessor,
            config::{ProcessorConfig, ProcessorConfigBuilder},
            observer::{Advanced, Applied, Failed, LeaseChange, ProjectionObserver, RebuildPhase, Rebuilt, Waiting},
        },
    },
    view::{
        View,
        projection::ViewProjection,
        repository::{StoredView, ViewMeta, ViewRepository},
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub(crate) const NAME: &str = "balances";
pub(crate) const LEASE: Duration = Duration::from_secs(1);
pub(crate) const POLL: Duration = Duration::from_millis(250);
pub(crate) const RETRY_BASE: Duration = Duration::from_millis(500);

/// The read model: the running total and every applied event, so a double application shows.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Balance {
    pub(crate) total: i64,
    pub(crate) history: Vec<(usize, i32)>,
}

impl View<Ledger> for Balance {
    const NAME: &'static str = NAME;
    fn apply(&mut self, event: &EventEnvelope<Ledger>) {
        self.total += i64::from(event.payload.0);
        self.history.push((event.sequence, event.payload.0));
    }
}

pub(crate) fn key() -> CheckpointKey {
    CheckpointKey::new(NAME, Ledger::TYPE)
}

/// A one-shot gate: the wrapped call reports that it reached the gate and waits until the test releases it;
/// after the release every call passes at once. A gate for the first call only lets every later call pass.
pub(crate) struct Gate {
    reached: watch::Sender<bool>,
    released: watch::Sender<bool>,
    first_only: bool,
}

impl Gate {
    fn new(first_only: bool) -> Arc<Self> {
        Arc::new(Self {
            reached: watch::Sender::new(false),
            released: watch::Sender::new(false),
            first_only,
        })
    }

    async fn wait(sender: &watch::Sender<bool>) {
        let mut receiver = sender.subscribe();
        while !*receiver.borrow_and_update() {
            receiver.changed().await.expect("the gate outlives its waiters");
        }
    }

    /// Resolves once a call has reached the gate.
    pub(crate) async fn reached(&self) {
        Self::wait(&self.reached).await;
    }

    pub(crate) fn release(&self) {
        self.released.send_replace(true);
    }

    async fn pass(&self) {
        if self.first_only && self.reached.send_replace(true) {
            return; // somebody reached it before: the gate holds only the first call
        }
        self.reached.send_replace(true);
        Self::wait(&self.released).await;
    }
}

/// Scripted faults shared by the wrappers: gates, failures that count down and replies that get lost.
#[derive(Default)]
struct Faults {
    gates: HashMap<String, Arc<Gate>>,
    failures: HashMap<String, usize>,
    lost_replies: HashMap<String, usize>,
}

impl Faults {
    fn gate(&mut self, name: &str) -> Arc<Gate> {
        self.gate_kind(name, false)
    }

    fn gate_kind(&mut self, name: &str, first_only: bool) -> Arc<Gate> {
        let gate = Gate::new(first_only);
        self.gates.insert(name.to_owned(), gate.clone());
        gate
    }

    fn gate_for(&self, name: &str) -> Option<Arc<Gate>> {
        self.gates.get(name).or_else(|| self.gates.get("*")).cloned()
    }

    /// Consumes one scripted failure of `name` (or of `*`).
    fn take(table: &mut HashMap<String, usize>, name: &str) -> bool {
        for candidate in [name, "*"] {
            if let Some(left) = table.get_mut(candidate)
                && *left > 0
            {
                *left -= 1;
                return true;
            }
        }
        false
    }
}

fn storage_error() -> CheckpointError {
    CheckpointError::Storage("the test asked the checkpoint store to fail".into())
}

fn connection_error() -> PersistenceError {
    PersistenceError::Connection("the test asked the store to fail".into())
}

/// The view repository with gates on a key's writes, failures per key, one-shot panics and a count of the
/// writes in flight at once.
pub(crate) struct Views {
    inner: MemoryViewRepository<Balance, Ledger>,
    faults: Mutex<Faults>,
    panics: Mutex<HashSet<String>>,
    in_flight: AtomicUsize,
    most_in_flight: AtomicUsize,
    writes: AtomicUsize,
}

impl Views {
    fn new(inner: MemoryViewRepository<Balance, Ledger>) -> Self {
        Self {
            inner,
            faults: Mutex::default(),
            panics: Mutex::default(),
            in_flight: AtomicUsize::new(0),
            most_in_flight: AtomicUsize::new(0),
            writes: AtomicUsize::new(0),
        }
    }

    /// The next writes of `key` (`*`: of any key) block until the gate is released.
    pub(crate) fn gate(&self, key: &str) -> Arc<Gate> {
        self.faults.lock().unwrap().gate(key)
    }

    /// Only the next write of `key` blocks until the gate is released; the writes after it pass.
    pub(crate) fn gate_first(&self, key: &str) -> Arc<Gate> {
        self.faults.lock().unwrap().gate_kind(key, true)
    }

    /// The next `times` writes of `key` (`*`: of any key) fail with a connection error.
    pub(crate) fn fail_next(&self, key: &str, times: usize) {
        self.faults.lock().unwrap().failures.insert(key.to_owned(), times);
    }

    /// The next write of `key` panics.
    pub(crate) fn panic_once(&self, key: &str) {
        self.panics.lock().unwrap().insert(key.to_owned());
    }

    /// Drops the document of `key`, as the demo's wipe does.
    pub(crate) async fn delete(&self, key: &str) {
        let kept: Vec<_> = self
            .inner
            .list()
            .await
            .unwrap()
            .into_iter()
            .filter(|stored| stored.id != key)
            .collect();
        self.inner.clear();
        for stored in kept {
            self.inner
                .put(&stored.id, stored.revision, stored.last_sequence, &stored.view)
                .unwrap();
        }
    }

    /// Drops every document, as dropping the collection does.
    pub(crate) fn clear(&self) {
        self.inner.clear();
    }

    pub(crate) fn most_in_flight(&self) -> usize {
        self.most_in_flight.load(Ordering::SeqCst)
    }

    pub(crate) fn writes(&self) -> usize {
        self.writes.load(Ordering::SeqCst)
    }
}

impl ViewRepository<Balance, Ledger> for Views {
    async fn load(&self, view_id: &str) -> Result<Option<Balance>, PersistenceError> {
        self.inner.load(view_id).await
    }

    async fn load_stored(&self, view_id: &str) -> Result<Option<StoredView<Balance>>, PersistenceError> {
        self.inner.load_stored(view_id).await
    }

    async fn update_view(&self, view: Balance, context: ViewMeta) -> Result<(), PersistenceError> {
        let key = context.id.clone();
        if self.panics.lock().unwrap().remove(&key) {
            panic!("the test asked the view store to panic");
        }
        let (gate, fail) = {
            let mut faults = self.faults.lock().unwrap();
            let fail = Faults::take(&mut faults.failures, &key);
            (faults.gate_for(&key), fail)
        };
        if fail {
            return Err(connection_error());
        }
        // Counted from the gate on: the processor's concurrency bounds the writes that get this far at once.
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.most_in_flight.fetch_max(now, Ordering::SeqCst);
        if let Some(gate) = gate {
            gate.pass().await;
        }
        self.writes.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.update_view(view, context).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        result
    }

    async fn list(&self) -> Result<Vec<StoredView<Balance>>, PersistenceError> {
        self.inner.list().await
    }

    async fn range(&self, lower: &str, upper: &str) -> Result<Vec<StoredView<Balance>>, PersistenceError> {
        self.inner.range(lower, upper).await
    }
}

/// The checkpoint store with gates before and after an operation, failures that count down, replies that
/// get lost after the effect and an `extend` that lies.
pub(crate) struct Checkpoints {
    pub(crate) inner: MemoryCheckpointStore,
    before: Mutex<Faults>,
    after: Mutex<Faults>,
    extend_lies: AtomicBool,
    calls: Mutex<Vec<&'static str>>,
}

impl Checkpoints {
    fn new(inner: MemoryCheckpointStore) -> Self {
        Self {
            inner,
            before: Mutex::default(),
            after: Mutex::default(),
            extend_lies: AtomicBool::new(false),
            calls: Mutex::default(),
        }
    }

    /// The next `operation` blocks before it runs until the gate is released: a crash there leaves no effect.
    pub(crate) fn gate_before(&self, operation: &str) -> Arc<Gate> {
        self.before.lock().unwrap().gate(operation)
    }

    /// The next `operation` blocks after it ran until the gate is released: a crash there keeps the effect.
    pub(crate) fn gate_after(&self, operation: &str) -> Arc<Gate> {
        self.after.lock().unwrap().gate(operation)
    }

    /// The next `times` calls of `operation` fail before they run.
    pub(crate) fn fail(&self, operation: &str, times: usize) {
        self.before.lock().unwrap().failures.insert(operation.to_owned(), times);
    }

    /// The next `times` calls of `operation` run and then fail: the effect is there, the reply is lost.
    pub(crate) fn reply_lost(&self, operation: &str, times: usize) {
        self.after.lock().unwrap().lost_replies.insert(operation.to_owned(), times);
    }

    /// `extend` answers `true` without extending: the owner believes in a lease the store lets expire.
    pub(crate) fn extend_lies(&self, lies: bool) {
        self.extend_lies.store(lies, Ordering::SeqCst);
    }

    pub(crate) fn calls(&self, operation: &str) -> usize {
        self.calls.lock().unwrap().iter().filter(|call| **call == operation).count()
    }

    async fn run<T>(
        &self,
        operation: &'static str,
        call: impl Future<Output = Result<T, CheckpointError>>,
    ) -> Result<T, CheckpointError> {
        self.calls.lock().unwrap().push(operation);
        let (gate, fail) = {
            let mut before = self.before.lock().unwrap();
            let fail = Faults::take(&mut before.failures, operation);
            (before.gate_for(operation), fail)
        };
        if let Some(gate) = gate {
            gate.pass().await;
        }
        if fail {
            return Err(storage_error());
        }
        let result = call.await?;
        let (gate, lost) = {
            let mut after = self.after.lock().unwrap();
            let lost = Faults::take(&mut after.lost_replies, operation);
            (after.gate_for(operation), lost)
        };
        if let Some(gate) = gate {
            gate.pass().await;
        }
        if lost {
            return Err(storage_error());
        }
        Ok(result)
    }
}

impl CheckpointStore for Checkpoints {
    async fn register(&self, key: &CheckpointKey) -> Result<(), CheckpointError> {
        self.run("register", self.inner.register(key)).await
    }

    async fn claim(&self, key: &CheckpointKey, owner: &str, lease: Duration) -> Result<Option<Claimed>, CheckpointError> {
        self.run("claim", self.inner.claim(key, owner, lease)).await
    }

    async fn extend(&self, key: &CheckpointKey, token: LeaseToken, lease: Duration) -> Result<bool, CheckpointError> {
        if self.extend_lies.load(Ordering::SeqCst) {
            return Ok(true);
        }
        self.run("extend", self.inner.extend(key, token, lease)).await
    }

    async fn advance(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        to: Position,
        delivered: u64,
    ) -> Result<bool, CheckpointError> {
        self.run("advance", self.inner.advance(key, token, to, delivered)).await
    }

    async fn record_failure(&self, key: &CheckpointKey, token: LeaseToken, error: &str) -> Result<bool, CheckpointError> {
        self.run("record_failure", self.inner.record_failure(key, token, error)).await
    }

    async fn request_rebuild(&self, key: &CheckpointKey, pace: Duration) -> Result<RebuildReply, CheckpointError> {
        self.run("request_rebuild", self.inner.request_rebuild(key, pace)).await
    }

    async fn request_rebuild_key(&self, key: &CheckpointKey, aggregate_id: &str) -> Result<(), CheckpointError> {
        self.run("request_rebuild_key", self.inner.request_rebuild_key(key, aggregate_id))
            .await
    }

    async fn take_rebuild_requests(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        limit: usize,
    ) -> Result<Option<PendingRebuilds>, CheckpointError> {
        self.run("take_rebuild_requests", self.inner.take_rebuild_requests(key, token, limit))
            .await
    }

    async fn begin_rebuild(&self, key: &CheckpointKey, token: LeaseToken) -> Result<bool, CheckpointError> {
        self.run("begin_rebuild", self.inner.begin_rebuild(key, token)).await
    }

    async fn advance_rebuild(
        &self,
        key: &CheckpointKey,
        token: LeaseToken,
        cursor: &str,
        keys: u64,
    ) -> Result<bool, CheckpointError> {
        self.run("advance_rebuild", self.inner.advance_rebuild(key, token, cursor, keys))
            .await
    }

    async fn finish_rebuild(&self, key: &CheckpointKey, token: LeaseToken) -> Result<bool, CheckpointError> {
        self.run("finish_rebuild", self.inner.finish_rebuild(key, token)).await
    }

    async fn release(&self, key: &CheckpointKey, token: LeaseToken) -> Result<(), CheckpointError> {
        self.run("release", self.inner.release(key, token)).await
    }

    async fn status(&self, projection: Option<&str>) -> Result<Vec<CheckpointStatus>, CheckpointError> {
        self.inner.status(projection).await
    }
}

/// The journal with a gate and failures on the feed and the head, and a count of the pages read.
pub(crate) struct Journal {
    pub(crate) inner: MemoryEventRepository,
    faults: Mutex<Faults>,
    feeds: AtomicUsize,
    /// Added to the head: a slower transaction with a lower id that the pages cannot see yet.
    horizon: AtomicUsize,
}

impl Journal {
    fn new(inner: MemoryEventRepository) -> Self {
        Self {
            inner,
            faults: Mutex::default(),
            feeds: AtomicUsize::new(0),
            horizon: AtomicUsize::new(0),
        }
    }

    /// The pages read from the feed so far, empty ones included.
    pub(crate) fn feed_reads(&self) -> usize {
        self.feeds.load(Ordering::SeqCst)
    }

    /// Makes the head stand `above` positions over what the pages deliver, as an open transaction would.
    pub(crate) fn hold_horizon(&self, above: u64) {
        self.horizon.store(above as usize, Ordering::SeqCst);
    }

    /// The next `feed_after` blocks before it reads until the gate is released.
    pub(crate) fn gate_feed(&self) -> Arc<Gate> {
        self.faults.lock().unwrap().gate("feed")
    }

    /// The next history read returns only when the gate is released: the reader holds a history that may
    /// be stale by then.
    pub(crate) fn gate_after_history(&self) -> Arc<Gate> {
        self.faults.lock().unwrap().gate_kind("history-read", true)
    }

    /// The next `times` calls of `operation` (`feed`, `head`, `ids`, `history`) fail with a connection error.
    pub(crate) fn fail(&self, operation: &str, times: usize) {
        self.faults.lock().unwrap().failures.insert(operation.to_owned(), times);
    }

    async fn fault(&self, operation: &str) -> Result<(), PersistenceError> {
        let (gate, fail) = {
            let mut faults = self.faults.lock().unwrap();
            let fail = Faults::take(&mut faults.failures, operation);
            (faults.gates.get(operation).cloned(), fail)
        };
        if let Some(gate) = gate {
            gate.pass().await;
        }
        if fail { Err(connection_error()) } else { Ok(()) }
    }
}

impl EventRepository for Journal {
    async fn events<A: Aggregate>(&self, aggregate_id: &str) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.fault("history").await?;
        let history = self.inner.events::<A>(aggregate_id).await;
        let gate = self.faults.lock().unwrap().gates.get("history-read").cloned();
        if let Some(gate) = gate {
            gate.pass().await;
        }
        history
    }

    async fn events_after<A: Aggregate>(
        &self,
        aggregate_id: &str,
        after_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.inner.events_after::<A>(aggregate_id, after_sequence).await
    }

    async fn snapshot<A: Aggregate>(&self, aggregate_id: &str) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        self.inner.snapshot::<A>(aggregate_id).await
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<SerializedSnapshot>,
    ) -> Result<(), PersistenceError> {
        self.inner.persist::<A>(events, snapshot_update).await
    }

    async fn feed_after<A: Aggregate>(&self, after: Position, limit: usize) -> Result<Vec<FeedEntry>, PersistenceError> {
        self.feeds.fetch_add(1, Ordering::SeqCst);
        self.fault("feed").await?;
        self.inner.feed_after::<A>(after, limit).await
    }

    async fn head<A: Aggregate>(&self) -> Result<Position, PersistenceError> {
        self.fault("head").await?;
        let head = self.inner.head::<A>().await?;
        Ok(Position::new(head.value() + self.horizon.load(Ordering::SeqCst) as u64))
    }

    async fn aggregate_ids_after<A: Aggregate>(&self, after: &str, limit: usize) -> Result<Vec<String>, PersistenceError> {
        self.fault("ids").await?;
        self.inner.aggregate_ids_after::<A>(after, limit).await
    }
}

/// Every report of the processors, in order, with the virtual time it arrived.
#[derive(Default)]
pub(crate) struct Recording(Mutex<Vec<(tokio::time::Instant, String)>>);

impl Recording {
    fn push(&self, event: String) {
        self.0.lock().unwrap().push((tokio::time::Instant::now(), event));
    }

    pub(crate) fn events(&self) -> Vec<String> {
        self.0.lock().unwrap().iter().map(|(_, event)| event.clone()).collect()
    }

    pub(crate) fn timed(&self, prefix: &str) -> Vec<tokio::time::Instant> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, event)| event.starts_with(prefix))
            .map(|(at, _)| *at)
            .collect()
    }

    pub(crate) fn count(&self, prefix: &str) -> usize {
        self.timed(prefix).len()
    }

    pub(crate) fn clear(&self) {
        self.0.lock().unwrap().clear();
    }
}

impl ProjectionObserver for Recording {
    fn applied(&self, report: &Applied<'_>) {
        self.push(format!("applied:{}:{}", report.aggregate_id, report.last_sequence));
    }
    fn advanced(&self, report: &Advanced) {
        self.push(format!("advanced:{}:{}", report.position, report.delivered));
    }
    fn failed(&self, report: &Failed<'_>) {
        self.push(format!(
            "failed:{}:{}:{}:{}",
            report.handler,
            report.category,
            report.aggregate_id.unwrap_or("-"),
            report.failures
        ));
    }
    fn rebuilt(&self, report: &Rebuilt<'_>) {
        self.push(format!(
            "rebuilt:{}:{:?}:{}",
            report.aggregate_id, report.reason, report.events
        ));
    }
    fn rebuild_phase(&self, event: RebuildPhase) {
        self.push(format!("rebuild:{event:?}"));
    }
    fn lease(&self, event: LeaseChange) {
        self.push(format!("lease:{event:?}"));
    }
    fn waiting(&self, reason: Waiting) {
        self.push(format!("waiting:{reason:?}"));
    }
}

type Store = JournalEventStore<MemoryEventRepository, Ledger>;

/// A running processor.
pub(crate) struct Node {
    task: JoinHandle<()>,
    shutdown: CancellationToken,
}

impl Node {
    /// Stops the node the graceful way and waits for it.
    pub(crate) async fn stop(self) {
        self.shutdown.cancel();
        self.task.await.unwrap();
    }

    /// Kills the node where it stands: no drain, no release.
    pub(crate) fn crash(self) {
        self.task.abort();
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.task.is_finished()
    }
}

pub(crate) struct Harness {
    pub(crate) journal: Arc<Journal>,
    pub(crate) views: Arc<Views>,
    pub(crate) checkpoints: Arc<Checkpoints>,
    pub(crate) wakeup: CommitWakeup,
    pub(crate) observer: Arc<Recording>,
    pub(crate) config: ProcessorConfig,
    executor: Arc<StoreCommandExecutor<Ledger, Store>>,
}

pub(crate) fn config() -> ProcessorConfig {
    config_builder().build().unwrap()
}

/// The configuration of the tests as a builder, for a test that changes one bound.
pub(crate) fn config_builder() -> ProcessorConfigBuilder {
    ProcessorConfig::builder()
        .poll_interval(POLL)
        .page_size(1000)
        .lease(LEASE)
        .delivery_limits(cqrsx::projection::processor::config::DeliveryLimits::default())
        .retry(RETRY_BASE, Duration::from_secs(10))
}

impl Harness {
    pub(crate) fn new() -> Self {
        Self::with_config(config())
    }

    pub(crate) fn with_config(config: ProcessorConfig) -> Self {
        let journal = MemoryEventRepository::default();
        let wakeup = CommitWakeup::new();
        let executor = Arc::new(
            StoreCommandExecutor::new(
                JournalEventStore::new(journal.clone()),
                Services::default(),
                AggregateConflictRetryPolicy::default(),
                LockLimits::default(),
            )
            .with_commit_wakeup(wakeup.clone()),
        );
        Self {
            journal: Arc::new(Journal::new(journal)),
            views: Arc::new(Views::new(MemoryViewRepository::default())),
            checkpoints: Arc::new(Checkpoints::new(MemoryCheckpointStore::default())),
            wakeup,
            observer: Arc::new(Recording::default()),
            config,
            executor,
        }
    }

    /// Starts a processor named [`NAME`] owned by `owner`.
    pub(crate) fn node(&self, owner: &str) -> Node {
        self.node_with(NAME, owner, self.config, Vec::new())
    }

    /// A second wrapper over the same checkpoint rows, so one node can get faults of its own.
    pub(crate) fn fork_checkpoints(&self) -> Arc<Checkpoints> {
        Arc::new(Checkpoints::new(self.checkpoints.inner.clone()))
    }

    /// Starts a processor named [`NAME`] that reaches the checkpoint rows through `checkpoints`.
    pub(crate) fn node_isolated(&self, owner: &str, checkpoints: Arc<Checkpoints>) -> Node {
        let projection = Arc::new(ViewProjection::new(self.views.clone()));
        let processor = ProjectionProcessor::new(NAME, self.journal.clone(), checkpoints, projection, self.config)
            .with_owner(owner)
            .with_observer(self.observer.clone())
            .with_commit_wakeup(self.wakeup.clone());
        let shutdown = CancellationToken::new();
        Node {
            task: tokio::spawn(processor.run(shutdown.clone())),
            shutdown,
        }
    }

    /// Starts a processor of projection `name` with its own configuration and upcasters.
    pub(crate) fn node_with(
        &self,
        name: &str,
        owner: &str,
        config: ProcessorConfig,
        upcasters: Vec<Box<dyn cqrsx::journal::upcaster::EventUpcaster>>,
    ) -> Node {
        let projection = Arc::new(ViewProjection::new(self.views.clone()));
        let processor = ProjectionProcessor::new(name, self.journal.clone(), self.checkpoints.clone(), projection, config)
            .with_owner(owner)
            .with_observer(self.observer.clone())
            .with_commit_wakeup(self.wakeup.clone())
            .with_upcasters(upcasters);
        let shutdown = CancellationToken::new();
        Node {
            task: tokio::spawn(processor.run(shutdown.clone())),
            shutdown,
        }
    }

    /// Commits one event of `key` through the command path, which wakes the processors.
    pub(crate) async fn commit(&self, key: &str, delta: i32) {
        self.executor_execute(key, Command::Adjust(delta)).await;
    }

    /// Commits the events of `deltas` of `key` in one transaction, past the command path (a command decides one
    /// event), and wakes the processors as a commit of the executor does.
    pub(crate) async fn commit_together(&self, key: &str, deltas: &[i32]) {
        let store = Store::new(self.journal.inner.clone());
        let context = store.load_aggregate(key).await.unwrap();
        let events = deltas.iter().map(|delta| Delta(*delta)).collect();
        store.commit(events, context, HashMap::new(), &[]).await.unwrap();
        self.wakeup.wake();
    }

    pub(crate) async fn executor_execute(&self, key: &str, command: Command) {
        self.executor.execute(&key.to_owned(), command).await.unwrap();
    }

    /// Commits `command` of `key` with `metadata` on every event, through the command path.
    pub(crate) async fn execute_with_metadata(&self, key: &str, command: Command, metadata: HashMap<String, String>) {
        self.executor
            .execute_with_metadata(&key.to_owned(), command, metadata)
            .await
            .unwrap();
    }

    /// The fold of the key's whole history, `None` without a history.
    pub(crate) async fn fold(&self, key: &str) -> Option<Balance> {
        let store = JournalEventStore::<_, Ledger>::new(self.journal.inner.clone());
        let events = store.load_events(key).await.unwrap();
        if events.is_empty() {
            return None;
        }
        let mut balance = Balance::default();
        for event in &events {
            balance.apply(event);
        }
        Some(balance)
    }

    pub(crate) async fn view(&self, key: &str) -> Option<StoredView<Balance>> {
        self.views.load_stored(key).await.unwrap()
    }

    pub(crate) async fn status(&self) -> CheckpointStatus {
        self.checkpoints.status(Some(NAME)).await.unwrap().remove(0)
    }

    pub(crate) async fn head(&self) -> Position {
        self.journal.inner.head::<Ledger>().await.unwrap()
    }

    /// Whether the checkpoint stands at the head and every key's view equals the fold of its history.
    pub(crate) async fn is_caught_up(&self, keys: &[&str]) -> bool {
        let status = self.checkpoints.status(Some(NAME)).await.unwrap();
        let head = self.head().await;
        if status.first().is_none_or(|status| status.position != head) {
            return false;
        }
        for key in keys {
            if self.view(key).await.map(|stored| stored.view) != self.fold(key).await {
                return false;
            }
        }
        true
    }

    /// Waits (in virtual time) until the processor caught up with the journal for `keys`.
    pub(crate) async fn caught_up(&self, keys: &[&str]) -> CheckpointStatus {
        tokio::time::timeout(Duration::from_secs(120), async {
            while !self.is_caught_up(keys).await {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the processor catches up with the journal");
        self.status().await
    }

    /// Waits (in virtual time) until `condition` holds.
    pub(crate) async fn until(&self, mut condition: impl AsyncFnMut() -> bool) {
        let waited = tokio::time::timeout(Duration::from_secs(120), async {
            while !condition().await {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            waited.is_ok(),
            "the condition never came true; events: {:#?}",
            self.observer.events()
        );
    }

    /// Lets the virtual clock run for `duration` in small steps, so every timer and task gets its turn.
    pub(crate) async fn run_for(&self, duration: Duration) {
        let end = tokio::time::Instant::now() + duration;
        while tokio::time::Instant::now() < end {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

/// Turns a version-0 delta nobody can read into a zero delta of the current version.
pub(crate) struct Heal;

impl cqrsx::journal::upcaster::EventUpcaster for Heal {
    fn can_upcast(&self, format: EventFormat<'_>) -> bool {
        format.event_type == "delta" && format.event_version == "0"
    }

    fn upcast(&self, mut event: SerializedEvent) -> SerializedEvent {
        event.payload = serde_json::json!(0);
        event.event_version = "1".into();
        event
    }
}

/// A delta of `key` at `sequence` in the version-0 format only [`Heal`] reads.
pub(crate) fn old_delta(key: &str, sequence: usize) -> SerializedEvent {
    let mut event = delta(key, sequence, 0);
    event.event_version = "0".into();
    event.payload = serde_json::json!({"unknown": true});
    event
}

pub(crate) fn delta(key: &str, sequence: usize, value: i32) -> SerializedEvent {
    SerializedEvent::try_from(&EventEnvelope::<Ledger> {
        aggregate_id: key.into(),
        sequence,
        payload: Delta(value),
        metadata: HashMap::new(),
    })
    .unwrap()
}
