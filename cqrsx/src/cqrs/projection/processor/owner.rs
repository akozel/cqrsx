//! One ownership of a checkpoint: the loop that reads pages, hands them to the key workers, moves the
//! checkpoint behind the applied pages and runs rebuilds, until the lease is lost or the processor stops.
//!
//! Pages form a window: the owner keeps reading while the receipts of earlier pages are in flight, up to
//! `pages_in_flight` pages, and settles the window from the front: a page moves the checkpoint only when
//! every page before it has. The first failure stops the reading; once the live receipts are in, the owner
//! moves the checkpoint to the last transaction before the failure, records the failure, clears the window
//! and reads again from the checkpoint after a pause (repeats are skipped by the contiguity rule). A document
//! that refuses an event as a gap is healed in place: a rebuild of its key is queued behind the failed job,
//! and the page waits for the rebuild's receipt as for the job's.
use super::{
    Context, Operation,
    delivery::{DeliveryError, DeliverySlot, Done},
    lease::LeaseKeeper,
    observer::{Advanced, Applied, Failed, RebuildEvent, RebuildReason, Rebuilt, Waiting},
};
use crate::{
    cqrs::{
        aggregate::Aggregate,
        event::EventEnvelope,
        projection::checkpoint::{CheckpointStore, Claimed, LeaseToken},
        store::{PersistenceError, feed::Position, repository::PersistedEventRepository},
    },
    diagnostics,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    pin::{Pin, pin},
    time::Duration,
};
use tokio::{
    sync::futures::Notified,
    task::{JoinError, JoinSet},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

/// How the ownership ended.
pub(super) enum Exit {
    /// The lease was lost: the processor claims again.
    Lost,
    /// The processor was stopped: the row was released.
    Stopped,
}

/// Why a step ended early.
enum Interrupt {
    Shutdown,
    Lost,
}

/// What the owner woke up for.
enum Woke {
    Shutdown,
    Lost,
    Live(Result<LiveOutcome, JoinError>),
    Rebuild(Result<RebuildOutcome, JoinError>),
    /// A commit woke the owner: it settles for `settle` so that the commits of a burst land in one page.
    Commit,
    Timer,
}

/// Idle polls between two looks at the journal's head.
const HEAD_CHECK_EVERY: u32 = 8;

/// What went wrong, as the status row and the observer see it.
struct Failure {
    key: Option<String>,
    handler: String,
    operation: Operation,
    category: &'static str,
    position: Position,
}

impl Failure {
    fn text(&self) -> String {
        format!("{}: {}", self.handler, self.category)
    }
}

struct Transaction {
    position: Position,
    keys: Vec<usize>,
    events: u64,
}

/// A dispatched page: its transactions and the receipts still to come.
struct Page {
    no: u64,
    last_position: Position,
    events: u64,
    transactions: Vec<Transaction>,
    pending: usize,
    failed: Vec<(usize, Failure)>,
}

struct LiveOutcome {
    page: u64,
    /// The key's index in the page, as its transactions name it.
    key: usize,
    aggregate_id: String,
    last_sequence: usize,
    events: usize,
    result: Result<Done, DeliveryError>,
}

/// The receipt of a key's rebuild.
struct RebuildOutcome {
    aggregate_id: String,
    result: Result<Done, DeliveryError>,
}

struct RebuildRun {
    cursor: String,
    pace: Duration,
    rebuilt: u64,
    started: Instant,
    page: Option<CursorPage>,
}

/// One page of the rebuild's cursor: recorded when every key of it is rebuilt.
struct CursorPage {
    last_key: String,
    keys: u64,
    pending: usize,
    failed: Option<&'static str>,
}

/// A live job that waits for the rebuild of its key as for its own answer.
struct Waiter {
    page: u64,
    key: usize,
    last_sequence: usize,
    events: usize,
}

/// Why a key is rebuilt, and what its rebuild carries.
enum RebuildJob {
    /// A key requested on its own: rebuilt at once.
    Requested,
    /// A key of the rebuild's cursor page: rebuilt at the rebuild's pace, counted on the page.
    Cursor { pace: Duration },
    /// A key whose document refused a live event as a gap: the job waits for the rebuild as for its own answer.
    Healed { waiter: Waiter },
}

/// A key whose rebuild is in flight.
struct Rebuilding {
    reason: RebuildReason,
    cursor_member: bool,
    waiting: Vec<Waiter>,
}

pub(super) struct Owner<'a, A: Aggregate, R, C> {
    ctx: &'a Context<A, R, C>,
    token: LeaseToken,
    lease: LeaseKeeper,
    /// Cancelled when the ownership ends: paced rebuilds stop at their next pause.
    stop: CancellationToken,
    checkpoint: Position,
    frontier: Position,
    reading: bool,
    /// The last page was full: read on without waiting.
    more: bool,
    /// The journal is read only for a reason: a wake-up, the poll, a full page before, the window
    /// having been full, or the end of a pause. The receipt of a page is no reason.
    read_due: bool,
    next_page: u64,
    pages: VecDeque<Page>,
    outcomes: JoinSet<LiveOutcome>,
    failed_keys: HashSet<String>,
    pending_failure: Option<Failure>,
    rebuild: Option<RebuildRun>,
    requested: VecDeque<String>,
    in_flight: HashMap<String, Rebuilding>,
    rebuild_outcomes: JoinSet<RebuildOutcome>,
    failures: u32,
    backoff_until: Option<Instant>,
    requests_due: Instant,
    idle_polls: u32,
    waiting: Waiting,
    draining: bool,
}

fn persistence_category(error: &PersistenceError) -> &'static str {
    match error {
        PersistenceError::OptimisticLockError => "optimistic_lock",
        PersistenceError::ConnectionError(_) => "connection",
        PersistenceError::DeserializationError(_) => "deserialization",
        PersistenceError::UnknownError(_) => "unknown",
        PersistenceError::SequenceGap { .. } => "sequence_gap",
    }
}

fn delivery_category(error: &DeliveryError) -> &'static str {
    match error {
        DeliveryError::Failed(error) => persistence_category(error),
        DeliveryError::Panicked => "panic",
        DeliveryError::Stopped => "stopped",
    }
}

fn is_gap(error: &DeliveryError) -> bool {
    matches!(error, DeliveryError::Failed(error) if matches!(**error, PersistenceError::SequenceGap { .. }))
}

impl<'a, A, R, C> Owner<'a, A, R, C>
where
    A: Aggregate + 'static,
    R: PersistedEventRepository + 'static,
    C: CheckpointStore + 'static,
{
    /// `anchor` is the instant before the claim request.
    pub(super) fn new(ctx: &'a Context<A, R, C>, claimed: Claimed, anchor: Instant) -> Self {
        let lease = LeaseKeeper::spawn(
            ctx.checkpoints.clone(),
            ctx.key.clone(),
            claimed.token,
            ctx.config.lease(),
            anchor,
        );
        Self {
            ctx,
            token: claimed.token,
            lease,
            stop: CancellationToken::new(),
            checkpoint: claimed.position,
            frontier: claimed.position,
            reading: true,
            more: false,
            read_due: true,
            next_page: 0,
            pages: VecDeque::new(),
            outcomes: JoinSet::new(),
            failed_keys: HashSet::new(),
            pending_failure: None,
            rebuild: claimed.rebuild.map(|state| RebuildRun {
                cursor: state.cursor,
                pace: state.pace,
                rebuilt: state.rebuilt,
                started: Instant::now(),
                page: None,
            }),
            requested: VecDeque::new(),
            in_flight: HashMap::new(),
            rebuild_outcomes: JoinSet::new(),
            failures: claimed.failures,
            backoff_until: None,
            requests_due: Instant::now(),
            idle_polls: 0,
            waiting: Waiting::Idle,
            draining: false,
        }
    }

    pub(super) async fn run(mut self) -> Exit {
        loop {
            if self.lease.is_lost() {
                return self.lost();
            }
            match self.turn().await {
                Ok(()) => {}
                Err(Interrupt::Lost) => return self.lost(),
                Err(Interrupt::Shutdown) => return self.drain().await,
            }
        }
    }

    fn lost(self) -> Exit {
        self.stop.cancel();
        Exit::Lost
    }

    fn log(&self, event: &'static str) {
        diagnostics::log_projection_event(self.ctx.key.projection(), self.ctx.key.aggregate_type(), event);
    }

    fn log_failure(&self, operation: Operation, category: &'static str, failures: u32) {
        diagnostics::log_projection_failure(
            self.ctx.key.projection(),
            self.ctx.key.aggregate_type(),
            operation,
            category,
            failures,
        );
    }

    fn page_no(&mut self) -> u64 {
        self.next_page += 1;
        self.next_page
    }

    /// A failure without a page (the journal, the checkpoint store, a rebuild): stops the reading; the
    /// first one of a window is the one recorded.
    fn store_failure(&mut self, operation: Operation, category: &'static str) {
        let handler = match operation {
            Operation::Feed | Operation::Head => "journal",
            Operation::Checkpoints | Operation::Page | Operation::Rebuild | Operation::Lease => "checkpoints",
        };
        self.pending_failure.get_or_insert(Failure {
            key: None,
            handler: handler.to_owned(),
            operation,
            category,
            position: self.checkpoint,
        });
        self.reading = false;
    }

    /// The window has a failure whose turn it is: the first page is complete and failed, or a failure
    /// without a page waits.
    fn settling(&self) -> bool {
        self.pending_failure.is_some()
            || self
                .pages
                .front()
                .is_some_and(|page| page.pending == 0 && !page.failed.is_empty())
    }

    /// One turn: register the wake-up, settle, back off or work, then wait for something to happen.
    async fn turn(&mut self) -> Result<(), Interrupt> {
        let ctx = self.ctx;
        // Registered before the page is read: a commit between the read and the wait is not lost.
        let mut woken = pin!(ctx.wakeup.notify().notified());
        woken.as_mut().enable();
        self.settle().await?;
        if self.settling() {
            if self.outcomes.is_empty() {
                self.back_off().await?;
            }
        } else if self.backoff_until.is_some_and(|until| Instant::now() >= until) {
            self.backoff_until = None;
            self.reading = true;
            self.read_due = true;
        }
        if self.backoff_until.is_none() && !self.settling() {
            self.poll_requests().await?;
            self.dispatch_rebuilds().await?;
            self.read_page().await?;
        }
        self.wait(woken).await
    }

    /// Moves the checkpoint behind the complete, successful pages at the front of the window.
    async fn settle(&mut self) -> Result<(), Interrupt> {
        while self
            .pages
            .front()
            .is_some_and(|page| page.pending == 0 && page.failed.is_empty())
        {
            let Some(page) = self.pages.pop_front() else { break };
            self.advance_to(page.last_position, page.events).await?;
        }
        Ok(())
    }

    async fn advance_to(&mut self, to: Position, applied: u64) -> Result<(), Interrupt> {
        if to <= self.checkpoint {
            return Ok(());
        }
        match self.ctx.checkpoints.advance(&self.ctx.key, self.token, to, applied).await {
            Ok(true) => {
                self.checkpoint = to;
                self.failures = 0;
                self.ctx.observer.advanced(&Advanced { position: to, applied });
                Ok(())
            }
            Ok(false) => Err(Interrupt::Lost),
            Err(error) => {
                self.store_failure(Operation::Checkpoints, error.category());
                Ok(())
            }
        }
    }

    /// The live receipts are in and a failure waits: move the checkpoint as far as the failed page allows,
    /// record the failure, clear the window and pause.
    async fn back_off(&mut self) -> Result<(), Interrupt> {
        let mut first = self.pending_failure.take();
        if self
            .pages
            .front()
            .is_some_and(|page| page.pending == 0 && !page.failed.is_empty())
            && let Some(page) = self.pages.pop_front()
        {
            let failed: HashSet<usize> = page.failed.iter().map(|(key, _)| *key).collect();
            let (mut cut, mut events) = (None, 0);
            for transaction in &page.transactions {
                if transaction.keys.iter().any(|key| failed.contains(key)) {
                    break;
                }
                cut = Some(transaction.position);
                events += transaction.events;
            }
            if let Some(cut) = cut {
                self.advance_to(cut, events).await?;
            }
            first = first
                .or_else(|| page.failed.into_iter().next().map(|(_, failure)| failure))
                .or_else(|| self.pending_failure.take());
        }
        let Some(failure) = first else { return Ok(()) };
        self.failures += 1;
        let failures = self.failures;
        match self
            .ctx
            .checkpoints
            .record_failure(&self.ctx.key, self.token, &failure.text())
            .await
        {
            Ok(true) => {}
            Ok(false) => return Err(Interrupt::Lost),
            Err(error) => self.log_failure(Operation::Checkpoints, error.category(), failures),
        }
        self.ctx.observer.failed(&Failed {
            aggregate_id: failure.key.as_deref(),
            handler: &failure.handler,
            category: failure.category,
            position: failure.position,
            failures,
        });
        self.log_failure(failure.operation, failure.category, failures);
        self.pages.clear();
        self.frontier = self.checkpoint;
        self.failed_keys.clear();
        self.pending_failure = None;
        self.backoff_until = Some(Instant::now() + self.ctx.config.backoff(failures));
        Ok(())
    }

    /// Takes the rebuild requests of the row, at most once per poll interval.
    async fn poll_requests(&mut self) -> Result<(), Interrupt> {
        if Instant::now() < self.requests_due {
            return Ok(());
        }
        self.requests_due = Instant::now() + self.ctx.config.poll_interval();
        let requests = match self
            .ctx
            .checkpoints
            .take_requests(&self.ctx.key, self.token, self.ctx.config.rebuild_page())
            .await
        {
            Ok(Some(requests)) => requests,
            Ok(None) => return Err(Interrupt::Lost),
            Err(error) => {
                self.store_failure(Operation::Checkpoints, error.category());
                return Ok(());
            }
        };
        self.requested.extend(requests.keys);
        if let Some(pace) = requests.rebuild
            && self.rebuild.is_none()
        {
            match self.ctx.checkpoints.begin_rebuild(&self.ctx.key, self.token).await {
                Ok(true) => {
                    self.rebuild = Some(RebuildRun {
                        cursor: String::new(),
                        pace,
                        rebuilt: 0,
                        started: Instant::now(),
                        page: None,
                    });
                    self.ctx.observer.rebuild(RebuildEvent::Started { pace });
                    self.log("rebuild started");
                }
                Ok(false) => return Err(Interrupt::Lost),
                Err(error) => self.store_failure(Operation::Checkpoints, error.category()), // the flag stays: next poll
            }
        }
        Ok(())
    }

    /// Queues the requested keys that are not in flight and, when a rebuild runs and its last page of keys
    /// is recorded, the next page of the cursor.
    async fn dispatch_rebuilds(&mut self) -> Result<(), Interrupt> {
        let requested: Vec<String> = self.requested.drain(..).collect();
        for key in requested {
            if self.in_flight.contains_key(&key) {
                self.requested.push_back(key);
            } else {
                self.spawn_rebuild(key, RebuildJob::Requested).await?;
            }
        }
        let Some((cursor, pace)) = self
            .rebuild
            .as_ref()
            .filter(|run| run.page.is_none())
            .map(|run| (run.cursor.clone(), run.pace))
        else {
            return Ok(());
        };
        let ids = match self
            .ctx
            .repository
            .aggregate_ids_after::<A>(&cursor, self.ctx.config.rebuild_page())
            .await
        {
            Ok(ids) => ids,
            Err(error) => {
                self.store_failure(Operation::Feed, persistence_category(&error));
                return Ok(());
            }
        };
        let Some(last_key) = ids.last().cloned() else {
            return self.finish_rebuild().await;
        };
        let keys = ids.len() as u64;
        let mut pending = 0;
        for id in ids {
            if self.in_flight.contains_key(&id) {
                continue; // being rebuilt already: it becomes the fold of its history all the same
            }
            self.spawn_rebuild(id, RebuildJob::Cursor { pace }).await?;
            pending += 1;
        }
        if let Some(run) = &mut self.rebuild {
            run.page = Some(CursorPage {
                last_key,
                keys,
                pending,
                failed: None,
            });
        }
        self.complete_cursor_page().await
    }

    async fn finish_rebuild(&mut self) -> Result<(), Interrupt> {
        match self.ctx.checkpoints.finish_rebuild(&self.ctx.key, self.token).await {
            Ok(true) => {
                if let Some(run) = self.rebuild.take() {
                    self.ctx.observer.rebuild(RebuildEvent::Finished {
                        keys: run.rebuilt,
                        elapsed: run.started.elapsed(),
                    });
                    self.log("rebuild finished");
                }
                Ok(())
            }
            Ok(false) => Err(Interrupt::Lost),
            Err(error) => {
                self.store_failure(Operation::Checkpoints, error.category());
                Ok(())
            }
        }
    }

    /// Records a page of the cursor whose keys are all rebuilt; a failed page is a failure of the round and
    /// is read again from the old cursor after the pause.
    async fn complete_cursor_page(&mut self) -> Result<(), Interrupt> {
        let done = self.rebuild.as_mut().and_then(|run| match &run.page {
            Some(page) if page.pending == 0 => run.page.take(),
            _ => None,
        });
        let Some(page) = done else { return Ok(()) };
        if let Some(category) = page.failed {
            self.pending_failure.get_or_insert(Failure {
                key: None,
                handler: self.ctx.handler.clone(),
                operation: Operation::Rebuild,
                category,
                position: self.checkpoint,
            });
            self.reading = false;
            return Ok(());
        }
        match self
            .ctx
            .checkpoints
            .advance_rebuild(&self.ctx.key, self.token, &page.last_key, page.keys)
            .await
        {
            Ok(true) => {
                if let Some(run) = &mut self.rebuild {
                    run.rebuilt += page.keys;
                    run.cursor = page.last_key;
                    self.ctx.observer.rebuild(RebuildEvent::Progress { keys: run.rebuilt });
                }
                Ok(())
            }
            Ok(false) => Err(Interrupt::Lost),
            Err(error) => {
                self.store_failure(Operation::Checkpoints, error.category());
                Ok(())
            }
        }
    }

    async fn spawn_rebuild(&mut self, key: String, job: RebuildJob) -> Result<(), Interrupt> {
        let (pace, rebuilding) = match job {
            RebuildJob::Requested => (
                Duration::ZERO,
                Rebuilding {
                    reason: RebuildReason::Requested,
                    cursor_member: false,
                    waiting: Vec::new(),
                },
            ),
            RebuildJob::Cursor { pace } => (
                pace,
                Rebuilding {
                    reason: RebuildReason::Requested,
                    cursor_member: true,
                    waiting: Vec::new(),
                },
            ),
            RebuildJob::Healed { waiter } => (
                Duration::ZERO,
                Rebuilding {
                    reason: RebuildReason::Healed,
                    cursor_member: false,
                    waiting: vec![waiter],
                },
            ),
        };
        let slot = self.reserve(&key).await?;
        let receipt = slot.rebuild(pace, self.stop.clone());
        let aggregate_id = key.clone();
        self.rebuild_outcomes.spawn(async move {
            RebuildOutcome {
                aggregate_id,
                result: receipt.outcome().await,
            }
        });
        self.in_flight.insert(key, rebuilding);
        Ok(())
    }

    /// Space in the key's buffer. Inside one ownership it is free at once (the window leaves one live job
    /// per page and one rebuild job per key within the buffer); after a lost lease the jobs of the old
    /// ownership may still hold it, so the wait also ends with the shutdown or another loss.
    async fn reserve(&self, key: &str) -> Result<DeliverySlot<A>, Interrupt> {
        tokio::select! {
            biased;
            _ = self.ctx.shutdown.cancelled() => Err(Interrupt::Shutdown),
            _ = self.lease.lost() => Err(Interrupt::Lost),
            slot = self.ctx.delivery.reserve(key) => Ok(slot),
        }
    }

    /// Reads the next page when the window has room and dispatches it; an event that does not decode cuts
    /// the page before its transaction and stops the reading on it.
    async fn read_page(&mut self) -> Result<(), Interrupt> {
        self.more = false;
        if !self.reading {
            return Ok(());
        }
        if self.pages.len() >= self.ctx.config.pages_in_flight() {
            self.read_due = true; // read as soon as a page of the window is settled
            return Ok(());
        }
        if !self.read_due {
            return Ok(());
        }
        self.read_due = false;
        let page_size = self.ctx.config.page_size();
        let entries = match self.ctx.repository.feed_after::<A>(self.frontier, page_size).await {
            Ok(entries) => entries,
            Err(error) => {
                self.store_failure(Operation::Feed, persistence_category(&error));
                return Ok(());
            }
        };
        if entries.is_empty() {
            return self.idle().await;
        }
        self.idle_polls = 0;
        // A page short of its size is the end of what the journal shows: the next read waits for a commit or
        // the poll. A full one may have more behind it.
        let full = entries.len() >= page_size;
        let mut decoded: Vec<(Position, EventEnvelope<A>)> = Vec::with_capacity(entries.len());
        let mut poison = None;
        for entry in entries {
            let (position, key) = (entry.position, entry.event.aggregate_id.clone());
            match entry.event.decode::<A>(self.ctx.upcasters.as_slice()) {
                Ok(event) => decoded.push((position, event)),
                Err(_) => {
                    poison = Some((position, key));
                    break;
                }
            }
        }
        if let Some((position, _)) = &poison {
            decoded.retain(|(p, _)| p < position);
        }
        if !decoded.is_empty() {
            self.dispatch(decoded).await?;
            self.more = full;
            self.read_due = full;
        }
        if let Some((position, key)) = poison {
            let no = self.page_no();
            let failure = Failure {
                key: Some(key),
                handler: "journal".to_owned(),
                operation: Operation::Page,
                category: "deserialization",
                position,
            };
            self.pages.push_back(Page {
                no,
                last_position: position,
                events: 0,
                transactions: Vec::new(),
                pending: 0,
                failed: vec![(0, failure)],
            });
            self.reading = false;
            self.more = false;
        }
        Ok(())
    }

    /// Groups the page by key in order of first appearance, reserves space for every key and then delivers
    /// without waiting, so an interrupted page leaves nothing half-dispatched.
    async fn dispatch(&mut self, decoded: Vec<(Position, EventEnvelope<A>)>) -> Result<(), Interrupt> {
        let last_position = decoded.last().map_or(self.frontier, |(position, _)| *position);
        let mut keys: Vec<String> = Vec::new();
        let mut index: HashMap<String, usize> = HashMap::new();
        let mut groups: Vec<Vec<EventEnvelope<A>>> = Vec::new();
        let mut transactions: Vec<Transaction> = Vec::new();
        for (position, event) in decoded {
            // A new key gets the next index, and no group yet.
            let idx = *index.entry(event.aggregate_id.clone()).or_insert(groups.len());
            match transactions.last_mut() {
                Some(transaction) if transaction.position == position => {
                    if !transaction.keys.contains(&idx) {
                        transaction.keys.push(idx);
                    }
                    transaction.events += 1;
                }
                _ => transactions.push(Transaction {
                    position,
                    keys: vec![idx],
                    events: 1,
                }),
            }
            match groups.get_mut(idx) {
                Some(group) => group.push(event),
                None => {
                    keys.push(event.aggregate_id.clone());
                    groups.push(vec![event]);
                }
            }
        }
        let mut slots = Vec::with_capacity(keys.len());
        for key in &keys {
            slots.push(self.reserve(key).await?);
        }
        let no = self.page_no();
        let events = transactions.iter().map(|transaction| transaction.events).sum();
        self.pages.push_back(Page {
            no,
            last_position,
            events,
            transactions,
            pending: slots.len(),
            failed: Vec::new(),
        });
        let deliveries = keys.into_iter().zip(slots).zip(groups).enumerate();
        for (key, ((aggregate_id, slot), events)) in deliveries {
            let (last_sequence, count) = (events.last().map_or(0, |event| event.sequence), events.len());
            let receipt = slot.deliver(events.into());
            self.outcomes.spawn(async move {
                LiveOutcome {
                    page: no,
                    key,
                    aggregate_id,
                    last_sequence,
                    events: count,
                    result: receipt.outcome().await,
                }
            });
        }
        self.frontier = last_position;
        Ok(())
    }

    /// Nothing new: every few idle polls the head tells whether a slower transaction holds the horizon.
    async fn idle(&mut self) -> Result<(), Interrupt> {
        self.idle_polls = self.idle_polls.wrapping_add(1);
        if self.idle_polls % HEAD_CHECK_EVERY == 1 {
            self.waiting = match self.ctx.repository.head::<A>().await {
                Ok(head) if head > self.frontier => Waiting::Horizon,
                Ok(_) => Waiting::Idle,
                Err(error) => {
                    self.log_failure(Operation::Head, persistence_category(&error), 0);
                    Waiting::Idle
                }
            };
        }
        self.ctx.observer.waiting(self.waiting);
        Ok(())
    }

    async fn wait(&mut self, woken: Pin<&mut Notified<'_>>) -> Result<(), Interrupt> {
        let can_read = self.reading
            && self.backoff_until.is_none()
            && !self.settling()
            && self.pages.len() < self.ctx.config.pages_in_flight();
        if (can_read && self.more) || (self.settling() && self.outcomes.is_empty()) {
            // The window has room and the journal has more, or a failure waits with nothing in flight:
            // the next turn reads on or backs off; nothing to wait for.
            tokio::task::yield_now().await;
            return Ok(());
        }
        let (backing_off, until) = match self.backoff_until {
            Some(until) => (true, until),
            None => (false, Instant::now()),
        };
        let woke = tokio::select! {
            biased;
            _ = self.ctx.shutdown.cancelled() => Woke::Shutdown,
            _ = self.lease.lost() => Woke::Lost,
            Some(joined) = self.outcomes.join_next(), if !self.outcomes.is_empty() => Woke::Live(joined),
            Some(joined) = self.rebuild_outcomes.join_next(), if !self.rebuild_outcomes.is_empty() => Woke::Rebuild(joined),
            _ = tokio::time::sleep_until(until), if backing_off => Woke::Timer,
            _ = woken, if can_read => Woke::Commit,
            _ = tokio::time::sleep(self.ctx.config.poll_interval()), if can_read => Woke::Timer,
        };
        self.handle(woke).await
    }

    async fn handle(&mut self, woke: Woke) -> Result<(), Interrupt> {
        match woke {
            Woke::Shutdown => Err(Interrupt::Shutdown),
            Woke::Lost => Err(Interrupt::Lost),
            Woke::Live(joined) => {
                self.on_live(joined).await?;
                while let Some(joined) = self.outcomes.try_join_next() {
                    self.on_live(joined).await?;
                }
                Ok(())
            }
            Woke::Rebuild(joined) => self.on_rebuild(joined).await,
            Woke::Commit => {
                let settle = self.ctx.config.settle();
                if !settle.is_zero() {
                    tokio::select! {
                        biased;
                        _ = self.ctx.shutdown.cancelled() => return Err(Interrupt::Shutdown),
                        _ = self.lease.lost() => return Err(Interrupt::Lost),
                        _ = tokio::time::sleep(settle) => {}
                    }
                }
                self.read_due = true;
                Ok(())
            }
            Woke::Timer => {
                self.read_due = true;
                Ok(())
            }
        }
    }

    async fn on_live(&mut self, joined: Result<LiveOutcome, JoinError>) -> Result<(), Interrupt> {
        let Ok(outcome) = joined else { return Ok(()) }; // a receipt task cannot panic or be cancelled
        let Some(page) = self.pages.iter_mut().find(|page| page.no == outcome.page) else {
            return Ok(());
        };
        let key = outcome.aggregate_id;
        let position = page.last_position;
        match outcome.result {
            Ok(_) => {
                page.pending -= 1;
                self.ctx.observer.applied(&Applied {
                    aggregate_id: &key,
                    last_sequence: outcome.last_sequence,
                    events: outcome.events,
                    position,
                });
            }
            Err(ref error) if is_gap(error) && !self.failed_keys.contains(&key) && !self.draining => {
                // Healed in place: the page waits for the rebuild's receipt as for the job's.
                let waiter = Waiter {
                    page: outcome.page,
                    key: outcome.key,
                    last_sequence: outcome.last_sequence,
                    events: outcome.events,
                };
                match self.in_flight.get_mut(&key) {
                    Some(rebuilding) => rebuilding.waiting.push(waiter),
                    None => {
                        self.spawn_rebuild(key, RebuildJob::Healed { waiter }).await?;
                    }
                }
            }
            Err(error) => {
                let failure = Failure {
                    key: Some(key.clone()),
                    handler: self.ctx.handler.clone(),
                    operation: Operation::Page,
                    category: delivery_category(&error),
                    position,
                };
                page.pending -= 1;
                page.failed.push((outcome.key, failure));
                self.failed_keys.insert(key);
                self.reading = false;
            }
        }
        Ok(())
    }

    async fn on_rebuild(&mut self, joined: Result<RebuildOutcome, JoinError>) -> Result<(), Interrupt> {
        let Ok(RebuildOutcome {
            aggregate_id: key,
            result,
        }) = joined
        else {
            return Ok(());
        };
        let Some(entry) = self.in_flight.remove(&key) else {
            return Ok(());
        };
        let had_waiters = !entry.waiting.is_empty();
        match result {
            Ok(done) => {
                let events = match done {
                    Done::Rebuilt { events } => events,
                    Done::Applied => 0,
                };
                self.ctx.observer.rebuilt(&Rebuilt {
                    aggregate_id: &key,
                    events,
                    reason: entry.reason,
                });
                if entry.reason == RebuildReason::Healed {
                    self.log("key healed");
                }
                for waiter in entry.waiting {
                    if let Some(page) = self.pages.iter_mut().find(|page| page.no == waiter.page) {
                        page.pending -= 1;
                        let position = page.last_position;
                        self.ctx.observer.applied(&Applied {
                            aggregate_id: &key,
                            last_sequence: waiter.last_sequence,
                            events: waiter.events,
                            position,
                        });
                    }
                }
                if entry.cursor_member
                    && let Some(page) = self.rebuild.as_mut().and_then(|run| run.page.as_mut())
                {
                    page.pending -= 1;
                }
            }
            Err(error) => {
                let category = delivery_category(&error);
                for waiter in entry.waiting {
                    if let Some(page) = self.pages.iter_mut().find(|page| page.no == waiter.page) {
                        page.pending -= 1;
                        let position = page.last_position;
                        page.failed.push((
                            waiter.key,
                            Failure {
                                key: Some(key.clone()),
                                handler: self.ctx.handler.clone(),
                                operation: Operation::Page,
                                category,
                                position,
                            },
                        ));
                    }
                }
                if had_waiters {
                    self.failed_keys.insert(key.clone());
                }
                if entry.cursor_member {
                    if let Some(page) = self.rebuild.as_mut().and_then(|run| run.page.as_mut()) {
                        page.pending -= 1;
                        page.failed.get_or_insert(category);
                    }
                } else if !had_waiters {
                    if entry.reason == RebuildReason::Requested {
                        self.requested.push_back(key.clone());
                    }
                    self.pending_failure.get_or_insert(Failure {
                        key: Some(key),
                        handler: self.ctx.handler.clone(),
                        operation: Operation::Rebuild,
                        category,
                        position: self.checkpoint,
                    });
                }
                self.reading = false;
            }
        }
        self.complete_cursor_page().await
    }

    /// Stops: paced rebuilds end at their next pause, the receipts in flight are waited for up to one lease,
    /// what completed moves the checkpoint, and the row is released so the next owner need not wait.
    async fn drain(mut self) -> Exit {
        self.draining = true;
        self.stop.cancel();
        let deadline = Instant::now() + self.ctx.config.lease();
        loop {
            if self.lease.is_lost() || self.settle().await.is_err() || self.complete_cursor_page().await.is_err() {
                return self.lost();
            }
            if self.outcomes.is_empty() && self.rebuild_outcomes.is_empty() {
                break;
            }
            let woke = tokio::select! {
                biased;
                _ = self.lease.lost() => Woke::Lost,
                Some(joined) = self.outcomes.join_next(), if !self.outcomes.is_empty() => Woke::Live(joined),
                Some(joined) = self.rebuild_outcomes.join_next(), if !self.rebuild_outcomes.is_empty() => Woke::Rebuild(joined),
                _ = tokio::time::sleep_until(deadline) => Woke::Timer,
            };
            match woke {
                Woke::Timer => break,
                woke => {
                    if self.handle(woke).await.is_err() {
                        return self.lost();
                    }
                }
            }
        }
        if self.rebuild.is_some() {
            self.ctx.observer.rebuild(RebuildEvent::Interrupted);
            self.log("rebuild interrupted");
        }
        if let Err(error) = self.ctx.checkpoints.release(&self.ctx.key, self.token).await {
            self.log_failure(Operation::Checkpoints, error.category(), 0);
        }
        Exit::Stopped
    }
}
