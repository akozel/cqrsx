//! The processor's delivery: the pages of one projection reach its handler through a bounded buffer per
//! aggregate key, handled in delivery order by a worker of that key, in batches. Rebuild jobs of a key go
//! through the same buffer, so a rebuild and the live events of one key never run at once.
//!
//! The owner [reserves](OrderedDelivery::reserve) space in a key's buffer and then
//! [delivers](DeliverySlot::deliver) the key's events of a page, or [queues a rebuild](DeliverySlot::rebuild),
//! without waiting. A worker per busy key takes up to [`max_batch`](DeliveryLimits::max_batch) queued live
//! jobs, hands their events to the handler in one call and answers every [`DeliveryReceipt`] of the batch
//! with the call's outcome. The buffer keeps the order of the deliveries per key; the owner delivers in
//! journal order.
//!
//! Invariants:
//! - a key has at most one worker, so the handler never runs twice at once for one key;
//! - the jobs of a key are handled in delivery order, each once; a batch is a run of consecutive live jobs,
//!   at most `max_batch` long, or one rebuild job: a batch never mixes the two;
//! - a key has an entry exactly while space is reserved, a job is queued or its worker runs, so an idle
//!   delivery holds no entry;
//! - reserved, queued and handled jobs of a key never exceed `capacity`;
//! - the std mutex guarding the entries is never held across an `.await`.
use super::config::DeliveryLimits;
use crate::{
    diagnostics,
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::PersistenceError,
    mutex::lock,
    projection::EventHandler,
    unwinding::{Panicked, unwinding},
};
use async_trait::async_trait;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;

/// Why an ordered handler did not handle a job. The events stay committed.
#[derive(Clone, Debug, Error)]
pub(crate) enum DeliveryError {
    /// The handler, or the rebuilder, failed for the batch that held this job; the batch shares the one error.
    #[error("event handler failed: {0}")]
    Failed(Arc<PersistenceError>),
    /// The handler panicked while handling the batch; the key's buffer keeps working.
    #[error("event handler panicked")]
    Panicked,
    /// The key's worker was dropped before it answered, e.g. because the runtime shut down; or the delivery broke an
    /// invariant of its own (logged) and dropped the job unhandled.
    #[error("event delivery stopped before the handler finished")]
    Stopped,
}

/// Replaces the document of one key with the fold of its history: the rebuild job of a projection
/// processor.
#[async_trait]
pub(crate) trait Rebuilder: Send + Sync {
    /// Reads, folds and writes the history of `aggregate_id` and returns how many events it had. `pace` is
    /// the pause before every event after the first; `stop` ends the pauses once the owner is gone.
    async fn rebuild(&self, aggregate_id: &str, pace: Duration, stop: CancellationToken) -> Result<usize, PersistenceError>;
}

/// What a job asks the key's worker to do.
enum Work<A: Aggregate> {
    /// Hand these events to the handler, joined with the neighbouring `Apply` jobs of the batch: the batch moves
    /// them into one vector, never copies them.
    Apply(Vec<EventEnvelope<A>>),
    /// Rebuild the key's document from its history: always a batch of its own.
    Rebuild { pace: Duration, stop: CancellationToken },
}

impl<A: Aggregate> Work<A> {
    /// The live events of an `Apply` job, which joins its neighbours in a batch; `None` for a job that is a batch of
    /// its own.
    fn events(&self) -> Option<&[EventEnvelope<A>]> {
        match self {
            Self::Apply(events) => Some(events),
            Self::Rebuild { .. } => None,
        }
    }
}

/// What a handled job did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Done {
    Applied,
    /// The document was rebuilt from a history of `events` events.
    Rebuilt {
        events: usize,
    },
}

/// One job in a key's buffer, holding the space it reserved until it is dropped: answered by the worker,
/// or abandoned with it.
struct Job<A: Aggregate> {
    work: Work<A>,
    receipt: oneshot::Sender<Result<Done, DeliveryError>>,
    _space: OwnedSemaphorePermit,
}

/// The buffer of one aggregate key.
struct KeyBuffer<A: Aggregate> {
    jobs: VecDeque<Job<A>>,
    /// A worker runs for this key: it is set by the delivery that starts it and cleared by the worker
    /// when it finds no job, both under the entries mutex, so a queued job always has a worker.
    worker: bool,
    /// Live [`BufferRef`]s: reservations waiting for space or holding it, not delivered yet.
    users: usize,
    /// FIFO permits, one per reserved, queued or handled job of the key.
    space: Arc<Semaphore>,
}

/// One buffer per aggregate key that is busy right now. The std mutex guards only the map and its buffers
/// and is held for a lookup, a push, a batch or a removal, never across an `.await`.
type Buffers<A> = Mutex<HashMap<String, KeyBuffer<A>>>;

fn guarded<A: Aggregate>(buffers: &Buffers<A>) -> MutexGuard<'_, HashMap<String, KeyBuffer<A>>> {
    // Nothing panics under this mutex, and the map stays consistent even if something did.
    lock(buffers, "projection delivery buffers")
}

struct Inner<A: Aggregate> {
    /// The processor's name, for the logs.
    projection: String,
    handler: Arc<dyn EventHandler<A>>,
    rebuilder: Arc<dyn Rebuilder>,
    limits: DeliveryLimits,
    buffers: Buffers<A>,
}

/// Per-key buffers and their workers in front of one projection's handler and rebuilder. A cheap handle:
/// clones share the buffers.
pub(crate) struct OrderedDelivery<A: Aggregate> {
    inner: Arc<Inner<A>>,
}

impl<A: Aggregate> Clone for OrderedDelivery<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<A: Aggregate + 'static> OrderedDelivery<A> {
    /// The delivery of the processor `projection`: live jobs go to `handler`, rebuild jobs to `rebuilder`,
    /// through the one buffer and worker of each key, so the two never run at once for one key.
    pub(crate) fn new(
        projection: &str,
        handler: Arc<dyn EventHandler<A>>,
        rebuilder: Arc<dyn Rebuilder>,
        limits: DeliveryLimits,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                projection: projection.to_owned(),
                handler,
                rebuilder,
                limits,
                buffers: Mutex::default(),
            }),
        }
    }

    #[cfg(test)]
    fn limits(&self) -> DeliveryLimits {
        self.inner.limits
    }

    /// Waits for space in the key's buffer, behind earlier reservations of the key. Dropping this future
    /// while it waits reserves nothing; dropping the returned slot gives the space back.
    pub(crate) async fn reserve(&self, aggregate_id: &str) -> DeliverySlot<A> {
        let buffer = self.buffer(aggregate_id);
        let space = buffer.space.clone().acquire_owned().await.ok();
        if space.is_none() {
            diagnostics::log_broken_invariant("projection", "the semaphore of a delivery buffer is never closed");
        }
        DeliverySlot { buffer, space }
    }

    /// Finds the key's buffer, or opens one, and counts the caller as its user.
    fn buffer(&self, aggregate_id: &str) -> BufferRef<A> {
        let mut buffers = guarded(&self.inner.buffers);
        let buffer = buffers.entry(aggregate_id.to_owned()).or_insert_with(|| KeyBuffer {
            jobs: VecDeque::new(),
            worker: false,
            users: 0,
            space: Arc::new(Semaphore::new(self.inner.limits.capacity())),
        });
        buffer.users += 1;
        let space = buffer.space.clone();
        drop(buffers);
        BufferRef {
            inner: self.inner.clone(),
            aggregate_id: aggregate_id.to_owned(),
            space,
        }
    }

    #[cfg(test)]
    fn busy_keys(&self) -> usize {
        guarded(&self.inner.buffers).len()
    }

    #[cfg(test)]
    fn queued(&self, aggregate_id: &str) -> usize {
        guarded(&self.inner.buffers)
            .get(aggregate_id)
            .map_or(0, |buffer| buffer.jobs.len())
    }

    /// The space the key's jobs hold: reserved, queued or in the handler.
    #[cfg(test)]
    fn held(&self, aggregate_id: &str) -> usize {
        guarded(&self.inner.buffers)
            .get(aggregate_id)
            .map_or(0, |buffer| self.inner.limits.capacity() - buffer.space.available_permits())
    }
}

/// A reference to a key's buffer, held from a reservation until it delivers or gives up.
struct BufferRef<A: Aggregate> {
    inner: Arc<Inner<A>>,
    aggregate_id: String,
    space: Arc<Semaphore>,
}

impl<A: Aggregate> Drop for BufferRef<A> {
    fn drop(&mut self) {
        let mut buffers = guarded(&self.inner.buffers);
        // Users are counted only under this mutex, and a buffer without users, jobs or a worker is closed
        // right there, so no idle buffer stays behind and no busy one is closed.
        if let Some(buffer) = buffers.get_mut(&self.aggregate_id) {
            buffer.users -= 1;
            if buffer.users == 0 && !buffer.worker && buffer.jobs.is_empty() {
                buffers.remove(&self.aggregate_id);
            }
        }
    }
}

/// Space reserved in one key's buffer; [`deliver`](Self::deliver) or [`rebuild`](Self::rebuild) fills it,
/// dropping it gives it back.
#[must_use = "dropping the slot gives the reserved space back"]
pub(crate) struct DeliverySlot<A: Aggregate> {
    buffer: BufferRef<A>,
    /// `None` only when the buffer's semaphore was closed, a bug: the job is then answered `Stopped`.
    space: Option<OwnedSemaphorePermit>,
}

impl<A: Aggregate + 'static> DeliverySlot<A> {
    /// Queues `events` behind every job delivered before for the key and starts the key's worker when none
    /// runs; never waits. Must be called inside a Tokio runtime. The handler gets the events whether or not
    /// anybody awaits the receipt.
    pub(crate) fn deliver(self, events: Vec<EventEnvelope<A>>) -> DeliveryReceipt {
        self.queue(Work::Apply(events))
    }

    /// Queues a rebuild of the key's document behind every job delivered before for the key: a live job
    /// delivered before it applies to the old document and the rebuild folds its events from the history; a
    /// live job delivered after it applies on top of the rebuilt document, or is skipped by the contiguity
    /// rule when the history already held its events.
    pub(crate) fn rebuild(self, pace: Duration, stop: CancellationToken) -> DeliveryReceipt {
        self.queue(Work::Rebuild { pace, stop })
    }

    /// A job that cannot be queued (a broken invariant, logged) is dropped: its receipt answers `Stopped`.
    fn queue(self, work: Work<A>) -> DeliveryReceipt {
        let Self { buffer, space } = self;
        let (receipt, outcome) = oneshot::channel();
        let Some(space) = space else {
            return DeliveryReceipt(outcome);
        };
        let job = Job {
            work,
            receipt,
            _space: space,
        };
        let start = {
            let mut buffers = guarded(&buffer.inner.buffers);
            let Some(entry) = buffers.get_mut(&buffer.aggregate_id) else {
                diagnostics::log_broken_invariant("projection", "a reserved delivery buffer stays registered");
                return DeliveryReceipt(outcome);
            };
            entry.jobs.push_back(job);
            !std::mem::replace(&mut entry.worker, true)
        };
        if start {
            let worker = Worker {
                inner: buffer.inner.clone(),
                aggregate_id: buffer.aggregate_id.clone(),
                finished: false,
            };
            // Logs of the worker go where the deliverer's would.
            tokio::spawn(worker.run().with_current_subscriber());
        }
        // `buffer` drops here: the queued job keeps the buffer registered.
        DeliveryReceipt(outcome)
    }
}

/// The outcome of the handler, or the rebuilder, for one job. Dropping it does not cancel the handling.
#[derive(Debug)]
pub(crate) struct DeliveryReceipt(oneshot::Receiver<Result<Done, DeliveryError>>);

impl DeliveryReceipt {
    /// Waits for the handler call that took this job.
    pub(crate) async fn outcome(self) -> Result<Done, DeliveryError> {
        self.0.await.unwrap_or(Err(DeliveryError::Stopped))
    }
}

/// The worker of one key: handles batches until the key's buffer is empty.
struct Worker<A: Aggregate> {
    inner: Arc<Inner<A>>,
    aggregate_id: String,
    /// Set when the worker left an empty buffer; otherwise dropping it abandons the queued jobs.
    finished: bool,
}

impl<A: Aggregate> Worker<A> {
    async fn run(mut self) {
        while let Some(mut batch) = self.next_batch() {
            let outcome = self.handle(&mut batch).await;
            for job in batch {
                // A receipt nobody awaits any more is fine: the events were handled all the same.
                let _ = job.receipt.send(outcome.clone());
            } // each job drops here: its space returns to the key
        }
    }

    /// The next run of queued jobs, or `None` after the worker has left the empty buffer (and closed it
    /// when nobody reserves space in it). Deciding to leave and queueing a job happen under one mutex, so a
    /// job is never queued behind a worker that has decided to leave. A rebuild job is a batch of its own: a
    /// run of live jobs ends at `max_batch` or before the next rebuild.
    fn next_batch(&mut self) -> Option<Vec<Job<A>>> {
        let mut buffers = guarded(&self.inner.buffers);
        let buffer = buffers.get_mut(&self.aggregate_id)?;
        let Some(first) = buffer.jobs.front() else {
            buffer.worker = false;
            if buffer.users == 0 {
                buffers.remove(&self.aggregate_id);
            }
            self.finished = true;
            return None;
        };
        let take = match &first.work {
            Work::Rebuild { .. } => 1,
            Work::Apply(_) => buffer
                .jobs
                .iter()
                .take(self.inner.limits.max_batch())
                .take_while(|job| job.work.events().is_some())
                .count(),
        };
        Some(buffer.jobs.drain(..take).collect())
    }

    /// One handler call for the whole batch, or one rebuild, isolated from a panic of the callee. The events of a
    /// batch of several jobs move into the first job's vector.
    async fn handle(&self, batch: &mut [Job<A>]) -> Result<Done, DeliveryError> {
        let projection = &self.inner.projection;
        if let [
            Job {
                work: Work::Rebuild { pace, stop },
                ..
            },
        ] = batch
        {
            return match unwinding(self.inner.rebuilder.rebuild(&self.aggregate_id, *pace, stop.clone())).await {
                Ok(Ok(events)) => Ok(Done::Rebuilt { events }),
                Ok(Err(error)) => {
                    crate::diagnostics::log_projection_batch_failure(projection, A::TYPE, "rebuild", 1, &error);
                    Err(DeliveryError::Failed(Arc::new(error)))
                }
                Err(Panicked) => {
                    crate::diagnostics::log_projection_batch_panic(projection, A::TYPE, "rebuild", 1);
                    Err(DeliveryError::Panicked)
                }
            };
        }
        let mut joined = Vec::new();
        for job in batch.iter_mut() {
            if let Work::Apply(events) = &mut job.work {
                if joined.is_empty() {
                    joined = std::mem::take(events);
                } else {
                    joined.append(events);
                }
            }
        }
        match unwinding(self.inner.handler.handle(&self.aggregate_id, &joined)).await {
            Ok(Ok(())) => Ok(Done::Applied),
            Ok(Err(error)) => {
                crate::diagnostics::log_projection_batch_failure(projection, A::TYPE, "deliver", batch.len(), &error);
                Err(DeliveryError::Failed(Arc::new(error)))
            }
            Err(Panicked) => {
                crate::diagnostics::log_projection_batch_panic(projection, A::TYPE, "deliver", batch.len());
                Err(DeliveryError::Panicked)
            }
        }
    }
}

impl<A: Aggregate> Drop for Worker<A> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Dropped mid-work (the runtime shut down): the queued jobs will never be handled. Drop them, so
        // their receipts answer `Stopped` and their space returns, and let a later delivery start a worker.
        let abandoned = {
            let mut buffers = guarded(&self.inner.buffers);
            let Some(buffer) = buffers.get_mut(&self.aggregate_id) else {
                return;
            };
            buffer.worker = false;
            let abandoned = std::mem::take(&mut buffer.jobs);
            if buffer.users == 0 {
                buffers.remove(&self.aggregate_id);
            }
            abandoned
        };
        drop(abandoned);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::persisted::tests::{TestAggregate, TestEvents};
    use async_trait::async_trait;
    use std::{
        collections::HashSet,
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicBool, Ordering},
        task::{Context, Poll, Waker},
    };

    type Envelope = EventEnvelope<TestAggregate>;

    fn events(aggregate_id: &str, sequences: impl IntoIterator<Item = usize>) -> Vec<Envelope> {
        sequences
            .into_iter()
            .map(|sequence| EventEnvelope {
                aggregate_id: aggregate_id.to_owned(),
                sequence,
                payload: TestEvents::SomethingWasDone,
                metadata: HashMap::new(),
            })
            .collect()
    }

    #[derive(Debug)]
    enum Finish {
        Ok,
        Fail,
        Panic,
    }

    /// Records every call (key and sequences) and every rebuild (key, pace, stop) and notices two calls of
    /// one key at once. A gated probe keeps each call open until the test finishes it.
    #[derive(Default)]
    struct Probe {
        gated: bool,
        calls: Mutex<Vec<(String, Vec<usize>)>>,
        rebuilds: Mutex<Vec<(String, Duration, CancellationToken)>>,
        open: Mutex<HashMap<String, oneshot::Sender<Finish>>>,
        inside: Mutex<HashSet<String>>,
        overlapped: AtomicBool,
    }

    impl Probe {
        fn gated() -> Arc<Self> {
            Arc::new(Self {
                gated: true,
                ..Self::default()
            })
        }

        fn calls_of(&self, aggregate_id: &str) -> Vec<Vec<usize>> {
            let calls = self.calls.lock().unwrap();
            calls
                .iter()
                .filter(|(id, _)| id == aggregate_id)
                .map(|(_, s)| s.clone())
                .collect()
        }

        fn rebuilds_of(&self, aggregate_id: &str) -> Vec<(Duration, CancellationToken)> {
            let rebuilds = self.rebuilds.lock().unwrap();
            rebuilds
                .iter()
                .filter(|(id, ..)| id == aggregate_id)
                .map(|(_, pace, stop)| (*pace, stop.clone()))
                .collect()
        }

        /// Enters a call of the key (noticing an overlap) and, when gated, waits for the test to finish it.
        async fn call(&self, aggregate_id: &str) -> Finish {
            if !self.inside.lock().unwrap().insert(aggregate_id.to_owned()) {
                self.overlapped.store(true, Ordering::SeqCst);
            }
            let how = if self.gated {
                let (sender, finished) = oneshot::channel();
                self.open.lock().unwrap().insert(aggregate_id.to_owned(), sender);
                finished.await.unwrap_or(Finish::Ok)
            } else {
                tokio::task::yield_now().await;
                Finish::Ok
            };
            self.inside.lock().unwrap().remove(aggregate_id);
            how
        }

        /// Ends the open call of the key; `false` when none is open.
        fn finish(&self, aggregate_id: &str, how: Finish) -> bool {
            let sender = self.open.lock().unwrap().remove(aggregate_id);
            sender.is_some_and(|sender| sender.send(how).is_ok())
        }
    }

    #[async_trait]
    impl EventHandler<TestAggregate> for Probe {
        fn name(&self) -> String {
            "probe".to_owned()
        }

        async fn handle(&self, aggregate_id: &str, events: &[Envelope]) -> Result<(), PersistenceError> {
            let sequences = events.iter().map(|e| e.sequence).collect();
            self.calls.lock().unwrap().push((aggregate_id.to_owned(), sequences));
            match self.call(aggregate_id).await {
                Finish::Ok => Ok(()),
                Finish::Fail => Err(PersistenceError::OptimisticLockError),
                Finish::Panic => panic!("the probe panicked as asked"),
            }
        }
    }

    #[async_trait]
    impl Rebuilder for Probe {
        async fn rebuild(&self, aggregate_id: &str, pace: Duration, stop: CancellationToken) -> Result<usize, PersistenceError> {
            self.rebuilds.lock().unwrap().push((aggregate_id.to_owned(), pace, stop));
            match self.call(aggregate_id).await {
                Finish::Ok => Ok(3),
                Finish::Fail => Err(PersistenceError::OptimisticLockError),
                Finish::Panic => panic!("the probe panicked as asked"),
            }
        }
    }

    /// A processor's delivery: the probe is the handler and the rebuilder.
    fn delivery(probe: &Arc<Probe>, capacity: usize, max_batch: usize) -> OrderedDelivery<TestAggregate> {
        OrderedDelivery::new(
            "probe",
            probe.clone(),
            probe.clone(),
            DeliveryLimits::try_new(capacity, max_batch).unwrap(),
        )
    }

    /// Lets the spawned workers of a current-thread runtime run until they wait again.
    async fn settle() {
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }

    /// A future polled by hand, so a test can see it pending without awaiting it.
    struct Polled<F: Future>(Option<Pin<Box<F>>>);

    impl<F: Future> Polled<F> {
        fn new(future: F) -> Self {
            Self(Some(Box::pin(future)))
        }

        /// `None` while pending; the output once.
        fn poll(&mut self) -> Option<F::Output> {
            let future = self.0.as_mut().expect("polled after completion");
            match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                Poll::Ready(output) => {
                    self.0 = None;
                    Some(output)
                }
                Poll::Pending => None,
            }
        }
    }

    async fn deliver(
        delivery: &OrderedDelivery<TestAggregate>,
        id: &str,
        sequences: impl IntoIterator<Item = usize>,
    ) -> DeliveryReceipt {
        delivery.reserve(id).await.deliver(events(id, sequences))
    }

    fn outcome_of(receipt: &mut Polled<impl Future<Output = Result<Done, DeliveryError>>>) -> Option<Result<Done, String>> {
        receipt.poll().map(|outcome| outcome.map_err(|error| error.to_string()))
    }

    #[tokio::test]
    async fn events_of_a_key_reach_the_handler_in_delivery_order_across_batches() {
        let probe = Probe::gated();
        let delivery = delivery(&probe, 64, 3);
        let mut receipts = Vec::new();
        for sequence in 1..=10 {
            receipts.push(deliver(&delivery, "k", [sequence]).await);
            if sequence % 4 == 0 {
                settle().await;
                probe.finish("k", Finish::Ok);
            }
        }
        while delivery.busy_keys() > 0 {
            settle().await;
            probe.finish("k", Finish::Ok);
        }
        for receipt in receipts {
            receipt.outcome().await.unwrap();
        }
        let handled: Vec<usize> = probe.calls_of("k").concat();
        assert_eq!(handled, (1..=10).collect::<Vec<_>>());
        assert!(probe.calls_of("k").iter().all(|batch| batch.len() <= 3));
        assert!(!probe.overlapped.load(Ordering::SeqCst));
        assert_eq!(delivery.busy_keys(), 0);
    }

    #[tokio::test]
    async fn a_blocked_call_lets_five_deliveries_gather_into_the_next_batch() {
        let probe = Probe::gated();
        let delivery = delivery(&probe, 64, 32);
        let first = deliver(&delivery, "k", [1]).await;
        settle().await;
        let rest: Vec<_> = futures_join(&delivery, "k", 2..=6).await;
        assert_eq!(delivery.queued("k"), 5);
        assert!(probe.finish("k", Finish::Ok));
        settle().await;
        assert!(probe.finish("k", Finish::Ok));
        first.outcome().await.unwrap();
        for receipt in rest {
            receipt.outcome().await.unwrap();
        }
        assert_eq!(probe.calls_of("k"), [vec![1], vec![2, 3, 4, 5, 6]]);
        assert_eq!(delivery.busy_keys(), 0);
    }

    /// Delivers one job per sequence, in order.
    async fn futures_join(
        delivery: &OrderedDelivery<TestAggregate>,
        id: &str,
        sequences: impl IntoIterator<Item = usize>,
    ) -> Vec<DeliveryReceipt> {
        let mut receipts = Vec::new();
        for sequence in sequences {
            receipts.push(deliver(delivery, id, [sequence]).await);
        }
        receipts
    }

    #[tokio::test]
    async fn batches_never_exceed_max_batch_and_one_means_a_call_per_command() {
        for max_batch in [1, 2] {
            let probe = Probe::gated();
            let delivery = delivery(&probe, 64, max_batch);
            let receipts = futures_join(&delivery, "k", 1..=5).await;
            while delivery.busy_keys() > 0 {
                settle().await;
                probe.finish("k", Finish::Ok);
            }
            for receipt in receipts {
                receipt.outcome().await.unwrap();
            }
            let calls = probe.calls_of("k");
            assert_eq!(calls.concat(), [1, 2, 3, 4, 5]);
            match max_batch {
                1 => assert_eq!(calls.len(), 5, "one call per command"),
                _ => assert_eq!(
                    calls,
                    [vec![1, 2], vec![3, 4], vec![5]],
                    "all five queued before the worker ran"
                ),
            }
        }
    }

    #[tokio::test]
    async fn keys_are_independent_a_blocked_key_does_not_delay_another() {
        let probe = Probe::gated();
        let delivery = delivery(&probe, 64, 32);
        let blocked = deliver(&delivery, "a", [1]).await;
        let other = deliver(&delivery, "b", [1]).await;
        settle().await;
        assert!(probe.finish("b", Finish::Ok));
        other.outcome().await.unwrap();
        let mut blocked = Polled::new(blocked.outcome());
        assert_eq!(outcome_of(&mut blocked), None, "key a still waits for its handler");
        assert_eq!(delivery.busy_keys(), 1);
        assert!(probe.finish("a", Finish::Ok));
        settle().await;
        assert_eq!(outcome_of(&mut blocked), Some(Ok(Done::Applied)));
        assert_eq!(delivery.busy_keys(), 0);
    }

    #[tokio::test]
    async fn the_reservation_past_capacity_waits_until_a_job_is_handled() {
        let probe = Probe::gated();
        let delivery = delivery(&probe, 2, 1);
        let first = deliver(&delivery, "k", [1]).await;
        let second = deliver(&delivery, "k", [2]).await;
        settle().await;
        let mut third = Polled::new(delivery.reserve("k"));
        assert!(
            third.poll().is_none(),
            "one job in the handler, one queued: the buffer is full"
        );
        assert!(probe.finish("k", Finish::Ok));
        settle().await;
        let third = third
            .poll()
            .expect("the handled job gave its space back")
            .deliver(events("k", [3]));
        while delivery.busy_keys() > 0 {
            settle().await;
            probe.finish("k", Finish::Ok);
        }
        for receipt in [first, second, third] {
            receipt.outcome().await.unwrap();
        }
        assert_eq!(probe.calls_of("k").concat(), [1, 2, 3]);
    }

    #[tokio::test]
    async fn a_dropped_reservation_reserves_nothing_and_leaves_no_buffer() {
        let probe = Probe::gated();
        let delivery = delivery(&probe, 2, 1);
        let slots = [delivery.reserve("k").await, delivery.reserve("k").await];
        assert_eq!((delivery.busy_keys(), delivery.held("k")), (1, 2));
        let mut waiting = Polled::new(delivery.reserve("k"));
        assert!(waiting.poll().is_none());
        drop(waiting);
        drop(slots);
        assert_eq!(delivery.busy_keys(), 0, "a reservation given back closes the idle buffer");
        let receipt = deliver(&delivery, "k", [1]).await;
        settle().await;
        assert!(probe.finish("k", Finish::Ok));
        receipt.outcome().await.unwrap();
        assert_eq!(probe.calls_of("k"), [vec![1]]);
        assert_eq!(delivery.busy_keys(), 0);
    }

    #[tokio::test]
    async fn a_closed_buffer_semaphore_or_a_lost_buffer_answers_stopped_and_logs_the_bug() {
        let probe = Probe::gated();
        let delivery = delivery(&probe, 2, 1);
        let (receipts, log) = crate::diagnostics::test_support::capture(async {
            let held = delivery.reserve("closed").await;
            guarded(&delivery.inner.buffers).get("closed").unwrap().space.close();
            let closed = delivery.reserve("closed").await.deliver(events("closed", [1]));
            drop(held);
            let lost = delivery.reserve("lost").await;
            guarded(&delivery.inner.buffers).remove("lost");
            [closed, lost.deliver(events("lost", [1]))]
        })
        .await;
        for receipt in receipts {
            assert!(matches!(receipt.outcome().await, Err(DeliveryError::Stopped)));
        }
        assert!(log.contains("the semaphore of a delivery buffer is never closed"), "{log}");
        assert!(log.contains("a reserved delivery buffer stays registered"), "{log}");
        assert_eq!(delivery.busy_keys(), 0, "nothing stays behind");
        assert!(probe.calls_of("closed").is_empty() && probe.calls_of("lost").is_empty());
    }

    #[tokio::test]
    async fn a_dropped_receipt_or_caller_does_not_cancel_the_handling() {
        let probe = Probe::gated();
        let delivery = delivery(&probe, 64, 32);
        drop(deliver(&delivery, "k", [1]).await);
        // A caller that goes away right after the hand-over: its future is dropped, the job stays.
        let mut caller = Polled::new(async {
            let receipt = deliver(&delivery, "k", [2]).await;
            receipt.outcome().await
        });
        assert!(caller.poll().is_none());
        drop(caller);
        assert_eq!(delivery.held("k"), 2, "both jobs stay queued");
        while delivery.busy_keys() > 0 {
            settle().await;
            probe.finish("k", Finish::Ok);
        }
        assert_eq!(probe.calls_of("k").concat(), [1, 2]);
    }

    #[tokio::test]
    async fn a_failure_reaches_every_receipt_of_the_batch_and_is_logged_once() {
        let probe = Probe::gated();
        let delivery = delivery(&probe, 64, 32);
        let ((outcomes, next), log) = crate::diagnostics::test_support::capture(async {
            let first = deliver(&delivery, "private_id", [1]).await;
            settle().await;
            let batch = futures_join(&delivery, "private_id", 2..=4).await;
            assert!(probe.finish("private_id", Finish::Ok));
            settle().await;
            assert!(probe.finish("private_id", Finish::Fail));
            let mut outcomes = vec![first.outcome().await.map_err(|e| e.to_string())];
            for receipt in batch {
                outcomes.push(receipt.outcome().await.map_err(|e| e.to_string()));
            }
            let next = deliver(&delivery, "private_id", [5]).await;
            settle().await;
            assert!(probe.finish("private_id", Finish::Ok));
            (outcomes, next.outcome().await.map_err(|e| e.to_string()))
        })
        .await;
        let failed = Err("event handler failed: optimistic lock error".to_owned());
        assert_eq!(outcomes, [Ok(Done::Applied), failed.clone(), failed.clone(), failed]);
        assert_eq!(next, Ok(Done::Applied), "the buffer keeps working after a failure");
        assert_eq!(log.matches("projection batch failed").count(), 1, "{log}");
        assert!(
            log.contains("jobs=3")
                && log.contains("projection=\"probe\"")
                && log.contains("operation=\"deliver\"")
                && log.contains("aggregate_type=\"TestAggregate\""),
            "{log}"
        );
        assert!(!log.contains("private_"), "{log}");
    }

    #[tokio::test]
    async fn a_panic_fails_the_batch_and_the_key_buffer_keeps_working() {
        let probe = Probe::gated();
        let delivery = delivery(&probe, 64, 32);
        let ((panicked, next), log) = crate::diagnostics::test_support::capture(async {
            let panicked = deliver(&delivery, "k", [1]).await;
            settle().await;
            assert!(probe.finish("k", Finish::Panic));
            let panicked = panicked.outcome().await.map_err(|e| e.to_string());
            let next = deliver(&delivery, "k", [2]).await;
            settle().await;
            assert!(probe.finish("k", Finish::Ok));
            (panicked, next.outcome().await)
        })
        .await;
        assert_eq!(panicked, Err("event handler panicked".to_owned()));
        next.unwrap();
        assert_eq!(log.matches("projection batch panicked").count(), 1, "{log}");
        assert_eq!(delivery.busy_keys(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_delivery_right_after_the_worker_left_starts_a_new_one_in_order() {
        let probe = Arc::new(Probe::default());
        let delivery = delivery(&probe, 4, 2);
        let mut keys = tokio::task::JoinSet::new();
        for key in 0..4 {
            let delivery = delivery.clone();
            // One deliverer per key, as under the key's lock; some wait for their receipt, some do not,
            // so deliveries meet a worker that is busy, one that is leaving and none at all.
            keys.spawn(async move {
                let id = key.to_string();
                for sequence in 1..=500 {
                    let receipt = deliver(&delivery, &id, [sequence]).await;
                    match sequence % 3 {
                        0 => {
                            receipt.outcome().await.unwrap();
                        }
                        1 => tokio::task::yield_now().await,
                        _ => {}
                    }
                }
            });
        }
        while let Some(result) = keys.join_next().await {
            result.unwrap();
        }
        // Every deliverer is done: a key without a buffer has no job left and no worker.
        for _ in 0..1000 {
            if delivery.busy_keys() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert_eq!(delivery.busy_keys(), 0, "an idle delivery holds no buffer");
        for key in 0..4 {
            assert_eq!(probe.calls_of(&key.to_string()).concat(), (1..=500).collect::<Vec<_>>());
        }
        assert!(
            !probe.overlapped.load(Ordering::SeqCst),
            "two workers handled one key at once"
        );
    }

    #[test]
    fn a_stopped_runtime_answers_stopped_to_every_receipt() {
        let probe = Probe::gated();
        let delivery = delivery(&probe, 64, 1);
        let first = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let receipts = first.block_on(async {
            let receipts = futures_join(&delivery, "k", 1..=3).await;
            settle().await;
            receipts
        });
        assert_eq!((probe.calls_of("k"), delivery.held("k")), (vec![vec![1]], 3));
        drop(first); // drops the worker in the middle of its first call
        let second = tokio::runtime::Builder::new_current_thread().build().unwrap();
        second.block_on(async {
            for receipt in receipts {
                assert!(matches!(receipt.outcome().await, Err(DeliveryError::Stopped)));
            }
            assert_eq!((delivery.held("k"), delivery.busy_keys()), (0, 0));
            // A new delivery starts a new worker.
            let receipt = deliver(&delivery, "k", [4]).await;
            settle().await;
            assert!(probe.finish("k", Finish::Ok));
            receipt.outcome().await.unwrap();
        });
        assert_eq!(probe.calls_of("k"), [vec![1], vec![4]]);
    }

    #[tokio::test]
    async fn a_rebuild_job_runs_after_the_live_jobs_before_it_and_before_the_ones_after() {
        let probe = Probe::gated();
        let delivery = delivery(&probe, 64, 32);
        let stop = CancellationToken::new();
        let before = deliver(&delivery, "k", [1]).await;
        let rebuild = delivery.reserve("k").await.rebuild(Duration::from_millis(5), stop.clone());
        let after = deliver(&delivery, "k", [2]).await;
        settle().await;
        assert_eq!((probe.calls_of("k"), probe.rebuilds_of("k").len()), (vec![vec![1]], 0));
        assert!(probe.finish("k", Finish::Ok));
        settle().await;
        assert_eq!(probe.calls_of("k"), [vec![1]], "the live job after the rebuild waits");
        let (pace, received) = probe.rebuilds_of("k").remove(0);
        assert_eq!(pace, Duration::from_millis(5));
        stop.cancel();
        assert!(received.is_cancelled(), "the rebuilder gets the owner's stop token");
        assert!(probe.finish("k", Finish::Ok));
        settle().await;
        assert_eq!(probe.calls_of("k"), [vec![1], vec![2]]);
        assert!(probe.finish("k", Finish::Ok));
        assert_eq!(before.outcome().await.unwrap(), Done::Applied);
        assert_eq!(rebuild.outcome().await.unwrap(), Done::Rebuilt { events: 3 });
        assert_eq!(after.outcome().await.unwrap(), Done::Applied);
        assert!(!probe.overlapped.load(Ordering::SeqCst));
        assert_eq!(delivery.busy_keys(), 0);
    }

    #[tokio::test]
    async fn a_batch_of_live_jobs_ends_before_a_rebuild_and_the_rebuild_is_a_batch_of_its_own() {
        let probe = Probe::gated();
        let delivery = delivery(&probe, 64, 32);
        let first = deliver(&delivery, "k", [1]).await;
        settle().await;
        let mut receipts = futures_join(&delivery, "k", 2..=3).await;
        receipts.push(delivery.reserve("k").await.rebuild(Duration::ZERO, CancellationToken::new()));
        receipts.extend(futures_join(&delivery, "k", 4..=5).await);
        for _ in 0..4 {
            assert!(probe.finish("k", Finish::Ok));
            settle().await;
        }
        first.outcome().await.unwrap();
        let outcomes: Vec<Done> = {
            let mut outcomes = Vec::new();
            for receipt in receipts {
                outcomes.push(receipt.outcome().await.unwrap());
            }
            outcomes
        };
        assert_eq!(probe.calls_of("k"), [vec![1], vec![2, 3], vec![4, 5]]);
        assert_eq!(probe.rebuilds_of("k").len(), 1);
        assert_eq!(
            outcomes,
            [
                Done::Applied,
                Done::Applied,
                Done::Rebuilt { events: 3 },
                Done::Applied,
                Done::Applied
            ]
        );
    }

    #[tokio::test]
    async fn a_failing_or_panicking_rebuild_answers_its_receipt_and_the_worker_keeps_working() {
        let probe = Probe::gated();
        let delivery = delivery(&probe, 64, 32);
        let ((failed, panicked, next), log) = crate::diagnostics::test_support::capture(async {
            let failed = delivery
                .reserve("private_id")
                .await
                .rebuild(Duration::ZERO, CancellationToken::new());
            settle().await;
            assert!(probe.finish("private_id", Finish::Fail));
            let failed = failed.outcome().await.map_err(|e| e.to_string());
            let panicked = delivery
                .reserve("private_id")
                .await
                .rebuild(Duration::ZERO, CancellationToken::new());
            settle().await;
            assert!(probe.finish("private_id", Finish::Panic));
            let panicked = panicked.outcome().await.map_err(|e| e.to_string());
            let next = deliver(&delivery, "private_id", [1]).await;
            settle().await;
            assert!(probe.finish("private_id", Finish::Ok));
            (failed, panicked, next.outcome().await)
        })
        .await;
        assert_eq!(failed, Err("event handler failed: optimistic lock error".to_owned()));
        assert_eq!(panicked, Err("event handler panicked".to_owned()));
        assert_eq!(next.unwrap(), Done::Applied);
        assert_eq!(log.matches("projection batch failed").count(), 1, "{log}");
        assert_eq!(log.matches("projection batch panicked").count(), 1, "{log}");
        assert_eq!(log.matches("operation=\"rebuild\"").count(), 2, "{log}");
        assert!(log.contains("jobs=1") && !log.contains("private_"), "{log}");
        assert_eq!(delivery.busy_keys(), 0);
    }

    #[test]
    fn errors_and_handles_describe_themselves() {
        let probe = Probe::gated();
        let delivery = delivery(&probe, 3, 2);
        assert_eq!(delivery.limits(), DeliveryLimits::try_new(3, 2).unwrap());
        assert_eq!(
            DeliveryError::Stopped.to_string(),
            "event delivery stopped before the handler finished"
        );
        let (_, receiver) = oneshot::channel();
        assert!(format!("{:?}", DeliveryReceipt(receiver)).starts_with("DeliveryReceipt"));
    }

    mod model {
        use super::*;
        use crate::journal::serialized_event::tests::property_config;
        use proptest::prelude::*;

        const KEYS: usize = 3;
        const CAPACITY: usize = 3;
        const MAX_BATCH: usize = 2;

        #[derive(Clone, Debug)]
        enum Op {
            Deliver(usize),
            Rebuild(usize),
            Finish(usize, bool),
            CancelReservation(usize),
            DropReceipt(usize),
        }

        fn op() -> impl Strategy<Value = Op> {
            prop_oneof![
                5 => (0..KEYS).prop_map(Op::Deliver),
                1 => (0..KEYS).prop_map(Op::Rebuild),
                4 => (0..KEYS, any::<bool>()).prop_map(|(k, ok)| Op::Finish(k, ok)),
                1 => (0..KEYS).prop_map(Op::CancelReservation),
                1 => any::<usize>().prop_map(Op::DropReceipt),
            ]
        }

        /// A job as the model sees it: a live job with its sequence, or a rebuild; both numbered per key.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        enum Item {
            Apply(usize),
            Rebuild(usize),
        }

        impl Item {
            fn id(self) -> usize {
                match self {
                    Item::Apply(id) | Item::Rebuild(id) => id,
                }
            }

            fn done(self) -> Done {
                match self {
                    Item::Apply(_) => Done::Applied,
                    Item::Rebuild(_) => Done::Rebuilt { events: 3 },
                }
            }
        }

        /// One key as the model sees it: queued jobs, the batch in the handler, the live calls and the
        /// rebuilds so far, and the next job number.
        #[derive(Default)]
        struct ModelKey {
            queued: VecDeque<Item>,
            in_call: Vec<Item>,
            calls: Vec<Vec<usize>>,
            rebuilds: usize,
            next: usize,
        }

        impl ModelKey {
            fn used(&self) -> usize {
                self.queued.len() + self.in_call.len()
            }

            /// What the key's worker does when it is free: a run of live jobs up to `MAX_BATCH`, or one rebuild.
            fn start_call(&mut self) {
                if self.in_call.is_empty() && !self.queued.is_empty() {
                    let take = match self.queued.front() {
                        Some(Item::Rebuild(_)) => 1,
                        _ => self
                            .queued
                            .iter()
                            .take(MAX_BATCH)
                            .take_while(|item| matches!(item, Item::Apply(_)))
                            .count(),
                    };
                    self.in_call = self.queued.drain(..take).collect();
                    match self.in_call[0] {
                        Item::Rebuild(_) => self.rebuilds += 1,
                        Item::Apply(_) => self.calls.push(self.in_call.iter().map(|item| item.id()).collect()),
                    }
                }
            }
        }

        type Reserving<'a> = Polled<Pin<Box<dyn Future<Output = DeliverySlot<TestAggregate>> + 'a>>>;
        type Receipt = Polled<Pin<Box<dyn Future<Output = Result<Done, DeliveryError>>>>>;

        /// Runs `ops` against a real delivery and a plain model of one FIFO buffer and one call per key.
        async fn check(ops: Vec<Op>) -> Result<(), TestCaseError> {
            let probe = Probe::gated();
            let delivery = delivery(&probe, CAPACITY, MAX_BATCH);
            let keys: Vec<String> = (0..KEYS).map(|k| k.to_string()).collect();
            let mut model: Vec<ModelKey> = (0..KEYS).map(|_| ModelKey::default()).collect();
            // A pending reservation per key and whether it will deliver a rebuild.
            let mut reserving: Vec<Option<(Reserving<'_>, bool)>> = (0..KEYS).map(|_| None).collect();
            // Receipts by key and job number, with the outcome the model expects once the job is handled.
            let mut receipts: Vec<(usize, usize, Receipt)> = Vec::new();
            let mut outcomes: HashMap<(usize, usize), (bool, Item)> = HashMap::new();
            for op in ops {
                let rebuild = matches!(op, Op::Rebuild(_));
                match op {
                    Op::Deliver(k) | Op::Rebuild(k) => {
                        if reserving[k].is_some() {
                            continue; // one job of a key at a time, as under its lock or in a page
                        }
                        reserving[k] = Some((Polled::new(Box::pin(delivery.reserve(&keys[k]))), rebuild));
                    }
                    Op::Finish(k, ok) => {
                        let m = &mut model[k];
                        if m.in_call.is_empty() {
                            continue;
                        }
                        let how = if ok { Finish::Ok } else { Finish::Fail };
                        prop_assert!(probe.finish(&keys[k], how));
                        for item in m.in_call.drain(..) {
                            outcomes.insert((k, item.id()), (ok, item));
                        }
                    }
                    Op::CancelReservation(k) => reserving[k] = None,
                    Op::DropReceipt(pick) => {
                        if !receipts.is_empty() {
                            receipts.remove(pick % receipts.len());
                        }
                    }
                }
                // Twice: a reservation granted after a finished call delivers, and its worker starts.
                for _ in 0..2 {
                    settle().await;
                    for k in 0..KEYS {
                        model[k].start_call();
                        let Some((reservation, rebuild)) = reserving[k].as_mut() else {
                            continue;
                        };
                        let m = &mut model[k];
                        match reservation.poll() {
                            Some(slot) => {
                                prop_assert!(m.used() < CAPACITY, "space reserved past capacity");
                                m.next += 1;
                                let item = if *rebuild {
                                    Item::Rebuild(m.next)
                                } else {
                                    Item::Apply(m.next)
                                };
                                let receipt = match item {
                                    Item::Rebuild(_) => slot.rebuild(Duration::ZERO, CancellationToken::new()),
                                    Item::Apply(sequence) => slot.deliver(events(&keys[k], [sequence])),
                                };
                                receipts.push((k, m.next, Polled::new(Box::pin(receipt.outcome()))));
                                m.queued.push_back(item);
                                reserving[k] = None;
                            }
                            None => prop_assert_eq!(m.used(), CAPACITY, "a reservation waits only for a full buffer"),
                        }
                    }
                }
                for (k, m) in model.iter().enumerate() {
                    prop_assert_eq!(&probe.calls_of(&keys[k]), &m.calls);
                    prop_assert_eq!(probe.rebuilds_of(&keys[k]).len(), m.rebuilds);
                    prop_assert_eq!(delivery.queued(&keys[k]), m.queued.len());
                }
                let mut still = Vec::new();
                for (k, id, mut receipt) in receipts.drain(..) {
                    match (receipt.poll(), outcomes.get(&(k, id))) {
                        (None, None) => still.push((k, id, receipt)),
                        (Some(outcome), Some(&(ok, item))) => {
                            prop_assert_eq!(outcome.is_ok(), ok);
                            if let Ok(done) = outcome {
                                prop_assert_eq!(done, item.done());
                            }
                        }
                        (outcome, expected) => {
                            return Err(TestCaseError::fail(format!(
                                "job {k}/{id}: got {outcome:?}, expected {expected:?}"
                            )));
                        }
                    }
                }
                receipts = still;
                for (k, m) in model.iter().enumerate() {
                    prop_assert_eq!(delivery.held(&keys[k]), m.used(), "space held by key {}", k);
                }
                let busy = (0..KEYS).filter(|&k| model[k].used() > 0 || reserving[k].is_some()).count();
                prop_assert_eq!(delivery.busy_keys(), busy);
            }
            drop(reserving);
            for (k, key) in keys.iter().enumerate() {
                while model[k].used() > 0 {
                    prop_assert!(probe.finish(key, Finish::Ok));
                    model[k].in_call.clear();
                    settle().await;
                    model[k].start_call();
                }
            }
            settle().await;
            prop_assert_eq!(delivery.busy_keys(), 0);
            prop_assert!(!probe.overlapped.load(Ordering::SeqCst));
            Ok(())
        }

        proptest! {
            #![proptest_config(proptest::test_runner::Config { cases: 256, ..property_config() })]
            #[test]
            fn property_ordered_delivery_matches_a_fifo_batch_model(ops in proptest::collection::vec(op(), 1..60)) {
                let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
                runtime.block_on(check(ops))?;
            }
        }
    }
}
