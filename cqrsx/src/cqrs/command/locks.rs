//! Aggregate locks: inside one process, commands to one aggregate run one at a time, in arrival order.
//!
//! Every command waits for its aggregate's lock in its own task and runs once it holds the lock. Different
//! aggregates stay parallel. Waiting is bounded twice: by time ([`LockLimits::max_wait`]) and by the number
//! of commands that wait for one aggregate ([`LockLimits::max_waiters`]); past either bound the command is
//! refused before it runs. Locks order commands inside a process only: between processes, and between
//! executors with registries of their own, the store's optimistic check still decides.
//!
//! Public are the limits and their defaults; the registry itself belongs to the command executor.
//!
//! Invariants:
//! - at most one command holds the lock of a key;
//! - waiting commands take the lock first in, first out, skipping those that gave up;
//! - a key has a registry entry exactly while a command holds or awaits its lock, so an idle registry is empty;
//! - the commands counted as waiting for a key never exceed `max_waiters`; the holder is not counted.
use crate::cqrs::aggregate::AggregateKey;
use std::{
    collections::HashMap,
    fmt,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use thiserror::Error;
use tokio::sync::OwnedMutexGuard;

/// How long a command waits for its aggregate's lock unless configured otherwise.
pub const DEFAULT_LOCK_MAX_WAIT: Duration = Duration::from_secs(1);
/// How many commands may wait for one aggregate's lock unless configured otherwise.
pub const DEFAULT_LOCK_MAX_WAITERS: NonZeroUsize = NonZeroUsize::new(128).unwrap();

/// Checked bounds of waiting for a lock: how long one command waits, and how many wait per key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LockLimits {
    max_wait: Duration,
    max_waiters: NonZeroUsize,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum LockLimitsError {
    #[error("the maximum wait for an aggregate lock must be positive")]
    ZeroMaxWait,
    #[error("the maximum number of commands waiting for an aggregate lock must be positive")]
    ZeroMaxWaiters,
}

impl LockLimits {
    pub fn new(max_wait: Duration, max_waiters: usize) -> Result<Self, LockLimitsError> {
        if max_wait.is_zero() {
            return Err(LockLimitsError::ZeroMaxWait);
        }
        Ok(Self {
            max_wait,
            max_waiters: NonZeroUsize::new(max_waiters).ok_or(LockLimitsError::ZeroMaxWaiters)?,
        })
    }

    /// How long a command waits for the lock before it is refused.
    pub fn max_wait(&self) -> Duration {
        self.max_wait
    }

    /// How many commands may wait for one key; the next one is refused at once. The command holding the
    /// lock is not counted.
    pub fn max_waiters(&self) -> usize {
        self.max_waiters.get()
    }
}

impl Default for LockLimits {
    /// [`DEFAULT_LOCK_MAX_WAIT`] and [`DEFAULT_LOCK_MAX_WAITERS`].
    fn default() -> Self {
        Self {
            max_wait: DEFAULT_LOCK_MAX_WAIT,
            max_waiters: DEFAULT_LOCK_MAX_WAITERS,
        }
    }
}

/// Why a command did not get its aggregate's lock. Nothing ran and nothing was written.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum LockRefused {
    #[error("the aggregate's lock was not acquired within the maximum wait")]
    TimedOut,
    #[error("too many commands already wait for the aggregate's lock")]
    TooManyWaiters,
}

impl LockRefused {
    /// A stable label for logs.
    pub(crate) fn reason(self) -> &'static str {
        match self {
            Self::TimedOut => "timed_out",
            Self::TooManyWaiters => "too_many_waiters",
        }
    }
}

/// The line of one aggregate key. Tokio's mutex queues its waiters first in, first out and hands a
/// released lock straight to the first of them, so the waiter list of `mutex` *is* the line of the key.
#[derive(Default)]
struct Line {
    mutex: Arc<tokio::sync::Mutex<()>>,
    /// Commands in the line, not counting the one holding the lock.
    waiting: AtomicUsize,
}

struct Entry {
    line: Arc<Line>,
    /// Live [`LineRef`]s: the commands holding or awaiting the lock. Changed only under the registry's mutex.
    users: usize,
}

/// One line per aggregate key that is busy right now. The std mutex guards only the map and is held
/// for a lookup, an insert or a removal, never across an `.await`.
type Registry = Mutex<HashMap<AggregateKey, Entry>>;

fn entries(registry: &Registry) -> MutexGuard<'_, HashMap<AggregateKey, Entry>> {
    // Nothing panics under this mutex, and the map stays consistent even if something did.
    registry.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One FIFO lock per aggregate key, owned by a command executor; a cheap handle, clones share the registry.
/// A key's entry lives while its lock is held or awaited, so memory follows the keys that are busy right
/// now, not every key ever seen.
#[derive(Clone)]
pub(crate) struct AggregateLockRegistry {
    limits: LockLimits,
    registry: Arc<Registry>,
}

impl Default for AggregateLockRegistry {
    /// A new registry with [`LockLimits::default`].
    fn default() -> Self {
        Self::new(LockLimits::default())
    }
}

impl AggregateLockRegistry {
    pub(crate) fn new(limits: LockLimits) -> Self {
        Self {
            limits,
            registry: Arc::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn limits(&self) -> LockLimits {
        self.limits
    }

    /// Waits for the key's lock behind the commands that came before, at most `max_wait`; refuses at once
    /// when `max_waiters` commands already wait. Dropping the returned lock hands it to the next command in
    /// line; dropping this future while it waits leaves the line without disturbing the others.
    pub(crate) async fn acquire(&self, key: AggregateKey) -> Result<AggregateLockGuard, LockRefused> {
        let line = self.line(key);
        // A free lock has nobody waiting for it (a released lock goes straight to the first waiter), so taking
        // it here never overtakes anyone. The common case of an idle key pays for no slot and no timer.
        if let Ok(guard) = line.line.mutex.clone().try_lock_owned() {
            return Ok(AggregateLockGuard { _guard: guard, line });
        }
        let guard = {
            let _slot = WaitingSlot::join(&line.line.waiting, self.limits.max_waiters).ok_or(LockRefused::TooManyWaiters)?;
            tokio::time::timeout(self.limits.max_wait, line.line.mutex.clone().lock_owned())
                .await
                .map_err(|_| LockRefused::TimedOut)?
        }; // `_slot` leaves the count here: this command no longer waits, it holds the lock
        Ok(AggregateLockGuard { _guard: guard, line })
    }

    /// Finds the key's line, or opens one if nobody holds or awaits the key.
    fn line(&self, key: AggregateKey) -> LineRef {
        let mut registry = entries(&self.registry);
        let line = match registry.get_mut(&key) {
            Some(entry) => {
                entry.users += 1; // someone holds or awaits this key: join their line
                entry.line.clone()
            }
            None => {
                let line = Arc::<Line>::default(); // the first command for the key opens a line
                registry.insert(
                    key.clone(),
                    Entry {
                        line: line.clone(),
                        users: 1,
                    },
                );
                line
            }
        };
        drop(registry);
        LineRef {
            registry: self.registry.clone(),
            key,
            line,
        }
    }

    #[cfg(test)]
    fn busy_keys(&self) -> usize {
        entries(&self.registry).len()
    }

    #[cfg(test)]
    fn waiting(&self, key: &AggregateKey) -> usize {
        entries(&self.registry)
            .get(key)
            .map_or(0, |entry| entry.line.waiting.load(Ordering::SeqCst))
    }
}

/// The key's lock, held while one command runs; dropping it hands the lock to the next command in line.
/// Fields drop in declaration order: first the guard (tokio passes the lock on), then the line reference
/// (which closes the line if nobody else holds or awaits it). So a lock never outlives its line, and a key
/// never has two lines with a held lock. Never `mem::forget` it: the key would stay taken.
#[must_use = "the lock passes to the next command as soon as it is dropped"]
pub(crate) struct AggregateLockGuard {
    _guard: OwnedMutexGuard<()>,
    line: LineRef,
}

impl AggregateLockGuard {
    /// The key whose lock this is: its text is the one the command's attempts load and plan with.
    pub(crate) fn key(&self) -> &AggregateKey {
        &self.line.key
    }
}

impl fmt::Debug for AggregateLockGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateLockGuard").field("key", &self.line.key).finish()
    }
}

/// A reference to a key's line, held by every command that waits in it or holds its lock.
struct LineRef {
    registry: Arc<Registry>,
    key: AggregateKey,
    line: Arc<Line>,
}

impl Drop for LineRef {
    fn drop(&mut self) {
        let mut registry = entries(&self.registry);
        // References are counted only under the registry's mutex, so "nobody left" is exact: two commands leaving
        // at once, or one leaving while another enters, never leave a stale entry or close a busy line.
        // The entry is ours: a key gets a new entry only after its last reference has gone.
        if let Some(entry) = registry.get_mut(&self.key) {
            entry.users -= 1;
            if entry.users == 0 {
                registry.remove(&self.key);
            }
        }
    }
}

/// Counts one command in the line and uncounts it on every exit: lock acquired, timed out, or the
/// command dropped while waiting (the client went away).
struct WaitingSlot<'a>(&'a AtomicUsize);

impl<'a> WaitingSlot<'a> {
    /// Joins unless `max` commands already wait; checking and counting is one atomic step, so commands
    /// entering at once never exceed the bound.
    fn join(waiting: &'a AtomicUsize, max: NonZeroUsize) -> Option<Self> {
        waiting
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                (count < max.get()).then_some(count + 1)
            })
            .ok()
            .map(|_| Self(waiting))
    }
}

impl Drop for WaitingSlot<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::VecDeque,
        future::Future,
        pin::Pin,
        task::{Context, Poll, Waker},
    };

    fn key(id: &str) -> AggregateKey {
        AggregateKey::new("hall", id)
    }

    fn locks(max_wait: Duration, max_waiters: usize) -> AggregateLockRegistry {
        AggregateLockRegistry::new(LockLimits::new(max_wait, max_waiters).unwrap())
    }

    /// A future polled by hand. The tests never `.await` a waiting command: on a paused clock an idle
    /// runtime advances time by itself up to the next timer, which would time every waiter out.
    struct Polled<F: Future>(Option<Pin<Box<F>>>);

    impl<F: Future> Polled<F> {
        fn new(future: F) -> Self {
            Self(Some(Box::pin(future)))
        }

        /// `None` while pending; the output once, after which the future is gone.
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

        fn is_pending(&mut self) -> bool {
            self.poll().is_none()
        }
    }

    fn acquired(output: Option<Result<AggregateLockGuard, LockRefused>>) -> AggregateLockGuard {
        match output {
            Some(Ok(lock)) => lock,
            other => panic!("expected the lock, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn uncontended_key_acquires_at_once_and_leaves_no_entry() {
        let locks = locks(Duration::from_secs(1), 1);
        let lock = locks.acquire(key("h-1")).await.unwrap();
        assert_eq!(
            format!("{lock:?}"),
            r#"AggregateLockGuard { key: AggregateKey { aggregate_type: "hall", aggregate_id: "h-1" } }"#
        );
        assert_eq!((locks.busy_keys(), locks.waiting(&key("h-1"))), (1, 0));
        drop(lock);
        assert_eq!(locks.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn one_holder_per_key_and_waiters_acquire_in_arrival_order() {
        let locks = locks(Duration::from_secs(1), 8);
        let mut holder = locks.acquire(key("h-1")).await.unwrap();
        let mut waiters: Vec<_> = (0..5).map(|_| Polled::new(locks.acquire(key("h-1")))).collect();
        for waiter in &mut waiters {
            assert!(waiter.is_pending());
        }
        assert_eq!(locks.waiting(&key("h-1")), 5);
        for next in 0..waiters.len() {
            for waiter in &mut waiters[next..] {
                assert!(waiter.is_pending(), "nobody acquires while the lock is held");
            }
            drop(holder);
            holder = acquired(waiters[next].poll());
            assert_eq!(locks.waiting(&key("h-1")), 4 - next);
        }
        drop(holder);
        assert_eq!(locks.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_newcomer_never_overtakes_a_waiter() {
        let locks = locks(Duration::from_secs(1), 8);
        let holder = locks.acquire(key("h-1")).await.unwrap();
        let mut first = Polled::new(locks.acquire(key("h-1")));
        assert!(first.is_pending());
        // The lock is now handed to `first`, which has not been polled yet: the key looks free, it is not.
        drop(holder);
        let mut newcomer = Polled::new(locks.acquire(key("h-1")));
        assert!(newcomer.is_pending());
        let first = acquired(first.poll());
        assert!(newcomer.is_pending());
        drop(first);
        drop(acquired(newcomer.poll()));
        assert_eq!(locks.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn keys_are_independent_also_for_the_same_id_of_another_type() {
        let locks = locks(Duration::from_secs(1), 8);
        let hall = locks.acquire(key("1")).await.unwrap();
        let other_hall = locks.acquire(key("2")).await.unwrap();
        let wallet = AggregateKey::new("wallet", "1");
        assert_eq!((wallet.aggregate_type(), wallet.aggregate_id()), ("wallet", "1"));
        let wallet = locks.acquire(wallet).await.unwrap();
        assert_eq!(locks.busy_keys(), 3);
        drop((hall, other_hall, wallet));
        assert_eq!(locks.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancelled_waiter_keeps_the_order_and_leaves_the_count() {
        let locks = locks(Duration::from_secs(1), 8);
        let holder = locks.acquire(key("h-1")).await.unwrap();
        let mut first = Polled::new(locks.acquire(key("h-1")));
        let mut cancelled = Polled::new(locks.acquire(key("h-1")));
        let mut third = Polled::new(locks.acquire(key("h-1")));
        assert!(first.is_pending() && cancelled.is_pending() && third.is_pending());
        assert_eq!(locks.waiting(&key("h-1")), 3);
        drop(cancelled);
        assert_eq!(locks.waiting(&key("h-1")), 2);
        drop(holder);
        assert!(third.is_pending());
        let first = acquired(first.poll());
        assert!(third.is_pending());
        drop(first);
        drop(acquired(third.poll()));
        assert_eq!(locks.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn the_last_waiter_cancelled_closes_the_line() {
        let locks = locks(Duration::from_secs(1), 8);
        let holder = locks.acquire(key("h-1")).await.unwrap();
        let mut waiter = Polled::new(locks.acquire(key("h-1")));
        assert!(waiter.is_pending());
        drop(holder);
        drop(waiter);
        assert_eq!(locks.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lock_handed_to_a_dropped_waiter_passes_to_the_next() {
        let locks = locks(Duration::from_secs(1), 8);
        let holder = locks.acquire(key("h-1")).await.unwrap();
        let mut first = Polled::new(locks.acquire(key("h-1")));
        let mut second = Polled::new(locks.acquire(key("h-1")));
        assert!(first.is_pending() && second.is_pending());
        drop(holder); // hands the lock to `first`…
        drop(first); // …which goes away before it ever sees it
        drop(acquired(second.poll()));
        assert_eq!((locks.busy_keys(), locks.waiting(&key("h-1"))), (0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn times_out_after_max_wait_uncounted_and_the_next_acquires() {
        let locks = locks(Duration::from_millis(100), 8);
        let holder = locks.acquire(key("h-1")).await.unwrap();
        let mut waiter = Polled::new(locks.acquire(key("h-1")));
        assert!(waiter.is_pending());
        tokio::time::advance(Duration::from_millis(99)).await;
        assert!(waiter.is_pending());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(waiter.poll().map(|r| r.map(drop)), Some(Err(LockRefused::TimedOut)));
        assert_eq!((locks.busy_keys(), locks.waiting(&key("h-1"))), (1, 0));
        let mut next = Polled::new(locks.acquire(key("h-1")));
        assert!(next.is_pending());
        drop(holder);
        drop(acquired(next.poll()));
        assert_eq!(locks.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_lock_handed_over_at_the_deadline_wins_over_the_timeout() {
        let locks = locks(Duration::from_millis(100), 8);
        let holder = locks.acquire(key("h-1")).await.unwrap();
        let mut waiter = Polled::new(locks.acquire(key("h-1")));
        assert!(waiter.is_pending());
        tokio::time::advance(Duration::from_millis(100)).await;
        drop(holder);
        drop(acquired(waiter.poll()));
        assert_eq!(locks.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn refuses_past_max_waiters_at_once_holder_not_counted() {
        let locks = locks(Duration::from_secs(1), 2);
        let holder = locks.acquire(key("h-1")).await.unwrap();
        let mut waiters = [Polled::new(locks.acquire(key("h-1"))), Polled::new(locks.acquire(key("h-1")))];
        assert!(waiters.iter_mut().all(Polled::is_pending));
        let mut refused = Polled::new(locks.acquire(key("h-1")));
        assert_eq!(refused.poll().map(|r| r.map(drop)), Some(Err(LockRefused::TooManyWaiters)));
        assert_eq!(locks.waiting(&key("h-1")), 2);
        drop(holder);
        let [mut first, mut second] = waiters;
        drop(acquired(first.poll()));
        drop(acquired(second.poll()));
        assert_eq!(locks.busy_keys(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn exactly_max_waiters_wait_under_parallel_entry() {
        let locks = locks(Duration::from_secs(60), 8);
        let holder = locks.acquire(key("h-1")).await.unwrap();
        let mut commands = tokio::task::JoinSet::new();
        for _ in 0..64 {
            let locks = locks.clone();
            commands.spawn(async move { locks.acquire(key("h-1")).await.map(drop) });
        }
        // While the lock is held a waiter cannot finish, so the first 56 results are all refusals.
        for _ in 0..56 {
            assert_eq!(commands.join_next().await.unwrap().unwrap(), Err(LockRefused::TooManyWaiters));
        }
        assert_eq!(locks.waiting(&key("h-1")), 8);
        drop(holder);
        while let Some(result) = commands.join_next().await {
            assert_eq!(result.unwrap(), Ok(()));
        }
        assert_eq!(locks.busy_keys(), 0);
    }

    #[tokio::test]
    async fn a_panicking_holder_releases_the_lock_to_the_next_in_line() {
        let locks = locks(Duration::from_secs(60), 8);
        let (acquired_tx, acquired_rx) = tokio::sync::oneshot::channel();
        let (panic_tx, panic_rx) = tokio::sync::oneshot::channel::<()>();
        let command = tokio::spawn({
            let locks = locks.clone();
            async move {
                let _lock = locks.acquire(key("h-1")).await.unwrap();
                acquired_tx.send(()).unwrap();
                panic_rx.await.unwrap();
                panic!("the command panicked while holding the lock");
            }
        });
        acquired_rx.await.unwrap();
        let mut waiter = Polled::new(locks.acquire(key("h-1")));
        assert!(waiter.is_pending());
        panic_tx.send(()).unwrap();
        assert!(command.await.unwrap_err().is_panic());
        drop(acquired(waiter.poll()));
        assert_eq!(locks.busy_keys(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn idle_registry_is_empty_after_parallel_churn_and_holders_never_overlap() {
        let locks = locks(Duration::from_secs(60), 1024);
        let inside: Arc<[AtomicUsize; 3]> = Arc::default();
        let mut commands = tokio::task::JoinSet::new();
        for task in 0..24 {
            let (locks, inside) = (locks.clone(), inside.clone());
            commands.spawn(async move {
                for round in 0..200 {
                    let slot = (task + round) % 3;
                    let _lock = locks.acquire(key(&slot.to_string())).await.unwrap();
                    assert_eq!(inside[slot].fetch_add(1, Ordering::SeqCst), 0, "two holders of one key");
                    tokio::task::yield_now().await;
                    inside[slot].fetch_sub(1, Ordering::SeqCst);
                }
            });
        }
        while let Some(result) = commands.join_next().await {
            result.unwrap();
        }
        assert_eq!(locks.busy_keys(), 0);
    }

    #[test]
    fn limits_reject_zero_and_default_uses_the_constants() {
        assert_eq!(LockLimits::new(Duration::ZERO, 1), Err(LockLimitsError::ZeroMaxWait));
        assert_eq!(
            LockLimits::new(Duration::from_millis(1), 0),
            Err(LockLimitsError::ZeroMaxWaiters)
        );
        let limits = LockLimits::new(Duration::from_millis(250), 3).unwrap();
        assert_eq!((limits.max_wait(), limits.max_waiters()), (Duration::from_millis(250), 3));
        let defaults = LockLimits::default();
        assert_eq!(
            (defaults.max_wait(), defaults.max_waiters()),
            (DEFAULT_LOCK_MAX_WAIT, DEFAULT_LOCK_MAX_WAITERS.get())
        );
        assert_eq!(
            (DEFAULT_LOCK_MAX_WAIT, DEFAULT_LOCK_MAX_WAITERS.get()),
            (Duration::from_secs(1), 128)
        );
        assert_eq!(
            LockLimitsError::ZeroMaxWait.to_string(),
            "the maximum wait for an aggregate lock must be positive"
        );
        assert_eq!(
            LockLimitsError::ZeroMaxWaiters.to_string(),
            "the maximum number of commands waiting for an aggregate lock must be positive"
        );
    }

    #[test]
    fn locks_keep_their_limits_and_refusals_have_stable_reasons() {
        let limits = LockLimits::new(Duration::from_millis(250), 3).unwrap();
        assert_eq!(AggregateLockRegistry::new(limits).limits(), limits);
        assert_eq!(AggregateLockRegistry::default().limits(), LockLimits::default());
        assert_eq!(LockRefused::TimedOut.reason(), "timed_out");
        assert_eq!(LockRefused::TooManyWaiters.reason(), "too_many_waiters");
        assert_eq!(
            LockRefused::TimedOut.to_string(),
            "the aggregate's lock was not acquired within the maximum wait"
        );
        assert_eq!(
            LockRefused::TooManyWaiters.to_string(),
            "too many commands already wait for the aggregate's lock"
        );
    }

    mod model {
        use super::*;
        use crate::cqrs::store::serialized_event::tests::property_config;
        use proptest::prelude::*;

        const KEYS: usize = 3;
        const MAX_WAITERS: usize = 3;
        /// 100.5 ms: a whole-millisecond clock is never exactly at a deadline, so the model and tokio's
        /// millisecond timer wheel agree on which waiters timed out.
        const MAX_WAIT: Duration = Duration::from_micros(100_500);

        #[derive(Clone, Debug)]
        enum Op {
            Acquire(usize),
            Release(usize),
            /// Releases the key and drops the waiter the lock was just handed to, before it sees it.
            ReleaseAndDropHead(usize),
            Cancel(usize),
            Advance(u64),
        }

        fn op() -> impl Strategy<Value = Op> {
            prop_oneof![
                4 => (0..KEYS).prop_map(Op::Acquire),
                2 => (0..KEYS).prop_map(Op::Release),
                1 => (0..KEYS).prop_map(Op::ReleaseAndDropHead),
                1 => any::<usize>().prop_map(Op::Cancel),
                2 => (1u64..60).prop_map(Op::Advance),
            ]
        }

        #[derive(Default)]
        struct ModelKey {
            holder: Option<u32>,
            /// Waiters in arrival order with their deadlines, as microseconds since the start.
            line: VecDeque<(u32, u128)>,
        }

        type Acquire<'a> = Polled<Pin<Box<dyn Future<Output = Result<AggregateLockGuard, LockRefused>> + 'a>>>;

        struct Waiter<'a> {
            id: u32,
            key: usize,
            future: Acquire<'a>,
        }

        /// Runs `ops` against real locks and a plain model of one holder and a FIFO line per key.
        async fn check(ops: Vec<Op>) -> Result<(), TestCaseError> {
            let locks = AggregateLockRegistry::new(LockLimits::new(MAX_WAIT, MAX_WAITERS).unwrap());
            let keys: Vec<AggregateKey> = (0..KEYS).map(|k| key(&k.to_string())).collect();
            let mut model: Vec<ModelKey> = (0..KEYS).map(|_| ModelKey::default()).collect();
            let mut holders: Vec<Option<(u32, AggregateLockGuard)>> = (0..KEYS).map(|_| None).collect();
            let mut waiters: Vec<Waiter<'_>> = Vec::new();
            let (mut now, mut next_id) = (0u128, 0u32);
            for op in ops {
                match op {
                    Op::Acquire(k) => {
                        let id = next_id;
                        next_id += 1;
                        let mut future: Acquire<'_> = Polled::new(Box::pin(locks.acquire(keys[k].clone())));
                        let first = future.poll();
                        let m = &mut model[k];
                        if m.holder.is_none() && m.line.is_empty() {
                            m.holder = Some(id);
                            holders[k] = Some((id, acquired(first)));
                        } else if m.line.len() >= MAX_WAITERS {
                            prop_assert_eq!(first.map(|r| r.map(drop)), Some(Err(LockRefused::TooManyWaiters)));
                        } else {
                            prop_assert!(first.is_none(), "a waiter must wait");
                            m.line.push_back((id, now + MAX_WAIT.as_micros()));
                            waiters.push(Waiter { id, key: k, future });
                        }
                    }
                    Op::Release(k) | Op::ReleaseAndDropHead(k) => {
                        if holders[k].take().is_none() {
                            continue;
                        }
                        let m = &mut model[k];
                        if matches!(op, Op::ReleaseAndDropHead(_))
                            && let Some((head, _)) = m.line.pop_front()
                        {
                            waiters.retain(|w| w.id != head);
                        }
                        m.holder = m.line.pop_front().map(|(id, _)| id);
                    }
                    Op::Cancel(pick) => {
                        if waiters.is_empty() {
                            continue;
                        }
                        let gone = waiters.remove(pick % waiters.len());
                        model[gone.key].line.retain(|(id, _)| *id != gone.id);
                    }
                    Op::Advance(ms) => {
                        tokio::time::advance(Duration::from_millis(ms)).await;
                        now += u128::from(ms) * 1000;
                    }
                }
                // Every waiter is polled after every step: the one handed a lock takes it, the expired ones leave.
                let mut still = Vec::new();
                for mut waiter in waiters.drain(..) {
                    let output = waiter.future.poll();
                    let m = &mut model[waiter.key];
                    if m.holder == Some(waiter.id) {
                        holders[waiter.key] = Some((waiter.id, acquired(output)));
                    } else if m.line.iter().any(|(id, deadline)| *id == waiter.id && *deadline <= now) {
                        prop_assert_eq!(output.map(|r| r.map(drop)), Some(Err(LockRefused::TimedOut)));
                        m.line.retain(|(id, _)| *id != waiter.id);
                    } else {
                        prop_assert!(output.is_none(), "waiter {} acquired out of order", waiter.id);
                        still.push(waiter);
                    }
                }
                waiters = still;
                for (k, m) in model.iter().enumerate() {
                    prop_assert_eq!(holders[k].as_ref().map(|(id, _)| *id), m.holder);
                    prop_assert_eq!(locks.waiting(&keys[k]), m.line.len());
                }
                let busy = model.iter().filter(|m| m.holder.is_some() || !m.line.is_empty()).count();
                prop_assert_eq!(locks.busy_keys(), busy);
            }
            drop(waiters);
            drop(holders);
            prop_assert_eq!(locks.busy_keys(), 0);
            Ok(())
        }

        proptest! {
            #![proptest_config(proptest::test_runner::Config { cases: 256, ..property_config() })]
            #[test]
            fn property_locks_match_a_fifo_model(ops in proptest::collection::vec(op(), 1..60)) {
                let runtime = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
                runtime.block_on(check(ops))?;
            }
        }
    }
}
