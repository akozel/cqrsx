//! Command turns: inside one process, commands to one aggregate run one at a time, in arrival order.
//!
//! Every command waits for its aggregate's turn in its own task and runs once the turn comes. Different
//! aggregates stay parallel. Waiting is bounded twice: by time ([`TurnLimits::max_wait`]) and by the number
//! of commands that wait for one aggregate ([`TurnLimits::max_waiting`]); past either bound the command is
//! refused before it runs. Turns order commands inside a process only: between processes, and between
//! executors with their own [`AggregateTurns`], the store's optimistic check still decides.
//!
//! Invariants:
//! - at most one command holds the turn of a key;
//! - waiting commands take the turn first in, first out, skipping those that gave up;
//! - a key has a registry entry exactly while a command holds or awaits its turn, so an idle registry is empty;
//! - the commands counted as waiting for a key never exceed `max_waiting`; the holder is not counted.
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

/// How long a command waits for its aggregate's turn unless configured otherwise.
pub const DEFAULT_TURN_MAX_WAIT: Duration = Duration::from_secs(1);
/// How many commands may wait for one aggregate's turn unless configured otherwise.
pub const DEFAULT_TURN_MAX_WAITING: NonZeroUsize = NonZeroUsize::new(128).unwrap();

/// How commands to one aggregate run inside this process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandOrdering {
    /// Concurrently: the store's optimistic check resolves races and the conflict policy retries the losers.
    Concurrent,
    /// One at a time per aggregate key, in arrival order; different keys stay parallel.
    PerAggregate(TurnLimits),
}

impl CommandOrdering {
    /// A new turn registry for `PerAggregate`, none for `Concurrent`. Build it once per executor: commands
    /// order each other only through the same registry.
    pub fn turns(self) -> Option<AggregateTurns> {
        match self {
            Self::Concurrent => None,
            Self::PerAggregate(limits) => Some(AggregateTurns::new(limits)),
        }
    }
}

/// Checked bounds of waiting for a turn: how long one command waits, and how many wait per key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TurnLimits {
    max_wait: Duration,
    max_waiting: NonZeroUsize,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum TurnLimitsError {
    #[error("the maximum wait for a turn must be positive")]
    ZeroMaxWait,
    #[error("the maximum number of commands waiting for a turn must be positive")]
    ZeroMaxWaiting,
}

impl TurnLimits {
    pub fn new(max_wait: Duration, max_waiting: usize) -> Result<Self, TurnLimitsError> {
        if max_wait.is_zero() {
            return Err(TurnLimitsError::ZeroMaxWait);
        }
        Ok(Self {
            max_wait,
            max_waiting: NonZeroUsize::new(max_waiting).ok_or(TurnLimitsError::ZeroMaxWaiting)?,
        })
    }

    /// How long a command waits for the turn before it is refused with [`TurnRefused::TimedOut`].
    pub fn max_wait(&self) -> Duration {
        self.max_wait
    }

    /// How many commands may wait for one key; the next one is refused at once with
    /// [`TurnRefused::TooManyWaiting`]. The command holding the turn is not counted.
    pub fn max_waiting(&self) -> usize {
        self.max_waiting.get()
    }
}

impl Default for TurnLimits {
    /// [`DEFAULT_TURN_MAX_WAIT`] and [`DEFAULT_TURN_MAX_WAITING`].
    fn default() -> Self {
        Self {
            max_wait: DEFAULT_TURN_MAX_WAIT,
            max_waiting: DEFAULT_TURN_MAX_WAITING,
        }
    }
}

/// Why a command did not get its aggregate's turn. Nothing ran and nothing was written.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum TurnRefused {
    #[error("the aggregate's turn did not come within the maximum wait")]
    TimedOut,
    #[error("too many commands already wait for the aggregate's turn")]
    TooManyWaiting,
}

impl TurnRefused {
    /// A stable label for logs.
    pub fn reason(self) -> &'static str {
        match self {
            Self::TimedOut => "timed_out",
            Self::TooManyWaiting => "too_many_waiting",
        }
    }
}

/// The line of one aggregate key. Tokio's mutex queues its waiters first in, first out and hands a
/// released turn straight to the first of them, so the waiter list of `turn` *is* the line of the key.
#[derive(Default)]
struct Lane {
    turn: Arc<tokio::sync::Mutex<()>>,
    /// Commands in the line, not counting the one holding the turn.
    waiting: AtomicUsize,
}

struct Entry {
    lane: Arc<Lane>,
    /// Live [`LaneRef`]s: the commands holding or awaiting the turn. Changed only under the registry lock.
    users: usize,
}

/// One line per aggregate key that is busy right now. The std mutex guards only the map and is held
/// for a lookup, an insert or a removal, never across an `.await`.
type Registry = Mutex<HashMap<AggregateKey, Entry>>;

fn lock(registry: &Registry) -> MutexGuard<'_, HashMap<AggregateKey, Entry>> {
    // Nothing panics under this lock, and the map stays consistent even if something did.
    registry.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One FIFO turn per aggregate key. Clones share the registry; a key's entry lives while its turn is held
/// or awaited, so memory follows the keys that are busy right now, not every key ever seen.
#[derive(Clone)]
pub struct AggregateTurns {
    limits: TurnLimits,
    registry: Arc<Registry>,
}

impl AggregateTurns {
    pub fn new(limits: TurnLimits) -> Self {
        Self {
            limits,
            registry: Arc::default(),
        }
    }

    pub fn limits(&self) -> TurnLimits {
        self.limits
    }

    /// Waits for the key's turn behind the commands that came before, at most
    /// [`max_wait`](TurnLimits::max_wait); refuses at once when [`max_waiting`](TurnLimits::max_waiting)
    /// commands already wait. Dropping the returned turn hands it to the next command in line; dropping
    /// this future while it waits leaves the line without disturbing the others.
    pub async fn enter(&self, key: AggregateKey) -> Result<AggregateTurn, TurnRefused> {
        let lane = self.lane(key);
        // A free turn has nobody waiting for it (a released turn goes straight to the first waiter), so taking
        // it here never overtakes anyone. The common case of an idle key pays for no slot and no timer.
        if let Ok(guard) = lane.lane.turn.clone().try_lock_owned() {
            return Ok(AggregateTurn { _guard: guard, lane });
        }
        let guard = {
            let _slot = WaitingSlot::join(&lane.lane.waiting, self.limits.max_waiting).ok_or(TurnRefused::TooManyWaiting)?;
            tokio::time::timeout(self.limits.max_wait, lane.lane.turn.clone().lock_owned())
                .await
                .map_err(|_| TurnRefused::TimedOut)?
        }; // `_slot` leaves the count here: this command no longer waits, it holds the turn
        Ok(AggregateTurn { _guard: guard, lane })
    }

    /// Finds the key's line, or opens one if nobody holds or awaits the key.
    fn lane(&self, key: AggregateKey) -> LaneRef {
        let mut registry = lock(&self.registry);
        let lane = match registry.get_mut(&key) {
            Some(entry) => {
                entry.users += 1; // someone holds or awaits this key: join their line
                entry.lane.clone()
            }
            None => {
                let lane = Arc::<Lane>::default(); // the first command for the key opens a line
                registry.insert(
                    key.clone(),
                    Entry {
                        lane: lane.clone(),
                        users: 1,
                    },
                );
                lane
            }
        };
        drop(registry);
        LaneRef {
            registry: self.registry.clone(),
            key,
            lane,
        }
    }

    #[cfg(test)]
    fn busy_keys(&self) -> usize {
        lock(&self.registry).len()
    }

    #[cfg(test)]
    fn waiting(&self, key: &AggregateKey) -> usize {
        lock(&self.registry)
            .get(key)
            .map_or(0, |entry| entry.lane.waiting.load(Ordering::SeqCst))
    }
}

/// The key's turn, held while one command runs; dropping it hands the turn to the next command in line.
/// Fields drop in declaration order: first the guard (tokio passes the turn on), then the line reference
/// (which closes the line if nobody else holds or awaits it). So a turn never outlives its line, and a key
/// never has two lines with a held turn. Never `mem::forget` it: the key would stay taken.
#[must_use = "the turn passes to the next command as soon as it is dropped"]
pub struct AggregateTurn {
    _guard: OwnedMutexGuard<()>,
    lane: LaneRef,
}

impl fmt::Debug for AggregateTurn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateTurn").field("key", &self.lane.key).finish()
    }
}

/// A reference to a key's line, held by every command that waits in it or holds its turn.
struct LaneRef {
    registry: Arc<Registry>,
    key: AggregateKey,
    lane: Arc<Lane>,
}

impl Drop for LaneRef {
    fn drop(&mut self) {
        let mut registry = lock(&self.registry);
        // References are counted only under this lock, so "nobody left" is exact: two commands leaving
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

/// Counts one command in the line and uncounts it on every exit: turn acquired, timed out, or the
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

    fn turns(max_wait: Duration, max_waiting: usize) -> AggregateTurns {
        AggregateTurns::new(TurnLimits::new(max_wait, max_waiting).unwrap())
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

    fn entered(output: Option<Result<AggregateTurn, TurnRefused>>) -> AggregateTurn {
        match output {
            Some(Ok(turn)) => turn,
            other => panic!("expected the turn, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn uncontended_key_enters_at_once_and_leaves_no_entry() {
        let turns = turns(Duration::from_secs(1), 1);
        let turn = turns.enter(key("h-1")).await.unwrap();
        assert_eq!(
            format!("{turn:?}"),
            r#"AggregateTurn { key: AggregateKey { aggregate_type: "hall", aggregate_id: "h-1" } }"#
        );
        assert_eq!((turns.busy_keys(), turns.waiting(&key("h-1"))), (1, 0));
        drop(turn);
        assert_eq!(turns.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn one_holder_per_key_and_waiters_enter_in_arrival_order() {
        let turns = turns(Duration::from_secs(1), 8);
        let mut holder = turns.enter(key("h-1")).await.unwrap();
        let mut waiters: Vec<_> = (0..5).map(|_| Polled::new(turns.enter(key("h-1")))).collect();
        for waiter in &mut waiters {
            assert!(waiter.is_pending());
        }
        assert_eq!(turns.waiting(&key("h-1")), 5);
        for next in 0..waiters.len() {
            for waiter in &mut waiters[next..] {
                assert!(waiter.is_pending(), "nobody enters while the turn is held");
            }
            drop(holder);
            holder = entered(waiters[next].poll());
            assert_eq!(turns.waiting(&key("h-1")), 4 - next);
        }
        drop(holder);
        assert_eq!(turns.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_newcomer_never_overtakes_a_waiter() {
        let turns = turns(Duration::from_secs(1), 8);
        let holder = turns.enter(key("h-1")).await.unwrap();
        let mut first = Polled::new(turns.enter(key("h-1")));
        assert!(first.is_pending());
        // The turn is now handed to `first`, which has not been polled yet: the key looks free, it is not.
        drop(holder);
        let mut newcomer = Polled::new(turns.enter(key("h-1")));
        assert!(newcomer.is_pending());
        let first = entered(first.poll());
        assert!(newcomer.is_pending());
        drop(first);
        drop(entered(newcomer.poll()));
        assert_eq!(turns.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn keys_are_independent_also_for_the_same_id_of_another_type() {
        let turns = turns(Duration::from_secs(1), 8);
        let hall = turns.enter(key("1")).await.unwrap();
        let other_hall = turns.enter(key("2")).await.unwrap();
        let wallet = AggregateKey::new("wallet", "1");
        assert_eq!((wallet.aggregate_type(), wallet.aggregate_id()), ("wallet", "1"));
        let wallet = turns.enter(wallet).await.unwrap();
        assert_eq!(turns.busy_keys(), 3);
        drop((hall, other_hall, wallet));
        assert_eq!(turns.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_cancelled_waiter_keeps_the_order_and_leaves_the_count() {
        let turns = turns(Duration::from_secs(1), 8);
        let holder = turns.enter(key("h-1")).await.unwrap();
        let mut first = Polled::new(turns.enter(key("h-1")));
        let mut cancelled = Polled::new(turns.enter(key("h-1")));
        let mut third = Polled::new(turns.enter(key("h-1")));
        assert!(first.is_pending() && cancelled.is_pending() && third.is_pending());
        assert_eq!(turns.waiting(&key("h-1")), 3);
        drop(cancelled);
        assert_eq!(turns.waiting(&key("h-1")), 2);
        drop(holder);
        assert!(third.is_pending());
        let first = entered(first.poll());
        assert!(third.is_pending());
        drop(first);
        drop(entered(third.poll()));
        assert_eq!(turns.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn the_last_waiter_cancelled_closes_the_line() {
        let turns = turns(Duration::from_secs(1), 8);
        let holder = turns.enter(key("h-1")).await.unwrap();
        let mut waiter = Polled::new(turns.enter(key("h-1")));
        assert!(waiter.is_pending());
        drop(holder);
        drop(waiter);
        assert_eq!(turns.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_turn_handed_to_a_dropped_waiter_passes_to_the_next() {
        let turns = turns(Duration::from_secs(1), 8);
        let holder = turns.enter(key("h-1")).await.unwrap();
        let mut first = Polled::new(turns.enter(key("h-1")));
        let mut second = Polled::new(turns.enter(key("h-1")));
        assert!(first.is_pending() && second.is_pending());
        drop(holder); // hands the turn to `first`…
        drop(first); // …which goes away before it ever sees it
        drop(entered(second.poll()));
        assert_eq!((turns.busy_keys(), turns.waiting(&key("h-1"))), (0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn times_out_after_max_wait_uncounted_and_the_next_enters() {
        let turns = turns(Duration::from_millis(100), 8);
        let holder = turns.enter(key("h-1")).await.unwrap();
        let mut waiter = Polled::new(turns.enter(key("h-1")));
        assert!(waiter.is_pending());
        tokio::time::advance(Duration::from_millis(99)).await;
        assert!(waiter.is_pending());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(waiter.poll().map(|r| r.map(drop)), Some(Err(TurnRefused::TimedOut)));
        assert_eq!((turns.busy_keys(), turns.waiting(&key("h-1"))), (1, 0));
        let mut next = Polled::new(turns.enter(key("h-1")));
        assert!(next.is_pending());
        drop(holder);
        drop(entered(next.poll()));
        assert_eq!(turns.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_turn_handed_over_at_the_deadline_wins_over_the_timeout() {
        let turns = turns(Duration::from_millis(100), 8);
        let holder = turns.enter(key("h-1")).await.unwrap();
        let mut waiter = Polled::new(turns.enter(key("h-1")));
        assert!(waiter.is_pending());
        tokio::time::advance(Duration::from_millis(100)).await;
        drop(holder);
        drop(entered(waiter.poll()));
        assert_eq!(turns.busy_keys(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn refuses_past_max_waiting_at_once_holder_not_counted() {
        let turns = turns(Duration::from_secs(1), 2);
        let holder = turns.enter(key("h-1")).await.unwrap();
        let mut waiters = [Polled::new(turns.enter(key("h-1"))), Polled::new(turns.enter(key("h-1")))];
        assert!(waiters.iter_mut().all(Polled::is_pending));
        let mut refused = Polled::new(turns.enter(key("h-1")));
        assert_eq!(refused.poll().map(|r| r.map(drop)), Some(Err(TurnRefused::TooManyWaiting)));
        assert_eq!(turns.waiting(&key("h-1")), 2);
        drop(holder);
        let [mut first, mut second] = waiters;
        drop(entered(first.poll()));
        drop(entered(second.poll()));
        assert_eq!(turns.busy_keys(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn exactly_max_waiting_wait_under_parallel_entry() {
        let turns = turns(Duration::from_secs(60), 8);
        let holder = turns.enter(key("h-1")).await.unwrap();
        let mut commands = tokio::task::JoinSet::new();
        for _ in 0..64 {
            let turns = turns.clone();
            commands.spawn(async move { turns.enter(key("h-1")).await.map(drop) });
        }
        // While the turn is held a waiter cannot finish, so the first 56 results are all refusals.
        for _ in 0..56 {
            assert_eq!(commands.join_next().await.unwrap().unwrap(), Err(TurnRefused::TooManyWaiting));
        }
        assert_eq!(turns.waiting(&key("h-1")), 8);
        drop(holder);
        while let Some(result) = commands.join_next().await {
            assert_eq!(result.unwrap(), Ok(()));
        }
        assert_eq!(turns.busy_keys(), 0);
    }

    #[tokio::test]
    async fn a_panicking_holder_releases_the_turn_to_the_next_in_line() {
        let turns = turns(Duration::from_secs(60), 8);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (panic_tx, panic_rx) = tokio::sync::oneshot::channel::<()>();
        let command = tokio::spawn({
            let turns = turns.clone();
            async move {
                let _turn = turns.enter(key("h-1")).await.unwrap();
                entered_tx.send(()).unwrap();
                panic_rx.await.unwrap();
                panic!("the command panicked while holding the turn");
            }
        });
        entered_rx.await.unwrap();
        let mut waiter = Polled::new(turns.enter(key("h-1")));
        assert!(waiter.is_pending());
        panic_tx.send(()).unwrap();
        assert!(command.await.unwrap_err().is_panic());
        drop(entered(waiter.poll()));
        assert_eq!(turns.busy_keys(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn idle_registry_is_empty_after_parallel_churn_and_holders_never_overlap() {
        let turns = turns(Duration::from_secs(60), 1024);
        let inside: Arc<[AtomicUsize; 3]> = Arc::default();
        let mut commands = tokio::task::JoinSet::new();
        for task in 0..24 {
            let (turns, inside) = (turns.clone(), inside.clone());
            commands.spawn(async move {
                for round in 0..200 {
                    let slot = (task + round) % 3;
                    let _turn = turns.enter(key(&slot.to_string())).await.unwrap();
                    assert_eq!(inside[slot].fetch_add(1, Ordering::SeqCst), 0, "two holders of one key");
                    tokio::task::yield_now().await;
                    inside[slot].fetch_sub(1, Ordering::SeqCst);
                }
            });
        }
        while let Some(result) = commands.join_next().await {
            result.unwrap();
        }
        assert_eq!(turns.busy_keys(), 0);
    }

    #[test]
    fn limits_reject_zero_and_default_uses_the_constants() {
        assert_eq!(TurnLimits::new(Duration::ZERO, 1), Err(TurnLimitsError::ZeroMaxWait));
        assert_eq!(
            TurnLimits::new(Duration::from_millis(1), 0),
            Err(TurnLimitsError::ZeroMaxWaiting)
        );
        let limits = TurnLimits::new(Duration::from_millis(250), 3).unwrap();
        assert_eq!((limits.max_wait(), limits.max_waiting()), (Duration::from_millis(250), 3));
        let defaults = TurnLimits::default();
        assert_eq!(
            (defaults.max_wait(), defaults.max_waiting()),
            (DEFAULT_TURN_MAX_WAIT, DEFAULT_TURN_MAX_WAITING.get())
        );
        assert_eq!(
            (DEFAULT_TURN_MAX_WAIT, DEFAULT_TURN_MAX_WAITING.get()),
            (Duration::from_secs(1), 128)
        );
        assert_eq!(
            TurnLimitsError::ZeroMaxWait.to_string(),
            "the maximum wait for a turn must be positive"
        );
        assert_eq!(
            TurnLimitsError::ZeroMaxWaiting.to_string(),
            "the maximum number of commands waiting for a turn must be positive"
        );
    }

    #[test]
    fn ordering_builds_turns_only_per_aggregate_and_refusals_have_stable_reasons() {
        assert!(CommandOrdering::Concurrent.turns().is_none());
        let limits = TurnLimits::new(Duration::from_millis(250), 3).unwrap();
        assert_eq!(CommandOrdering::PerAggregate(limits).turns().unwrap().limits(), limits);
        assert_eq!(TurnRefused::TimedOut.reason(), "timed_out");
        assert_eq!(TurnRefused::TooManyWaiting.reason(), "too_many_waiting");
        assert_eq!(
            TurnRefused::TimedOut.to_string(),
            "the aggregate's turn did not come within the maximum wait"
        );
        assert_eq!(
            TurnRefused::TooManyWaiting.to_string(),
            "too many commands already wait for the aggregate's turn"
        );
    }

    mod model {
        use super::*;
        use crate::cqrs::store::serialized_event::tests::property_config;
        use proptest::prelude::*;

        const KEYS: usize = 3;
        const MAX_WAITING: usize = 3;
        /// 100.5 ms: a whole-millisecond clock is never exactly at a deadline, so the model and tokio's
        /// millisecond timer wheel agree on which waiters timed out.
        const MAX_WAIT: Duration = Duration::from_micros(100_500);

        #[derive(Clone, Debug)]
        enum Op {
            Enter(usize),
            Release(usize),
            /// Releases the key and drops the waiter the turn was just handed to, before it sees it.
            ReleaseAndDropHead(usize),
            Cancel(usize),
            Advance(u64),
        }

        fn op() -> impl Strategy<Value = Op> {
            prop_oneof![
                4 => (0..KEYS).prop_map(Op::Enter),
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

        type Enter<'a> = Polled<Pin<Box<dyn Future<Output = Result<AggregateTurn, TurnRefused>> + 'a>>>;

        struct Waiter<'a> {
            id: u32,
            key: usize,
            future: Enter<'a>,
        }

        /// Runs `ops` against real turns and a plain model of one holder and a FIFO line per key.
        async fn check(ops: Vec<Op>) -> Result<(), TestCaseError> {
            let turns = AggregateTurns::new(TurnLimits::new(MAX_WAIT, MAX_WAITING).unwrap());
            let keys: Vec<AggregateKey> = (0..KEYS).map(|k| key(&k.to_string())).collect();
            let mut model: Vec<ModelKey> = (0..KEYS).map(|_| ModelKey::default()).collect();
            let mut holders: Vec<Option<(u32, AggregateTurn)>> = (0..KEYS).map(|_| None).collect();
            let mut waiters: Vec<Waiter<'_>> = Vec::new();
            let (mut now, mut next_id) = (0u128, 0u32);
            for op in ops {
                match op {
                    Op::Enter(k) => {
                        let id = next_id;
                        next_id += 1;
                        let mut future: Enter<'_> = Polled::new(Box::pin(turns.enter(keys[k].clone())));
                        let first = future.poll();
                        let m = &mut model[k];
                        if m.holder.is_none() && m.line.is_empty() {
                            m.holder = Some(id);
                            holders[k] = Some((id, entered(first)));
                        } else if m.line.len() >= MAX_WAITING {
                            prop_assert_eq!(first.map(|r| r.map(drop)), Some(Err(TurnRefused::TooManyWaiting)));
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
                // Every waiter is polled after every step: the one handed a turn takes it, the expired ones leave.
                let mut still = Vec::new();
                for mut waiter in waiters.drain(..) {
                    let output = waiter.future.poll();
                    let m = &mut model[waiter.key];
                    if m.holder == Some(waiter.id) {
                        holders[waiter.key] = Some((waiter.id, entered(output)));
                    } else if m.line.iter().any(|(id, deadline)| *id == waiter.id && *deadline <= now) {
                        prop_assert_eq!(output.map(|r| r.map(drop)), Some(Err(TurnRefused::TimedOut)));
                        m.line.retain(|(id, _)| *id != waiter.id);
                    } else {
                        prop_assert!(output.is_none(), "waiter {} entered out of turn", waiter.id);
                        still.push(waiter);
                    }
                }
                waiters = still;
                for (k, m) in model.iter().enumerate() {
                    prop_assert_eq!(holders[k].as_ref().map(|(id, _)| *id), m.holder);
                    prop_assert_eq!(turns.waiting(&keys[k]), m.line.len());
                }
                let busy = model.iter().filter(|m| m.holder.is_some() || !m.line.is_empty()).count();
                prop_assert_eq!(turns.busy_keys(), busy);
            }
            drop(waiters);
            drop(holders);
            prop_assert_eq!(turns.busy_keys(), 0);
            Ok(())
        }

        proptest! {
            #![proptest_config(proptest::test_runner::Config { cases: 256, ..property_config() })]
            #[test]
            fn turns_match_a_fifo_model(ops in proptest::collection::vec(op(), 1..60)) {
                let runtime = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
                runtime.block_on(check(ops))?;
            }
        }
    }
}
