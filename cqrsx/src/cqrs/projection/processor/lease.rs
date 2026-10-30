//! The lease keeper of an owner: extends the checkpoint's lease while the owner works and notices when the
//! lease is lost, by the store's answer or by the local deadline.
use crate::{
    cqrs::projection::{
        checkpoint::{CheckpointKey, CheckpointStore, LeaseToken},
        processor::Operation,
    },
    locking::lock,
};
use std::{
    pin::pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Notify, task::JoinHandle, time::Instant};
use tracing::instrument::WithSubscriber;

/// Aborts the task when dropped: the keeper never outlives its owner.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Shared {
    lost: AtomicBool,
    /// Conservative: anchored before the request that granted or extended the lease.
    deadline: Mutex<Instant>,
    changed: Notify,
}

impl Shared {
    fn deadline(&self) -> Instant {
        *lock(&self.deadline, "projection lease deadline")
    }

    fn is_lost(&self) -> bool {
        self.lost.load(Ordering::SeqCst) || Instant::now() >= self.deadline()
    }

    fn mark_lost(&self) {
        self.lost.store(true, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

/// Keeps one lease alive: a task extends it every third of its length. The lease counts as lost when the
/// store refuses an extension, or when the local deadline passes without a successful one (the store was
/// unreachable long enough for another node to claim the row).
pub(super) struct LeaseKeeper {
    shared: Arc<Shared>,
    _task: AbortOnDrop,
}

impl LeaseKeeper {
    /// `anchor` is the instant before the claim request: the lease holds at least until `anchor + lease`.
    pub(super) fn spawn<C: CheckpointStore + 'static>(
        store: Arc<C>,
        key: CheckpointKey,
        token: LeaseToken,
        lease: Duration,
        anchor: Instant,
    ) -> Self {
        let shared = Arc::new(Shared {
            lost: AtomicBool::new(false),
            deadline: Mutex::new(anchor + lease),
            changed: Notify::new(),
        });
        let task = tokio::spawn(keep(store, key, token, lease, shared.clone()).with_current_subscriber());
        Self {
            shared,
            _task: AbortOnDrop(task),
        }
    }

    pub(super) fn is_lost(&self) -> bool {
        self.shared.is_lost()
    }

    /// Resolves once the lease is lost.
    pub(super) async fn lost(&self) {
        loop {
            let mut changed = pin!(self.shared.changed.notified());
            changed.as_mut().enable();
            if self.shared.is_lost() {
                return;
            }
            let deadline = self.shared.deadline();
            tokio::select! {
                _ = &mut changed => {}
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }
    }
}

async fn keep<C: CheckpointStore>(store: Arc<C>, key: CheckpointKey, token: LeaseToken, lease: Duration, shared: Arc<Shared>) {
    let every = lease / 3;
    loop {
        tokio::time::sleep(every).await;
        let anchor = Instant::now();
        match store.extend(&key, token, lease).await {
            Ok(true) => {
                *lock(&shared.deadline, "projection lease deadline") = anchor + lease;
                shared.changed.notify_waiters();
            }
            Ok(false) => {
                shared.mark_lost();
                return;
            }
            Err(error) => {
                crate::diagnostics::log_projection_failure(
                    key.projection(),
                    key.aggregate_type(),
                    Operation::Lease,
                    error.category(),
                    0,
                );
                if shared.is_lost() {
                    shared.mark_lost();
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cqrs::testing::mem_checkpoint_store::MemCheckpointStore;

    const LEASE: Duration = Duration::from_secs(3);

    async fn held(store: &MemCheckpointStore) -> chrono::DateTime<chrono::Utc> {
        store.status(None).await.unwrap()[0].locked_until.expect("a held row")
    }

    async fn claimed(store: &MemCheckpointStore, key: &CheckpointKey, owner: &str) -> (LeaseToken, Instant) {
        let anchor = Instant::now();
        let claimed = store.claim(key, owner, LEASE).await.unwrap().expect("a free row");
        (claimed.token, anchor)
    }

    #[tokio::test(start_paused = true)]
    async fn the_keeper_extends_every_third_of_the_lease_and_stops_with_its_owner() {
        let store = Arc::new(MemCheckpointStore::default());
        let key = CheckpointKey::new("halls", "Concert");
        store.register(&key).await.unwrap();
        let (token, anchor) = claimed(&store, &key, "a").await;
        let before = held(&store).await;
        let keeper = LeaseKeeper::spawn(store.clone(), key.clone(), token, LEASE, anchor);
        tokio::task::yield_now().await; // the keeper starts its first pause
        tokio::time::advance(Duration::from_millis(1100)).await;
        tokio::task::yield_now().await;
        assert!(held(&store).await > before, "extended after lease / 3");
        assert!(!keeper.is_lost());
        for _ in 0..6 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        assert!(!keeper.is_lost(), "kept alive well past the first lease");
        let last = held(&store).await;
        drop(keeper);
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        assert_eq!(held(&store).await, last, "a dropped keeper extends nothing");
    }

    #[tokio::test(start_paused = true)]
    async fn a_refused_extension_loses_the_lease_at_once() {
        let store = Arc::new(MemCheckpointStore::default());
        let key = CheckpointKey::new("halls", "Concert");
        store.register(&key).await.unwrap();
        let (token, anchor) = claimed(&store, &key, "a").await;
        let keeper = LeaseKeeper::spawn(store.clone(), key.clone(), token, LEASE, anchor);
        store.expire_lease(&key);
        claimed(&store, &key, "b").await; // another node took the row
        let lost = tokio::time::timeout(Duration::from_secs(2), keeper.lost()).await;
        assert!(lost.is_ok() && keeper.is_lost(), "the next extension is refused");
    }

    #[tokio::test(start_paused = true)]
    async fn an_unreachable_store_loses_the_lease_only_when_the_local_deadline_passes() {
        let store = Arc::new(MemCheckpointStore::default());
        let key = CheckpointKey::new("halls", "Concert");
        store.register(&key).await.unwrap();
        let (token, anchor) = claimed(&store, &key, "a").await;
        let keeper = LeaseKeeper::spawn(store.clone(), key.clone(), token, LEASE, anchor);
        store.set_unavailable(true);
        tokio::time::advance(Duration::from_millis(2500)).await;
        tokio::task::yield_now().await;
        assert!(!keeper.is_lost(), "two failed extensions, deadline not reached");
        let mut waiting = std::pin::pin!(keeper.lost());
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(waiting.as_mut().poll(cx).is_pending())).await,
            "lost() waits for the deadline"
        );
        tokio::time::advance(Duration::from_millis(600)).await;
        tokio::task::yield_now().await;
        assert!(keeper.is_lost(), "past anchor + lease without an extension");
        tokio::time::timeout(Duration::from_secs(1), waiting).await.unwrap();
        store.set_unavailable(false);
    }
}
