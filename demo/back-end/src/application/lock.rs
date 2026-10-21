//! The box-office lock: held while the read models are being rebuilt. UI commands answer 423 and
//! chain tasks wait a little, then retry. The write side never depends on the read side, so the
//! lock is only what keeps a demo rebuild simple and deterministic (see `rebuild`).
//!
//! A command is admitted with a [`CommandPass`] and holds it until it has finished, its inline
//! projection included. A rebuild takes the lock and then waits for every pass already given out
//! ([`BoxOfficeLock::wait_idle`]): an admitted command may still wait for its aggregate's turn, and
//! its projection must not write into a read model that is being wiped and replayed.
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::Notify;

#[derive(Debug, Default)]
pub struct BoxOfficeLock {
    locked: AtomicBool,
    /// Commands admitted and not finished yet.
    in_flight: AtomicUsize,
    idle: Notify,
}

impl BoxOfficeLock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_locked(&self) -> bool {
        self.locked.load(Ordering::SeqCst)
    }

    /// Takes the lock; `None` when it is already held. The guard releases it on drop, on every
    /// exit path including a panic of the holder.
    pub fn try_acquire(self: &Arc<Self>) -> Option<LockGuard> {
        (!self.locked.swap(true, Ordering::SeqCst)).then(|| LockGuard(self.clone()))
    }

    /// Admits a command unless the lock is held. A command counts itself first and checks the lock
    /// second, a rebuild locks first and counts second (all `SeqCst`), so the two never miss each
    /// other: either the command sees the lock, or the rebuild waits for the command.
    pub fn admit(self: &Arc<Self>) -> Option<CommandPass> {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        let pass = CommandPass(self.clone());
        (!self.is_locked()).then_some(pass) // a refused pass uncounts itself on drop
    }

    /// Waits until every admitted command has finished. Called with the lock held, it waits only
    /// for the commands admitted before the lock was taken: no new one is admitted.
    pub async fn wait_idle(&self) {
        loop {
            let mut notified = std::pin::pin!(self.idle.notified());
            // Registered before the check, so a pass dropped in between still wakes this waiter.
            notified.as_mut().enable();
            if self.in_flight.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }
}

#[must_use = "the lock is released as soon as the guard is dropped"]
pub struct LockGuard(Arc<BoxOfficeLock>);

impl Drop for LockGuard {
    fn drop(&mut self) {
        self.0.locked.store(false, Ordering::SeqCst);
    }
}

/// An admitted command; a rebuild does not start while one exists.
#[must_use = "the command counts as finished as soon as the pass is dropped"]
pub struct CommandPass(Arc<BoxOfficeLock>);

impl Drop for CommandPass {
    fn drop(&mut self) {
        if self.0.in_flight.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::Future, task::Poll};

    async fn is_pending(mut future: std::pin::Pin<&mut impl Future>) -> bool {
        std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await
    }

    #[test]
    fn lock_is_exclusive_and_released_by_the_guard() {
        let lock = Arc::new(BoxOfficeLock::new());
        assert!(!lock.is_locked());
        let guard = lock.try_acquire().expect("free lock");
        assert!(lock.is_locked());
        assert!(lock.try_acquire().is_none(), "second holder is refused");
        drop(guard);
        assert!(!lock.is_locked());
        assert!(lock.try_acquire().is_some());
    }

    #[tokio::test]
    async fn admit_refuses_under_the_lock_and_a_refused_command_is_not_waited_for() {
        let lock = Arc::new(BoxOfficeLock::new());
        let guard = lock.try_acquire().unwrap();
        assert!(lock.admit().is_none());
        lock.wait_idle().await;
        drop(guard);
        assert!(lock.admit().is_some());
    }

    #[tokio::test]
    async fn wait_idle_returns_once_every_admitted_command_has_finished() {
        let lock = Arc::new(BoxOfficeLock::new());
        let (first, second) = (lock.admit().unwrap(), lock.admit().unwrap());
        let _guard = lock.try_acquire().unwrap();
        let mut idle = std::pin::pin!(lock.wait_idle());
        assert!(is_pending(idle.as_mut()).await);
        drop(first);
        assert!(is_pending(idle.as_mut()).await, "one command is still running");
        drop(second);
        idle.await;
    }
}
