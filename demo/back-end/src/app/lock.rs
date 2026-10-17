//! The box-office lock: held while the read models are being rebuilt. UI commands answer 423 and
//! chain tasks wait a little, then retry. The write side never depends on the read side, so the
//! lock is only what keeps a demo rebuild simple and deterministic (see `rebuild`).
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Default)]
pub struct BoxOfficeLock {
    locked: AtomicBool,
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
}

#[must_use = "the lock is released as soon as the guard is dropped"]
pub struct LockGuard(Arc<BoxOfficeLock>);

impl Drop for LockGuard {
    fn drop(&mut self) {
        self.0.locked.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
