//! The one way the crate takes a `std::sync::Mutex`. Under every such mutex runs one short operation on a container,
//! never user code and never an `.await`, so a panic of another thread cannot leave the data half-changed: a poisoned
//! mutex is logged with the lock's name, cleared and used on. A lock that guards a change of several steps must not
//! come here: it returns `Err` on poison instead.
use crate::diagnostics;
use std::sync::{Mutex, MutexGuard, PoisonError};

/// Locks `mutex`; a poisoned one is logged as the lock `name`, cleared and locked all the same.
pub(crate) fn lock<'a, T>(mutex: &'a Mutex<T>, name: &'static str) -> MutexGuard<'a, T> {
    mutex.lock().unwrap_or_else(|poisoned| {
        diagnostics::log_poisoned_lock(name);
        mutex.clear_poison();
        poisoned.into_inner()
    })
}

/// The value of an exclusively borrowed `mutex`; a poisoned one is logged as the lock `name` and cleared.
pub(crate) fn get_mut<'a, T>(mutex: &'a mut Mutex<T>, name: &'static str) -> &'a mut T {
    if mutex.is_poisoned() {
        diagnostics::log_poisoned_lock(name);
        mutex.clear_poison();
    }
    // Cleared above: the borrow is exclusive, nobody can poison the mutex in between.
    mutex.get_mut().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::test_support::capture;

    /// Poisons `mutex`: another thread panics while it holds the lock.
    fn poison(mutex: &Mutex<Vec<u32>>) {
        std::thread::scope(|scope| {
            let holder = scope.spawn(|| {
                let _guard = mutex.lock();
                panic!("injected panic under the lock");
            });
            assert!(holder.join().is_err(), "the holder panics");
        });
        assert!(mutex.is_poisoned());
    }

    #[tokio::test]
    async fn a_poisoned_lock_is_logged_by_name_cleared_and_used() {
        let mutex = Mutex::new(vec![1]);
        poison(&mutex);
        let (value, log) = capture(async {
            lock(&mutex, "test values").push(2);
            lock(&mutex, "test values").clone()
        })
        .await;
        assert_eq!(value, [1, 2]);
        assert!(!mutex.is_poisoned());
        assert_eq!(
            log.matches("lock=\"test values\"").count(),
            1,
            "logged once, then cleared: {log}"
        );
        assert!(log.contains("ERROR"), "{log}");
    }

    #[tokio::test]
    async fn a_poisoned_exclusive_borrow_is_logged_by_name_and_cleared() {
        let mut mutex = Mutex::new(vec![1]);
        poison(&mutex);
        let (value, log) = capture(async {
            get_mut(&mut mutex, "test values").push(2);
            get_mut(&mut mutex, "test values").clone()
        })
        .await;
        assert_eq!(value, [1, 2]);
        assert!(!mutex.is_poisoned());
        assert_eq!(
            log.matches("lock=\"test values\"").count(),
            1,
            "logged once, then cleared: {log}"
        );
    }
}
