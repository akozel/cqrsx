//! The one way to take a `std::sync::Mutex` in cqrsx and in the crates built on it (`cqrsx-engine`, `cqrsx-testing`), so
//! that a poisoned mutex is handled in one place (rule 10 of `.claude/rules/rust-style.md`).
//!
//! # Contract
//!
//! Under the mutex runs one short operation on a container (insert, remove, take, read a field): never user code and
//! never an `.await`. A panic of another thread then cannot leave the data half-changed, so a poisoned mutex is safe to
//! use on: [`lock`] and [`get_mut`] log an `error!` naming the lock (the `name` argument, e.g. `"engine processors"`,
//! as the field `lock`), clear the poison and hand out the data. A lock that guards a change of several steps, where
//! data may stay half-updated, does not belong here: it must return `Err` on poison instead.
use crate::diagnostics;
use std::sync::{Mutex, MutexGuard, PoisonError};

/// Locks `mutex`; a poisoned one is logged as the lock `name`, cleared and locked all the same.
pub fn lock<'a, T>(mutex: &'a Mutex<T>, name: &'static str) -> MutexGuard<'a, T> {
    mutex.lock().unwrap_or_else(|poisoned| {
        diagnostics::log_poisoned_lock(name);
        mutex.clear_poison();
        poisoned.into_inner()
    })
}

/// The value of an exclusively borrowed `mutex`; a poisoned one is logged as the lock `name` and cleared.
pub fn get_mut<'a, T>(mutex: &'a mut Mutex<T>, name: &'static str) -> &'a mut T {
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
