//! The command path's wake-up for the projection processors of an aggregate type.
use std::sync::Arc;
use tokio::sync::Notify;

/// Wakes the projection processors of one aggregate type after a commit, so they read the journal at once
/// instead of at their next poll. One per aggregate type, shared by its command executor
/// (`with_commit_wakeup`) and its processors (`with_wakeup`): clones share the one notification.
///
/// Not a delivery: it carries no events, and a processor reads the journal whether or not it was woken; a
/// commit on another node reaches that node's processors only through their poll.
#[derive(Clone, Debug, Default)]
pub struct CommitWakeup(Arc<Notify>);

impl CommitWakeup {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wakes everyone waiting right now; with nobody waiting nothing happens. A processor registers its wait
    /// before it reads the journal, so a commit between the read and the wait is not lost.
    pub fn wake(&self) {
        self.0.notify_waiters();
    }

    /// Waits for the next [`wake`](Self::wake).
    pub async fn wait(&self) {
        self.0.notified().await;
    }

    pub(crate) fn notifier(&self) -> &Notify {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };

    fn pending(future: &mut std::pin::Pin<&mut impl Future>) -> bool {
        future.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending()
    }

    #[tokio::test]
    async fn a_wake_reaches_every_registered_waiter_and_nobody_else() {
        let wakeup = CommitWakeup::new();
        let shared = wakeup.clone();
        wakeup.wake(); // nobody waits: nothing is stored
        let mut late = pin!(shared.wait());
        assert!(pending(&mut late), "a wake before the wait is not kept");
        let mut registered = pin!(wakeup.notifier().notified());
        registered.as_mut().enable();
        let mut other = pin!(shared.wait());
        assert!(pending(&mut other));
        wakeup.wake();
        assert!(!pending(&mut late) && !pending(&mut other) && !pending(&mut registered));
    }

    #[tokio::test]
    async fn a_wait_registered_before_the_wake_resolves_even_when_polled_later() {
        let wakeup = CommitWakeup::default();
        let mut registered = pin!(wakeup.notifier().notified());
        registered.as_mut().enable();
        wakeup.wake();
        assert!(matches!(
            registered.poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(())
        ));
        assert!(format!("{wakeup:?}").starts_with("CommitWakeup"));
    }
}
