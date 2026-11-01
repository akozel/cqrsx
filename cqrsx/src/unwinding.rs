//! The boundary with user code run by long-lived workers: a panic of a projection's handler or rebuilder, or of a
//! task's handler, is caught here and becomes an error, so that the worker lives on. Never control flow inside the
//! engine.
use std::{future::Future, panic::AssertUnwindSafe, task::Poll};

/// A polled future panicked. The panic's payload is dropped unread: it may hold the data the callee handled.
pub(crate) struct Panicked;

/// Polls `future` and turns a panic in any poll into `Err(Panicked)`; a future that panicked is not
/// polled again.
pub(crate) async fn catch_unwind<F: Future>(future: F) -> Result<F::Output, Panicked> {
    let mut future = std::pin::pin!(future);
    std::future::poll_fn(
        move |cx| match std::panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(_) => Poll::Ready(Err(Panicked)),
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_panic_in_any_poll_is_an_error_and_an_answer_passes_through() {
        assert!(matches!(catch_unwind(async { 7 }).await, Ok(7)));
        let late = async {
            tokio::task::yield_now().await;
            panic!("injected panic after a pending poll");
        };
        assert!(matches!(catch_unwind(late).await, Err(Panicked)));
    }
}
