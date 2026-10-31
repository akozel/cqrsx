//! Capturing the log lines of a test: one subscriber for one task, so that parallel tests do not see each other's lines.
use std::{
    future::Future,
    io,
    sync::{Arc, Mutex},
};
use tracing::instrument::WithSubscriber;

#[derive(Clone, Default)]
struct Writer(Arc<Mutex<Vec<u8>>>);

impl io::Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Writer {
    type Writer = Self;

    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}

/// The output of `work` and the `ERROR` lines it logged.
///
/// `warm_up` runs once under the subscriber before `work`: the caller logs each of its own lines there, so that no
/// callsite that parallel tests registered earlier without a subscriber stays disabled. What it logs is dropped.
pub async fn capture<T>(warm_up: impl FnOnce(), work: impl Future<Output = T>) -> (T, String) {
    capture_at(tracing::Level::ERROR, warm_up, work).await
}

/// The output of `work` and the lines of `level` and above it logged; `warm_up` as in [`capture`].
pub async fn capture_at<T>(level: tracing::Level, warm_up: impl FnOnce(), work: impl Future<Output = T>) -> (T, String) {
    let writer = Writer::default();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::fmt()
            .with_writer(writer.clone())
            .with_max_level(level)
            .with_ansi(false)
            .without_time()
            .finish(),
    );
    tracing::dispatcher::with_default(&dispatch, || {
        warm_up();
        tracing::callsite::rebuild_interest_cache();
        writer.0.lock().unwrap().clear();
    });
    let result = work.with_subscriber(dispatch).await;
    let text = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
    (result, text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn writer_clones_append_to_shared_buffer_and_flush_preserves_bytes() {
        let mut writer = Writer::default();
        let mut clone = writer.clone();
        writer.write_all(b"first event\n").unwrap();
        clone.write_all(b"second event\n").unwrap();
        clone.flush().unwrap();
        assert_eq!(*writer.0.lock().unwrap(), b"first event\nsecond event\n");
    }

    #[tokio::test]
    async fn capture_keeps_subscriber_across_await_and_filters_by_level() {
        let (result, log) = capture(|| tracing::error!("prime"), async {
            tracing::info!("filtered info event");
            tokio::task::yield_now().await;
            tracing::error!("captured error event");
            42
        })
        .await;
        assert_eq!(result, 42);
        assert!(log.contains("captured error event"));
        assert!(!log.contains("filtered info event"));
        assert!(!log.contains("prime"));

        let (_, log) = capture_at(tracing::Level::INFO, || {}, async {
            tokio::task::yield_now().await;
            tracing::info!("captured info event");
        })
        .await;
        assert!(log.contains("captured info event"));
    }
}
