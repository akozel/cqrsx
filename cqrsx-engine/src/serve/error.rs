use cqrsx::worker::runtime::TaskRuntimeError;
use std::{fmt, io};
use thiserror::Error;
use tokio::task::JoinError;
use tracing::error;

/// A signal observation or component failure encountered by the process host.
#[derive(Error)]
pub enum HostError {
    #[error("failed to observe a shutdown signal")]
    ShutdownSignal(#[source] io::Error),
    #[error("inbound adapter failed")]
    Inbound(#[source] io::Error),
    #[error("inbound adapter terminated before shutdown")]
    InboundStopped,
    #[error("task runtime task failed")]
    TaskRuntimeJoin(#[source] JoinError),
    #[error("task runtime failed")]
    TaskRuntime(#[source] TaskRuntimeError),
    #[error("task runtime terminated before shutdown")]
    TaskRuntimeStopped,
}

// Debug is also used by Rust's Result termination report. Never format a
// JoinError directly: both its Debug and Display can reveal the panic payload.
impl fmt::Debug for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ShutdownSignal(error) => f.debug_struct("ShutdownSignal").field("kind", &error.kind()).finish(),
            Self::Inbound(error) => f.debug_struct("Inbound").field("kind", &error.kind()).finish(),
            Self::TaskRuntime(error) => f.debug_tuple("TaskRuntime").field(error).finish(),
            Self::TaskRuntimeJoin(error) => f
                .debug_struct("TaskRuntimeJoin")
                .field("tokio_task_id", &error.id())
                .field("panicked", &error.is_panic())
                .field("cancelled", &error.is_cancelled())
                .finish(),
            Self::InboundStopped | Self::TaskRuntimeStopped => fmt::Display::fmt(self, f),
        }
    }
}

impl HostError {
    /// Log the failure without formatting a runtime task's panic payload.
    pub fn log(&self, message: &str) {
        match self {
            Self::TaskRuntimeJoin(source) => error!(
                error = %self, tokio_task_id = %source.id(), panicked = source.is_panic(),
                cancelled = source.is_cancelled(), "{message}"
            ),
            _ => error!(error = ?self, "{message}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn secondary_task_panics_and_cancellations_log_only_safe_join_context() {
        let panic = tokio::spawn(async {
            panic!("private_join_payload");
        })
        .await
        .unwrap_err();
        let cancelled = tokio::spawn(std::future::pending::<()>());
        cancelled.abort();
        let cancelled = cancelled.await.unwrap_err();
        let ids = [panic.id().to_string(), cancelled.id().to_string()];
        let (_, log) = crate::test_support::capture(async {
            for source in [panic, cancelled] {
                HostError::TaskRuntimeJoin(source).log("additional failure during host shutdown");
            }
        })
        .await;
        assert_eq!(log.matches("additional failure during host shutdown").count(), 2);
        for id in ids {
            assert!(log.contains(&id));
        }
        assert!(log.contains("panicked=true cancelled=false"));
        assert!(log.contains("panicked=false cancelled=true"));
        assert!(!log.contains("private_join_payload"));
    }

    #[tokio::test]
    async fn io_and_dispatcher_diagnostics_preserve_sources_without_rendering_private_text() {
        use cqrsx::worker::runtime::worker_pool_config::TaskWorkerPoolConfigError;

        let (_, log) = crate::test_support::capture(async {
            for error in [
                HostError::Inbound(io::Error::new(io::ErrorKind::PermissionDenied, "private_io_source")),
                HostError::ShutdownSignal(io::Error::other("private_signal_source")),
                HostError::TaskRuntime(TaskRuntimeError::Configuration(TaskWorkerPoolConfigError::LeaseTooShort)),
            ] {
                assert!(std::error::Error::source(&error).is_some());
                assert!(!format!("{error:?} {error}").contains("private_"));
                error.log("additional failure during host shutdown");
            }
        })
        .await;
        assert!(log.contains("PermissionDenied"));
        assert!(log.contains("LeaseTooShort"));
        assert!(!log.contains("private_"));
        assert!(!log.contains("mail@example.com"));
    }

    #[derive(Clone, Default)]
    struct RecordingWriter(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for RecordingWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RecordingWriter {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[tokio::test]
    async fn runtime_task_diagnostics_and_debug_redact_panic_payloads() {
        let writer = RecordingWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer.clone())
            .with_ansi(false)
            .without_time()
            .finish();
        let panic = tokio::spawn(async {
            panic!("private_panic_payload_sentinel");
        })
        .await
        .unwrap_err();
        let tokio_task_id = panic.id().to_string();
        let error = HostError::TaskRuntimeJoin(panic);
        tracing::subscriber::with_default(subscriber, || error.log("service stopped with an error"));
        let log = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
        assert!(log.contains(&tokio_task_id));
        assert!(log.contains("panicked=true cancelled=false"));
        assert!(!log.contains("private_panic_payload_sentinel"));
        let debug = format!("{error:?}");
        assert!(debug.contains("panicked: true"));
        assert!(!debug.contains("private_panic_payload_sentinel"));
        // Keep the original error as a source without rendering its payload.
        assert!(std::error::Error::source(&error).is_some());
        let task = tokio::spawn(std::future::pending::<()>());
        task.abort();
        let error = HostError::TaskRuntimeJoin(task.await.unwrap_err());
        assert!(format!("{error:?}").contains("cancelled: true"));
    }

    // The log lines a process writes through the `log` crate when no `tracing` subscriber is installed, as the
    // process-global logger of a test of its own (the logger is installed once and never removed).
    struct RecordingLogger(std::sync::Mutex<Vec<String>>);
    static LOGGER: RecordingLogger = RecordingLogger(std::sync::Mutex::new(Vec::new()));

    impl log::Log for RecordingLogger {
        fn enabled(&self, _: &log::Metadata<'_>) -> bool {
            true
        }

        fn log(&self, record: &log::Record<'_>) {
            self.0.lock().unwrap().push(record.args().to_string());
        }

        fn flush(&self) {}
    }

    #[test]
    fn host_failures_fall_back_to_log_without_a_tracing_subscriber_and_redact_private_data() {
        use crate::test_support;
        if test_support::scenario().is_some() {
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            runtime.block_on(async {
                assert!(!tracing::dispatcher::has_been_set());
                log::set_logger(&LOGGER).unwrap();
                log::set_max_level(log::LevelFilter::Trace);
                let panic = tokio::spawn(async {
                    panic!("private_panic_payload");
                })
                .await
                .unwrap_err();
                let panic_id = panic.id().to_string();
                HostError::TaskRuntimeJoin(panic).log("host stopped");
                let cancelled = tokio::spawn(std::future::pending::<()>());
                cancelled.abort();
                let cancelled = cancelled.await.unwrap_err();
                let cancelled_id = cancelled.id().to_string();
                HostError::TaskRuntimeJoin(cancelled).log("host stopped");
                let log = LOGGER.0.lock().unwrap().join("\n");
                assert!(log.contains(&panic_id));
                assert!(log.contains(&cancelled_id));
                assert!(log.contains("panicked=true cancelled=false"));
                assert!(log.contains("panicked=false cancelled=true"));
                assert!(!log.contains("private_"));
                assert!(!tracing::dispatcher::has_been_set());
            });
            return;
        }
        test_support::isolated(
            concat!(
                module_path!(),
                "::host_failures_fall_back_to_log_without_a_tracing_subscriber_and_redact_private_data"
            ),
            "fallback",
            |_| {},
        );
    }
}
