#[cfg(test)]
use crate::task::runtime::worker_pool_config::TaskWorkerPoolConfigError;
use crate::{cqrs::projection::runtime::ProjectionRuntime, host::error::HostError, task::runtime::TaskRuntimeError};
use std::{future::Future, io};
use thiserror::Error;
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;

type TaskRuntimeExit = Result<Result<(), TaskRuntimeError>, JoinError>;

enum StopReason {
    Signal(io::Result<()>),
    Requested,
    Inbound(io::Result<()>),
    TaskRuntime(TaskRuntimeExit),
}

/// Coordinate signal, shared-token cancellation, or unexpected component exit.
///
/// Creating this future starts nothing. Its first poll installs a cancellation
/// guard before spawning `task_runtime` on Tokio. Inbound and signal futures run in
/// this future and need neither `Send` nor `'static`.
///
/// Observation priority is signal, token cancellation, inbound, then task_runtime.
/// An unexpected exit remains the primary error; requested shutdown prefers
/// signal-observation errors, then task_runtime errors, then inbound errors.
/// Secondary failures are logged separately.
/// Fatal task_runtime errors cancel the shared token before draining, allowing inbound
/// to stop admission immediately. Both components finish before resource cleanup;
/// Inbound and resource cleanup currently have no process-wide shutdown deadline.
/// Dropping or aborting a polled host requests cancellation and detaches the
/// task_runtime task, allowing its own drain to continue on a live Tokio runtime.
/// The inbound future is dropped; the host cannot await components or guarantee
/// resource cleanup in that case. Normal return waits for both components.
pub async fn run<Inbound, RuntimeFuture, Signal>(
    inbound: Inbound,
    task_runtime: RuntimeFuture,
    shutdown: CancellationToken,
    signal: Signal,
) -> Result<(), HostError>
where
    Inbound: Future<Output = io::Result<()>>,
    RuntimeFuture: Future<Output = Result<(), TaskRuntimeError>> + Send + 'static,
    Signal: Future<Output = io::Result<()>>,
{
    let _cancel_on_exit = shutdown.clone().drop_guard();
    let runtime_task = tokio::spawn(task_runtime);
    tokio::pin!(inbound);
    tokio::pin!(signal);
    tokio::pin!(runtime_task);

    let reason = tokio::select! {
        biased;
        result = &mut signal => StopReason::Signal(result),
        _ = shutdown.cancelled() => StopReason::Requested,
        result = &mut inbound => StopReason::Inbound(result),
        result = &mut runtime_task => StopReason::TaskRuntime(result),
    };
    shutdown.cancel();

    match reason {
        StopReason::Signal(signal_result) => {
            let (inbound_result, runtime_result) = tokio::join!(&mut inbound, &mut runtime_task);
            preserve_primary_failure(
                signal_result.map_err(HostError::ShutdownSignal),
                preserve_primary_failure(expected_runtime_exit(runtime_result), expected_inbound_exit(inbound_result)),
            )
        }
        StopReason::Requested => {
            let (inbound_result, runtime_result) = tokio::join!(&mut inbound, &mut runtime_task);
            preserve_primary_failure(expected_runtime_exit(runtime_result), expected_inbound_exit(inbound_result))
        }
        StopReason::Inbound(inbound_result) => {
            let primary = match inbound_result {
                Ok(()) => HostError::InboundStopped,
                Err(error) => HostError::Inbound(error),
            };
            preserve_primary_failure(Err(primary), expected_runtime_exit((&mut runtime_task).await))
        }
        StopReason::TaskRuntime(runtime_result) => preserve_primary_failure(
            unexpected_runtime_exit(runtime_result),
            expected_inbound_exit((&mut inbound).await),
        ),
    }
}

/// What stopped the components of a process ([`run_with_processors`]): the host's outcome, as [`run`] reports it, and
/// the read side's.
#[derive(Debug)]
#[must_use = "the host's and the read side's failures are lost unless they are checked"]
pub struct Stopped {
    pub host: Result<(), HostError>,
    pub read_side: Result<(), ReadSideStopped>,
}

/// The projection processors failed: one panicked (and stopped the others), or their runtime did.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("the read side stopped: {0}")]
pub struct ReadSideStopped(pub String);

/// Runs the inbound server and the task runtime under [`run`], with the projection processors beside them on the same
/// token: the processors start only now, after everything that could fail at start has succeeded, so a failed start
/// never leaves a checkpoint held. Whatever stops first stops the rest (the projection runtime cancels the token when it
/// ends). The processors are awaited after the server and the task runtime: they finish what is in flight and give
/// their checkpoints back; a panicking processor is the read side's failure.
pub async fn run_with_processors<I, T, S>(
    inbound: I,
    task_runtime: T,
    processors: ProjectionRuntime,
    shutdown: CancellationToken,
    signal: S,
) -> Stopped
where
    I: Future<Output = io::Result<()>>,
    T: Future<Output = Result<(), TaskRuntimeError>> + Send + 'static,
    S: Future<Output = io::Result<()>>,
{
    let read_side = tokio::spawn(processors.run(shutdown.clone()));
    let host = run(inbound, task_runtime, shutdown, signal).await;
    let read_side = match read_side.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(ReadSideStopped(error.to_string())),
        Err(error) => Err(ReadSideStopped(error.to_string())),
    };
    Stopped { host, read_side }
}

fn preserve_primary_failure(primary: Result<(), HostError>, secondary: Result<(), HostError>) -> Result<(), HostError> {
    match primary {
        Ok(()) => secondary,
        Err(error) => {
            if let Err(secondary) = secondary {
                secondary.log("additional failure during host shutdown");
            }
            Err(error)
        }
    }
}

fn expected_inbound_exit(result: io::Result<()>) -> Result<(), HostError> {
    result.map_err(HostError::Inbound)
}

fn expected_runtime_exit(result: TaskRuntimeExit) -> Result<(), HostError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(HostError::TaskRuntime(error)),
        Err(error) => Err(HostError::TaskRuntimeJoin(error)),
    }
}

fn unexpected_runtime_exit(result: TaskRuntimeExit) -> Result<(), HostError> {
    match result {
        Ok(Ok(())) => Err(HostError::TaskRuntimeStopped),
        other => expected_runtime_exit(other),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use super::*;

    async fn wait_for_shutdown(shutdown: CancellationToken, stopped: Arc<AtomicBool>) {
        shutdown.cancelled().await;
        stopped.store(true, Ordering::SeqCst);
    }

    #[tokio::test]
    async fn signal_drains_inbound_and_task_runtime_before_returning() {
        let shutdown = CancellationToken::new();
        let inbound_shutdown = shutdown.clone();
        let runtime_shutdown = shutdown.clone();
        let inbound_stopped = Arc::new(AtomicBool::new(false));
        let runtime_stopped = Arc::new(AtomicBool::new(false));
        let inbound = {
            let stopped = inbound_stopped.clone();
            async move {
                wait_for_shutdown(inbound_shutdown, stopped).await;
                Ok(())
            }
        };
        let task_runtime = {
            let stopped = runtime_stopped.clone();
            async move {
                wait_for_shutdown(runtime_shutdown, stopped).await;
                Ok(())
            }
        };

        let result = run(inbound, task_runtime, shutdown, async { Ok(()) }).await;

        assert!(result.is_ok());
        assert!(inbound_stopped.load(Ordering::SeqCst));
        assert!(runtime_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn signal_failure_still_drains_components() {
        let shutdown = CancellationToken::new();
        let inbound_shutdown = shutdown.clone();
        let runtime_shutdown = shutdown.clone();
        let inbound_stopped = Arc::new(AtomicBool::new(false));
        let runtime_stopped = Arc::new(AtomicBool::new(false));
        let inbound = {
            let stopped = inbound_stopped.clone();
            async move {
                wait_for_shutdown(inbound_shutdown, stopped).await;
                Ok(())
            }
        };
        let task_runtime = {
            let stopped = runtime_stopped.clone();
            async move {
                wait_for_shutdown(runtime_shutdown, stopped).await;
                Ok(())
            }
        };
        let signal = async { Err(io::Error::other("signal handler failed")) };

        let result = run(inbound, task_runtime, shutdown, signal).await;

        assert!(matches!(result, Err(HostError::ShutdownSignal(_))));
        assert!(inbound_stopped.load(Ordering::SeqCst));
        assert!(runtime_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn inbound_failure_stops_task_runtime_before_returning_error() {
        let shutdown = CancellationToken::new();
        let runtime_shutdown = shutdown.clone();
        let runtime_stopped = Arc::new(AtomicBool::new(false));
        let task_runtime = {
            let stopped = runtime_stopped.clone();
            async move {
                wait_for_shutdown(runtime_shutdown, stopped).await;
                Ok(())
            }
        };
        let inbound = async { Err(io::Error::other("Inbound failed")) };
        let signal = std::future::pending();

        let result = run(inbound, task_runtime, shutdown, signal).await;

        assert!(matches!(result, Err(HostError::Inbound(_))));
        assert!(runtime_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn unexpected_task_runtime_exit_stops_inbound_before_returning_error() {
        let shutdown = CancellationToken::new();
        let inbound_shutdown = shutdown.clone();
        let inbound_stopped = Arc::new(AtomicBool::new(false));
        let inbound = {
            let stopped = inbound_stopped.clone();
            async move {
                wait_for_shutdown(inbound_shutdown, stopped).await;
                Ok(())
            }
        };
        let task_runtime = async { Ok(()) };
        let signal = std::future::pending();

        let result = run(inbound, task_runtime, shutdown, signal).await;

        assert!(matches!(result, Err(HostError::TaskRuntimeStopped)));
        assert!(inbound_stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn task_runtime_panic_stops_inbound_before_returning_join_error() {
        let shutdown = CancellationToken::new();
        let inbound_shutdown = shutdown.clone();
        let inbound_stopped = Arc::new(AtomicBool::new(false));
        let inbound = {
            let stopped = inbound_stopped.clone();
            async move {
                wait_for_shutdown(inbound_shutdown, stopped).await;
                Ok(())
            }
        };
        let task_runtime = async {
            panic!("task_runtime panic");
            #[allow(unreachable_code)]
            Ok(())
        };
        let signal = std::future::pending();

        let result = run(inbound, task_runtime, shutdown, signal).await;

        assert!(matches!(result, Err(HostError::TaskRuntimeJoin(_))));
        assert!(inbound_stopped.load(Ordering::SeqCst));
    }
    fn task_runtime_error() -> TaskRuntimeError {
        TaskRuntimeError::Configuration(TaskWorkerPoolConfigError::LeaseTooShort)
    }

    #[tokio::test]
    async fn shared_cancellation_stops_inbound_before_task_runtime_drain_finishes() {
        let shutdown = CancellationToken::new();
        let (fail, failure) = tokio::sync::oneshot::channel::<()>();
        let (release, drain) = tokio::sync::oneshot::channel::<()>();
        let (inbound_started, inbound_shutdown_started) = tokio::sync::oneshot::channel();
        let runtime_shutdown = shutdown.clone();
        let task_runtime = async move {
            failure.await.unwrap();
            runtime_shutdown.cancel();
            drain.await.unwrap();
            Err(task_runtime_error())
        };
        let inbound_shutdown = shutdown.clone();
        let inbound = async move {
            inbound_shutdown.cancelled().await;
            inbound_started.send(()).unwrap();
            Ok(())
        };
        let runtime = tokio::spawn(run(inbound, task_runtime, shutdown, std::future::pending()));
        fail.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), inbound_shutdown_started)
            .await
            .unwrap()
            .unwrap();
        assert!(
            !runtime.is_finished(),
            "Inbound must observe cancellation while task_runtime still drains"
        );
        release.send(()).unwrap();
        assert!(matches!(
            runtime.await.unwrap(),
            Err(HostError::TaskRuntime(TaskRuntimeError::Configuration(
                TaskWorkerPoolConfigError::LeaseTooShort
            )))
        ));
    }

    #[tokio::test]
    async fn cancellation_and_signal_win_over_simultaneous_successful_component_exits() {
        for signal_ready in [false, true] {
            let shutdown = CancellationToken::new();
            if !signal_ready {
                shutdown.cancel();
            }
            let task_runtime = async { Ok(()) };
            let signal = async move { if signal_ready { Ok(()) } else { std::future::pending().await } };
            assert!(run(async { Ok(()) }, task_runtime, shutdown, signal).await.is_ok());
        }
    }

    #[tokio::test]
    async fn clean_inbound_exit_without_cancellation_is_unexpected() {
        let shutdown = CancellationToken::new();
        let runtime_shutdown = shutdown.clone();
        let task_runtime = async move {
            runtime_shutdown.cancelled().await;
            Ok(())
        };
        assert!(matches!(
            run(async { Ok(()) }, task_runtime, shutdown, std::future::pending()).await,
            Err(HostError::InboundStopped)
        ));
    }

    #[tokio::test]
    async fn dropping_host_requests_shutdown_and_detaches_task_runtime() {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        let shutdown = CancellationToken::new();
        let runtime_shutdown = shutdown.clone();
        let (stopped, runtime_stopped) = tokio::sync::oneshot::channel();
        let task_runtime = async move {
            runtime_shutdown.cancelled().await;
            stopped.send(()).unwrap();
            Ok(())
        };
        let mut runtime = Box::pin(run(
            std::future::pending(),
            task_runtime,
            shutdown.clone(),
            std::future::pending(),
        ));
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(runtime.as_mut().poll(&mut context), Poll::Pending));
        drop(runtime);
        assert!(shutdown.is_cancelled());
        tokio::time::timeout(std::time::Duration::from_secs(1), runtime_stopped)
            .await
            .unwrap()
            .unwrap();
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
    async fn primary_failures_survive_shutdown_and_secondary_failures_are_logged() {
        use tracing::instrument::WithSubscriber;
        for initiator in ["signal_failure", "signal", "token", "inbound", "task_runtime"] {
            let writer = RecordingWriter::default();
            let subscriber = tracing_subscriber::fmt()
                .with_writer(writer.clone())
                .with_ansi(false)
                .without_time()
                .finish();
            let dispatch = tracing::Dispatch::new(subscriber);
            // Another test can first register the shared log callsite without
            // a subscriber. Prime it and rebuild its interest under this capture
            // before exercising the error matrix concurrently with those tests.
            tracing::dispatcher::with_default(&dispatch, || {
                HostError::InboundStopped.log("initialize log capture");
                tracing::callsite::rebuild_interest_cache();
            });
            writer.0.lock().unwrap().clear();
            let shutdown = CancellationToken::new();
            if initiator == "token" {
                shutdown.cancel();
            }
            let inbound_shutdown = shutdown.clone();
            let inbound = async move {
                if initiator != "inbound" {
                    inbound_shutdown.cancelled().await;
                }
                Err(io::Error::other("private_inbound_source"))
            };
            let runtime_shutdown = shutdown.clone();
            let task_runtime = async move {
                if initiator == "inbound" {
                    runtime_shutdown.cancelled().await;
                }
                Err(task_runtime_error())
            };
            let signal = async move {
                match initiator {
                    "signal_failure" => Err(io::Error::other("private_signal_source")),
                    "signal" => Ok(()),
                    _ => std::future::pending().await,
                }
            };
            let result = run(inbound, task_runtime, shutdown, signal).with_subscriber(dispatch).await;
            match initiator {
                "signal_failure" => assert!(matches!(result, Err(HostError::ShutdownSignal(_)))),
                "inbound" => assert!(matches!(result, Err(HostError::Inbound(_)))),
                _ => assert!(matches!(result, Err(HostError::TaskRuntime(_)))),
            }
            let log = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
            assert!(!log.contains("private_"));
            assert_eq!(
                log.matches("additional failure during host shutdown").count(),
                if initiator == "signal_failure" { 2 } else { 1 }
            );
            if initiator == "inbound" || initiator == "signal_failure" {
                assert!(log.contains("TaskRuntime"));
            }
            if initiator != "inbound" {
                assert!(log.contains("Inbound"));
            }
        }
    }

    #[tokio::test]
    async fn creating_and_dropping_unpolled_host_starts_nothing() {
        let shutdown = CancellationToken::new();
        let started = Arc::new(AtomicBool::new(false));
        let runtime_started = started.clone();
        let inbound_started = started.clone();
        let signal_started = started.clone();
        let host = run(
            async move {
                inbound_started.store(true, Ordering::SeqCst);
                Ok(())
            },
            async move {
                runtime_started.store(true, Ordering::SeqCst);
                Ok(())
            },
            shutdown.clone(),
            async move {
                signal_started.store(true, Ordering::SeqCst);
                Ok(())
            },
        );
        tokio::task::yield_now().await;
        assert!(!started.load(Ordering::SeqCst));
        drop(host);
        tokio::task::yield_now().await;
        assert!(!started.load(Ordering::SeqCst));
        assert!(!shutdown.is_cancelled());
    }

    #[tokio::test]
    async fn drop_and_abort_allow_started_task_runtime_work_to_drain() {
        use std::task::{Context, Poll, Waker};
        use std::time::Duration;
        use tokio::sync::oneshot;
        for abort in [false, true] {
            let shutdown = CancellationToken::new();
            let token = shutdown.clone();
            let (started, work_started) = oneshot::channel();
            let (cancelled, cancellation_observed) = oneshot::channel();
            let (release, drain) = oneshot::channel();
            let (finished, work_finished) = oneshot::channel();
            let task_runtime = async move {
                started.send(()).unwrap();
                token.cancelled().await;
                cancelled.send(()).unwrap();
                drain.await.unwrap();
                finished.send(()).unwrap();
                Ok(())
            };
            let mut host = Box::pin(run(
                std::future::pending(),
                task_runtime,
                shutdown.clone(),
                std::future::pending(),
            ));
            assert!(matches!(
                host.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
            tokio::time::timeout(Duration::from_secs(1), work_started)
                .await
                .unwrap()
                .unwrap();
            if abort {
                let task = tokio::spawn(host);
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                drop(host);
            }
            assert!(shutdown.is_cancelled());
            tokio::time::timeout(Duration::from_secs(1), cancellation_observed)
                .await
                .unwrap()
                .unwrap();
            assert!(!release.is_closed(), "host must leave task_runtime's active work alive");
            release.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), work_finished)
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[tokio::test]
    async fn normal_return_waits_for_both_drains_before_cleanup() {
        use std::time::Duration;
        use tokio::sync::oneshot;
        for inbound_first in [false, true] {
            let shutdown = CancellationToken::new();
            let token = shutdown.clone();
            let host_token = shutdown.clone();
            let (inbound_release, inbound_drain) = oneshot::channel();
            let (inbound_stopped, inbound_finished) = oneshot::channel();
            let (task_runtime_release, task_runtime_drain) = oneshot::channel();
            let (task_runtime_stopped, task_runtime_finished) = oneshot::channel();
            let cleanup = Arc::new(AtomicBool::new(false));
            let cleanup_started = cleanup.clone();
            let host = tokio::spawn(async move {
                let result = run(
                    async move {
                        shutdown.cancelled().await;
                        inbound_drain.await.unwrap();
                        inbound_stopped.send(()).unwrap();
                        Ok(())
                    },
                    async move {
                        token.cancelled().await;
                        task_runtime_drain.await.unwrap();
                        task_runtime_stopped.send(()).unwrap();
                        Ok(())
                    },
                    host_token,
                    async { Ok(()) },
                )
                .await;
                cleanup_started.store(true, Ordering::SeqCst);
                result
            });
            if inbound_first {
                inbound_release.send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(1), inbound_finished)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(!host.is_finished());
                assert!(!cleanup.load(Ordering::SeqCst));
                task_runtime_release.send(()).unwrap();
                task_runtime_finished.await.unwrap();
            } else {
                task_runtime_release.send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(1), task_runtime_finished)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(!host.is_finished());
                assert!(!cleanup.load(Ordering::SeqCst));
                inbound_release.send(()).unwrap();
                inbound_finished.await.unwrap();
            }
            assert!(
                tokio::time::timeout(Duration::from_secs(1), host)
                    .await
                    .unwrap()
                    .unwrap()
                    .is_ok()
            );
            assert!(cleanup.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn inbound_and_signal_can_borrow_non_send_state() {
        use std::rc::Rc;
        let local = Rc::new(());
        let shutdown = CancellationToken::new();
        let token = shutdown.clone();
        let result = run(
            async {
                let _borrow = &local;
                Ok(())
            },
            async move {
                token.cancelled().await;
                Ok(())
            },
            shutdown,
            async {
                let _borrow = &local;
                Ok(())
            },
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn signal_error_wins_over_ready_token_and_components() {
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let result = run(
            async { Err(io::Error::other("inbound failure")) },
            async { Err(task_runtime_error()) },
            shutdown,
            async { Err(io::Error::other("signal failure")) },
        )
        .await;
        assert!(matches!(result, Err(HostError::ShutdownSignal(_))));
    }

    #[tokio::test]
    async fn inbound_exit_wins_over_simultaneous_task_runtime_exit() {
        use std::task::{Context, Poll, Waker};
        let ready = Arc::new(AtomicBool::new(false));
        let runtime_ready = ready.clone();
        let inbound_ready = ready.clone();
        let inbound = std::future::poll_fn(move |_| {
            if inbound_ready.load(Ordering::SeqCst) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        });
        let task_runtime = async move {
            runtime_ready.store(true, Ordering::SeqCst);
            Ok(())
        };
        let mut host = Box::pin(run(inbound, task_runtime, CancellationToken::new(), std::future::pending()));
        assert!(matches!(
            host.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        // The task_runtime task completes before the host gets its next poll.
        tokio::task::yield_now().await;
        assert!(ready.load(Ordering::SeqCst));
        assert!(matches!(host.await, Err(HostError::InboundStopped)));
    }

    mod processors {
        use super::*;
        use crate::{
            cqrs::{
                projection::{
                    View, ViewProjection,
                    processor::{
                        ProjectionProcessor,
                        config::ProcessorConfig,
                        observer::{LeaseEvent, ProjectionObserver},
                    },
                },
                testing::{
                    mem_checkpoint_store::MemCheckpointStore, mem_event_repository::MemEventRepository,
                    mem_view_repository::MemViewRepository,
                },
            },
            domain::{
                aggregate::Aggregate,
                command::Decide,
                event::{DomainEvent, EventEnvelope},
            },
        };
        use serde::{Deserialize, Serialize};
        use std::time::Duration;

        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct Counted;

        impl DomainEvent for Counted {
            fn event_type(&self) -> &'static str {
                "counted"
            }
            fn event_version(&self) -> &'static str {
                "1"
            }
        }

        #[derive(Debug, thiserror::Error)]
        #[error("never")]
        struct Never;

        #[derive(Debug, Default, Clone, Serialize, Deserialize)]
        struct Counter;

        impl Aggregate for Counter {
            const TYPE: &'static str = "counter";
            type Id = String;
            type Command = ();
            type Event = Counted;
            type Error = Never;
            type Rules = ();
            fn apply(&mut self, _: Counted) {}
        }

        impl Decide<Counter> for () {
            fn decide(self, _: &Counter, _: &()) -> Result<Counted, Never> {
                Ok(Counted)
            }
        }

        #[derive(Debug, Default, Serialize, Deserialize)]
        struct Total;

        impl View<Counter> for Total {
            const NAME: &'static str = "totals";
            fn update(&mut self, _: &EventEnvelope<Counter>) {}
        }

        /// An observer that panics as soon as its processor holds the checkpoint: a bug in the read side.
        struct Panicking;

        impl ProjectionObserver for Panicking {
            fn lease(&self, event: LeaseEvent) {
                assert!(event != LeaseEvent::Acquired, "a bug in an observer");
            }
        }

        fn runtime(observer: Option<Arc<dyn ProjectionObserver>>) -> ProjectionRuntime {
            let processor = ProjectionProcessor::new(
                "totals",
                Arc::new(MemEventRepository::default()),
                Arc::new(MemCheckpointStore::default()),
                Arc::new(ViewProjection::new(Arc::new(MemViewRepository::<Total, Counter>::default()))),
                ProcessorConfig::default(),
            );
            let processor = match observer {
                Some(observer) => processor.with_observer(observer),
                None => processor,
            };
            ProjectionRuntime::new().with_processor(processor)
        }

        /// An inbound server and a task runtime that run until the token is cancelled.
        fn components(
            shutdown: &CancellationToken,
        ) -> (
            impl Future<Output = io::Result<()>>,
            impl Future<Output = Result<(), TaskRuntimeError>> + Send + 'static,
        ) {
            let (inbound, runtime) = (shutdown.clone(), shutdown.clone());
            (
                async move {
                    inbound.cancelled().await;
                    Ok(())
                },
                async move {
                    runtime.cancelled().await;
                    Ok(())
                },
            )
        }

        #[tokio::test]
        async fn a_panicking_processor_stops_the_server_and_fails_the_process() {
            let shutdown = CancellationToken::new();
            let (inbound, task_runtime) = components(&shutdown);
            let result = run_with_processors(
                inbound,
                task_runtime,
                runtime(Some(Arc::new(Panicking))),
                shutdown.clone(),
                std::future::pending(),
            )
            .await
            .read_side;
            assert!(matches!(result, Err(ReadSideStopped(message)) if message.contains("totals")));
            assert!(shutdown.is_cancelled(), "the panic stopped everything else");
        }

        #[tokio::test]
        async fn a_requested_stop_ends_every_component_and_the_processors_last() {
            let shutdown = CancellationToken::new();
            let (inbound, task_runtime) = components(&shutdown);
            let stop = shutdown.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                stop.cancel();
            });
            let stopped =
                run_with_processors(inbound, task_runtime, runtime(None), shutdown.clone(), std::future::pending()).await;
            let result = stopped.read_side;
            assert!(result.is_ok(), "{result:?}");
            assert!(stopped.host.is_ok());
        }
    }
}
