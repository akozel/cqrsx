//! Queue-backed invocation and settlement within original lease deadlines.
use super::lease_budget::{ExecutionPlan, execution_plan};
use crate::{
    task::{
        adapter::TaskAdapter,
        dispatcher::TaskDispatcher,
        executor::TaskExecutor,
        policy::Policies,
        queue::{
            TaskProcessingError, TaskQueue,
            lease::LeasedTask,
            retry::redelivery_backoff,
            settlement::{SettlementRequest, SettlementResult},
            storage_config::TaskStorageConfig,
        },
        result::TaskResult,
    },
    unwinding::{Panicked, unwinding},
};
use std::{fmt, sync::Arc, time::Duration};
use tokio::time::{Instant, sleep_until};

/// The reason a retry of a handler that panicked is settled with: fixed, since the panic's message may hold the payload.
pub const HANDLER_PANICKED: &str = "handler panicked";

/// Queue-backed executor with checked storage budgets and concrete queue ownership.
/// Construction performs no I/O and reads no environment variables. Callers
/// create shared ownership explicitly with `Arc::new` when needed.
///
/// A retry waits the pause of the task type's policy ([`with_policies`](Self::with_policies)), or the queue's
/// [`redelivery_backoff`] for a type without one; the handler sees that pause in its context before it answers. A
/// handler that times out and an attempt the lease cannot fit wait the redelivery backoff whatever the policy.
///
/// A handler that panics is the boundary's to catch: the panic is logged (`error`, without its message, the payload or
/// the lease token) and the attempt is settled as a retry with the reason [`HANDLER_PANICKED`] after the policy's
/// pause, as if the handler had answered retry; the worker lives on. A handler that always panics spends the task's
/// attempt budget and the task fails.
#[must_use]
pub struct DefaultTaskExecutor<Q: TaskQueue> {
    queue: Arc<Q>,
    storage_timeout: Duration,
    policies: Policies,
}

// By hand: the queue need not be `Debug`.
impl<Q: TaskQueue> fmt::Debug for DefaultTaskExecutor<Q> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DefaultTaskExecutor")
            .field("storage_timeout", &self.storage_timeout)
            .field("policies", &self.policies)
            .finish_non_exhaustive()
    }
}

impl<Q: TaskQueue> DefaultTaskExecutor<Q> {
    pub fn new(queue: Arc<Q>, config: TaskStorageConfig) -> Self {
        Self {
            queue,
            storage_timeout: config.storage_timeout(),
            policies: Policies::default(),
        }
    }

    /// The same executor pausing every retry by the policy of its task's type.
    pub fn with_policies(mut self, policies: Policies) -> Self {
        self.policies = policies;
        self
    }

    async fn settle_before(
        &self,
        task: &LeasedTask,
        outcome: SettlementRequest,
        settlement_deadline: Instant,
    ) -> Result<SettlementResult, TaskProcessingError> {
        let now = Instant::now();
        if now >= settlement_deadline {
            return Err(TaskProcessingError::LeaseBudgetExhausted);
        }
        let deadline = (now + self.storage_timeout).min(settlement_deadline);
        tokio::select! {
            // Check the deadline before polling storage, including after a pause.
            biased;
            _ = sleep_until(deadline) => Err(TaskProcessingError::TimedOut),
            result = self.queue.settle(task, outcome) => result,
        }
    }
}

impl<Q: TaskQueue> TaskExecutor for DefaultTaskExecutor<Q> {
    fn storage_timeout(&self) -> Duration {
        self.storage_timeout
    }

    async fn execute_and_settle<A: TaskAdapter>(
        &self,
        dispatcher: &TaskDispatcher<A>,
        task: LeasedTask,
        handler_timeout: Duration,
    ) -> Result<SettlementResult, TaskProcessingError> {
        let (handler_deadline, settlement_deadline) =
            match execution_plan(Instant::now(), task.lease().deadline(), handler_timeout, self.storage_timeout)? {
                ExecutionPlan::Run {
                    handler_deadline,
                    settlement_deadline,
                } => (handler_deadline, settlement_deadline),
                ExecutionPlan::ScheduleRetry { settlement_deadline } => {
                    return self
                        .settle_before(
                            &task,
                            SettlementRequest::Retry {
                                reason: "insufficient lease budget before handler execution".to_owned(),
                                delay: redelivery_backoff(task.attempt().number()),
                            },
                            settlement_deadline,
                        )
                        .await;
                }
                ExecutionPlan::AwaitRecovery => return Err(TaskProcessingError::LeaseBudgetExhausted),
            };
        // The pause of a retry, chosen before the handler runs so that its context can tell it.
        let retry_pause = self.policies.pause(task.attempt().format(), task.attempt().number());
        let outcome = tokio::select! {
            biased;
            _ = sleep_until(handler_deadline) => SettlementRequest::Retry {
                reason: "handler timed out".to_owned(),
                delay: redelivery_backoff(task.attempt().number()),
            },
            answer = unwinding(dispatcher.dispatch(task.shared_attempt(), retry_pause)) => match answer {
                Ok(result) => settlement_request(result, retry_pause),
                Err(Panicked) => {
                    crate::diagnostics::log_task_handler_panic(task.attempt());
                    SettlementRequest::Retry {
                        reason: HANDLER_PANICKED.to_owned(),
                        delay: retry_pause,
                    }
                }
            },
        };
        // Retain a margin before local expiry; queue-side token and database
        // clock checks remain authoritative, including after clock changes.
        self.settle_before(&task, outcome, settlement_deadline).await
    }
}

fn settlement_request(result: TaskResult, retry_pause: Duration) -> SettlementRequest {
    match result {
        TaskResult::Done => SettlementRequest::Completed,
        TaskResult::Fail { reason } => SettlementRequest::Failed { reason },
        TaskResult::Retry { reason } => SettlementRequest::Retry {
            reason,
            delay: retry_pause,
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::task::{
        adapter::TaskAdapter,
        context::TaskContext,
        format::TaskFormat,
        policy::{PolicyName, Retry, TaskPolicy},
        queue::lease::LEASE_SAFETY_MARGIN,
    };

    use super::*;
    use crate::task::test_support::*;
    use std::{
        num::NonZeroU16,
        sync::{Arc, atomic::Ordering},
        time::Duration,
    };

    struct ProbeAdapter {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        simulate_pause: bool,
    }

    impl TaskAdapter for ProbeAdapter {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }

        async fn invoke(&self, _: &TaskContext) -> TaskResult {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.simulate_pause {
                tokio::time::advance(Duration::from_secs(120)).await;
            }
            TaskResult::Done
        }
    }

    #[tokio::test(start_paused = true)]
    async fn delayed_task_retries_without_starting_handler_and_expired_task_waits_for_recovery() {
        let queue = Arc::new(Queue::default());
        let executor = Arc::new(DefaultTaskExecutor::new(
            queue.clone(),
            TaskStorageConfig::try_new(Duration::from_secs(5)).unwrap(),
        ));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let adapter = ProbeAdapter {
            calls: calls.clone(),
            simulate_pause: false,
        };
        let dispatcher = dispatcher(adapter);
        let mut delayed = claimed();
        delayed = with_deadline(&delayed, Instant::now() + Duration::from_secs(39));
        assert!(
            executor
                .execute_and_settle(&dispatcher, delayed, Duration::from_secs(30))
                .await
                .unwrap()
                .is_applied()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            matches!(&queue.outcomes.lock().unwrap()[0].1, SettlementRequest::Retry { reason, .. }
                if reason == "insufficient lease budget before handler execution")
        );
        let mut expired = claimed();
        expired = with_deadline(&expired, Instant::now());
        assert_eq!(
            executor
                .execute_and_settle(&dispatcher, expired, Duration::from_secs(30))
                .await,
            Err(TaskProcessingError::LeaseBudgetExhausted)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(queue.outcomes.lock().unwrap().len(), 1);
        assert_eq!(dispatcher.formats(), vec![format()]);
    }

    #[tokio::test(start_paused = true)]
    async fn lease_margin_shortens_settlement_deadline_without_starting_handler() {
        let queue = Arc::new(Queue::default());
        queue.hang_settle.store(true, Ordering::SeqCst);
        let executor = Arc::new(DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default()));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let adapter = ProbeAdapter {
            calls: calls.clone(),
            simulate_pause: false,
        };
        let dispatcher = dispatcher(adapter);
        let mut task = claimed();
        task = with_deadline(&task, Instant::now() + LEASE_SAFETY_MARGIN + Duration::from_secs(1));
        let started = Instant::now();
        assert_eq!(
            executor.execute_and_settle(&dispatcher, task, Duration::from_secs(30)).await,
            Err(TaskProcessingError::TimedOut)
        );
        assert_eq!(started.elapsed(), Duration::from_secs(1));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(queue.outcomes.lock().unwrap()[0].1, SettlementRequest::Retry { .. }));
    }

    #[tokio::test(start_paused = true)]
    async fn process_pause_exhausting_lease_prevents_a_late_acknowledgement() {
        let queue = Arc::new(Queue::default());
        let executor = Arc::new(DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default()));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let adapter = ProbeAdapter {
            calls: calls.clone(),
            simulate_pause: true,
        };
        let dispatcher = dispatcher(adapter);
        assert_eq!(
            executor
                .execute_and_settle(&dispatcher, claimed(), Duration::from_secs(30))
                .await,
            Err(TaskProcessingError::LeaseBudgetExhausted)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(queue.outcomes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unrepresentable_handler_budget_is_rejected_before_execution() {
        let queue = Arc::new(Queue::default());
        let executor = Arc::new(DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default()));
        assert_eq!(
            executor
                .execute_and_settle(&dispatcher(adapter()), claimed(), Duration::MAX)
                .await,
            Err(TaskProcessingError::InvalidExecutionBudget)
        );
        assert!(queue.outcomes.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn storage_deadline_bounds_settlement_without_a_terminal_outcome() {
        let queue = Arc::new(Queue::default());
        let executor = Arc::new(DefaultTaskExecutor::new(
            queue.clone(),
            TaskStorageConfig::try_new(Duration::from_secs(3)).unwrap(),
        ));
        queue.hang_settle.store(true, Ordering::SeqCst);
        let started = Instant::now();
        assert_eq!(
            executor
                .execute_and_settle(&dispatcher(adapter()), claimed(), Duration::from_secs(30))
                .await,
            Err(TaskProcessingError::TimedOut)
        );
        assert_eq!(started.elapsed(), Duration::from_secs(3));
        assert_eq!(queue.outcomes.lock().unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn delivery_classifies_success_errors_and_timeout_and_preserves_ownership_result() {
        let queue = Arc::new(Queue::default());
        let executor = Arc::new(DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default()));
        // The throttled task's type waits 300 s before its next attempt.
        let throttled = DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default())
            .with_policies(policies(Retry::fixed(Duration::from_secs(300))));
        for (result, hang, executor) in [
            (TaskResult::Done, false, executor.as_ref()),
            (TaskResult::retry("throttle"), false, &throttled),
            (TaskResult::retry("network"), false, executor.as_ref()),
            (TaskResult::fail("invalid"), false, executor.as_ref()),
            (TaskResult::Done, true, executor.as_ref()),
        ] {
            let mut task_adapter = adapter();
            task_adapter.result = result;
            task_adapter.hang = hang;
            let dispatcher = dispatcher(task_adapter);
            assert!(
                executor
                    .execute_and_settle(&dispatcher, claimed(), Duration::from_secs(30))
                    .await
                    .unwrap()
                    .is_applied()
            );
        }
        {
            let outcomes = queue.outcomes.lock().unwrap();
            assert_eq!(outcomes[0].1, SettlementRequest::Completed);
            assert_eq!(
                outcomes[1].1,
                SettlementRequest::Retry {
                    delay: Duration::from_secs(300),
                    reason: "throttle".into()
                }
            );
            assert!(
                matches!(&outcomes[2].1, SettlementRequest::Retry { delay, .. } if *delay >= Duration::from_secs(8) && *delay <= Duration::from_secs(12))
            );
            assert_eq!(
                outcomes[3].1,
                SettlementRequest::Failed {
                    reason: "invalid".into()
                }
            );
            assert!(matches!(&outcomes[4].1, SettlementRequest::Retry { reason, .. } if reason == "handler timed out"));
        }
        let dispatcher = dispatcher(adapter());
        queue.lost_owner.store(true, Ordering::SeqCst);
        assert!(
            !executor
                .execute_and_settle(&dispatcher, claimed(), Duration::from_secs(30))
                .await
                .unwrap()
                .is_applied()
        );
        queue.settle_error.store(true, Ordering::SeqCst);
        assert_eq!(
            executor
                .execute_and_settle(&dispatcher, claimed(), Duration::from_secs(30))
                .await,
            Err(TaskProcessingError::Unavailable)
        );
    }

    /// Panics on every delivery, with a message that must stay out of the log.
    struct Panicking;

    impl TaskAdapter for Panicking {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }

        async fn invoke(&self, _: &TaskContext) -> TaskResult {
            panic!("injected handler panic with a secret");
        }
    }

    #[tokio::test]
    async fn a_handler_panic_is_a_retry_after_the_pause_of_its_policy_logged_without_payload_or_lease_token() {
        let queue = Arc::new(Queue::default());
        let executor = DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default())
            .with_policies(policies(Retry::fixed(Duration::from_secs(300))));
        let task = claimed();
        let (task_id, token) = (task.attempt().task_id(), task.lease().lock_token());
        let (settled, log) = crate::diagnostics::test_support::capture(executor.execute_and_settle(
            &dispatcher(Panicking),
            task,
            Duration::from_secs(30),
        ))
        .await;
        assert_eq!(settled, Ok(SettlementResult::RetryScheduled));
        assert_eq!(
            queue.outcomes.lock().unwrap().as_slice(),
            [(
                task_id,
                SettlementRequest::Retry {
                    delay: Duration::from_secs(300),
                    reason: "handler panicked".into()
                }
            )]
        );
        assert!(log.contains("task handler panicked; the task is retried"), "{log}");
        assert!(log.contains(&task_id.to_string()) && log.contains("attempt=2"), "{log}");
        for private in ["secret", "runtime", &token.to_string()] {
            assert!(!log.contains(private), "{private} in {log}");
        }
    }

    /// The policies binding the dummy task's type to `retry`.
    fn policies(retry: Retry) -> Policies {
        const DUMMY: PolicyName = PolicyName::try_new("dummy").unwrap();
        Policies::default()
            .with_policy(DUMMY, TaskPolicy::new(retry, NonZeroU16::new(5).unwrap()))
            .unwrap()
            .with_task::<DummyTask>(DUMMY)
            .unwrap()
    }

    /// Answers retry and records the pause its context promised.
    struct Promised(Arc<std::sync::Mutex<Vec<Duration>>>);

    impl TaskAdapter for Promised {
        fn formats(&self) -> Vec<TaskFormat> {
            vec![format()]
        }

        async fn invoke(&self, ctx: &TaskContext) -> TaskResult {
            self.0.lock().unwrap().push(ctx.retry_pause());
            TaskResult::retry("later")
        }
    }

    /// The pause the handler was told and the delay its retry was settled with, for a claim of attempt 2.
    async fn told_and_settled(executor: DefaultTaskExecutor<Queue>, queue: &Queue) -> (Duration, Duration) {
        let promised = Arc::default();
        executor
            .execute_and_settle(
                &dispatcher(Promised(Arc::clone(&promised))),
                claimed(),
                Duration::from_secs(30),
            )
            .await
            .unwrap();
        let told = promised.lock().unwrap()[0];
        let SettlementRequest::Retry { delay, reason } = queue.outcomes.lock().unwrap().pop().unwrap().1 else {
            panic!("a retry");
        };
        assert_eq!(reason, "later");
        (told, delay)
    }

    #[tokio::test]
    async fn a_retry_waits_the_pause_of_its_task_types_policy_which_the_handler_was_told() {
        let queue = Arc::new(Queue::default());
        let executor = DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default())
            .with_policies(policies(Retry::backoff(Duration::from_secs(1), Duration::from_secs(8))));
        // The claim is attempt 2: the backoff's second pause.
        assert_eq!(
            told_and_settled(executor, &queue).await,
            (Duration::from_secs(2), Duration::from_secs(2))
        );
    }

    #[tokio::test]
    async fn without_a_policy_a_retry_waits_the_redelivery_backoff_the_handler_was_told() {
        let queue = Arc::new(Queue::default());
        for executor in [
            DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default()),
            DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::default()).with_policies(Policies::default()),
        ] {
            let (told, settled) = told_and_settled(executor, &queue).await;
            assert_eq!(told, settled);
            assert!(
                (Duration::from_secs(8)..=Duration::from_secs(12)).contains(&settled),
                "{settled:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_timeout_and_a_short_lease_wait_the_redelivery_backoff_even_under_a_policy() {
        let queue = Arc::new(Queue::default());
        let executor = DefaultTaskExecutor::new(queue.clone(), TaskStorageConfig::try_new(Duration::from_secs(5)).unwrap())
            .with_policies(policies(Retry::fixed(Duration::from_secs(300))));
        let mut hanging = adapter();
        hanging.hang = true;
        executor
            .execute_and_settle(&dispatcher(hanging), claimed(), Duration::from_secs(30))
            .await
            .unwrap();
        let short = with_deadline(&claimed(), Instant::now() + Duration::from_secs(39));
        executor
            .execute_and_settle(&dispatcher(adapter()), short, Duration::from_secs(30))
            .await
            .unwrap();
        let outcomes = queue.outcomes.lock().unwrap();
        let reasons_and_delays: Vec<(&str, Duration)> = outcomes
            .iter()
            .map(|(_, outcome)| match outcome {
                SettlementRequest::Retry { reason, delay } => (reason.as_str(), *delay),
                other => panic!("a retry, not {other:?}"),
            })
            .collect();
        assert_eq!(
            reasons_and_delays.iter().map(|(reason, _)| *reason).collect::<Vec<_>>(),
            ["handler timed out", "insufficient lease budget before handler execution"]
        );
        for (_, delay) in reasons_and_delays {
            assert!(
                (Duration::from_secs(8)..=Duration::from_secs(12)).contains(&delay),
                "{delay:?}"
            );
        }
    }
}
