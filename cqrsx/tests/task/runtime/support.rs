use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

use cqrsx::task::{
    adapter::TaskAdapter,
    attempt::TaskAttempt,
    context::TaskContext,
    dispatcher::TaskDispatcher,
    format::{TaskFormat, TaskPayload},
    queue::{
        TaskProcessingError, TaskQueue,
        lease::{LeasedTask, TaskLease},
        settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult, TaskFailure, TaskFailureCause},
        storage_config::TaskStorageConfig,
    },
    result::TaskResult,
};

#[derive(Serialize)]
pub(crate) struct DummyTask {
    pub(crate) value: &'static str,
}

impl TaskPayload for DummyTask {
    const TASK_TYPE: &'static str = "dummy_task";
    const TASK_VERSION: i32 = 1;
}

pub(crate) fn format() -> TaskFormat {
    TaskFormat::of::<DummyTask>()
}

pub(crate) fn claimed() -> LeasedTask {
    LeasedTask::new(
        TaskAttempt::try_new(
            Uuid::new_v4(),
            format(),
            json!({"value": "runtime"}),
            json!({"correlation_id": "test"}),
            2,
        )
        .unwrap(),
        TaskLease::new(Uuid::new_v4(), tokio::time::Instant::now() + Duration::from_secs(60)),
    )
}

#[derive(Default)]
pub(crate) struct Queue {
    pub(crate) incoming: Mutex<VecDeque<LeasedTask>>,
    pub(crate) outcomes: Mutex<Vec<(Uuid, SettlementRequest)>>,
    pub(crate) requested: Mutex<Vec<TaskFormat>>,
    pub(crate) claim_limits: Mutex<Vec<usize>>,
    pub(crate) recovery_limits: Mutex<Vec<usize>>,
    pub(crate) claim_error: AtomicBool,
    pub(crate) recovery_error: AtomicBool,
    pub(crate) settle_error: AtomicBool,
    pub(crate) lost_owner: AtomicBool,
    pub(crate) recovery_calls: AtomicUsize,
    pub(crate) hang_claim: AtomicBool,
    pub(crate) hang_recovery: AtomicBool,
    pub(crate) hang_settle: AtomicBool,
    pub(crate) failing_format: Mutex<Option<TaskFormat>>,
    pub(crate) claim_delay: Mutex<Duration>,
}

impl TaskQueue for Queue {
    async fn claim(
        &self,
        format: &TaskFormat,
        _: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
        let lease_deadline = tokio::time::Instant::now() + lease;
        self.requested.lock().unwrap().push(format.clone());
        self.claim_limits.lock().unwrap().push(limit);
        let delay = *self.claim_delay.lock().unwrap();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if self.hang_claim.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.claim_error.load(Ordering::SeqCst) || self.failing_format.lock().unwrap().as_ref() == Some(format) {
            return Err(TaskProcessingError::Unavailable);
        }
        let mut incoming = self.incoming.lock().unwrap();
        let mut result = Vec::new();
        while result.len() < limit && incoming.front().is_some_and(|message| message.attempt().format() == format) {
            let mut task = incoming.pop_front().unwrap();
            task = with_deadline(&task, lease_deadline);
            result.push(task);
        }
        Ok(result)
    }

    async fn settle(&self, task: &LeasedTask, outcome: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
        let confirmed = match &outcome {
            SettlementRequest::Completed => SettlementResult::Completed,
            SettlementRequest::Retry { .. } => SettlementResult::RetryScheduled,
            SettlementRequest::Failed { .. } => SettlementResult::Failed(TaskFailure {
                task_id: task.attempt().task_id(),
                format: task.attempt().format().clone(),
                attempt_number: task.attempt().number(),
                cause: TaskFailureCause::PermanentRejection,
            }),
        };
        self.outcomes.lock().unwrap().push((task.attempt().task_id(), outcome));
        if self.hang_settle.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.settle_error.load(Ordering::SeqCst) {
            return Err(TaskProcessingError::Unavailable);
        }
        if self.lost_owner.load(Ordering::SeqCst) {
            return Ok(SettlementResult::OwnershipLost);
        }
        Ok(confirmed)
    }

    async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
        self.recovery_limits.lock().unwrap().push(limit);
        self.recovery_calls.fetch_add(1, Ordering::SeqCst);
        if self.hang_recovery.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.recovery_error.load(Ordering::SeqCst) {
            return Err(TaskProcessingError::Unavailable);
        }
        Ok(LeaseRecoveryReport::default())
    }
}

pub(crate) struct Adapter {
    pub(crate) result: TaskResult,
    pub(crate) hang: bool,
}

impl TaskAdapter for Adapter {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn invoke(&self, _: &TaskContext) -> TaskResult {
        if self.hang {
            std::future::pending::<()>().await;
        }
        self.result.clone()
    }
}

pub(crate) fn adapter() -> Adapter {
    Adapter {
        result: TaskResult::Done,
        hang: false,
    }
}

#[cfg(feature = "worker")]
pub(crate) struct CountingAdapter(pub(crate) std::sync::Arc<AtomicUsize>);

#[cfg(feature = "worker")]
impl TaskAdapter for CountingAdapter {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }

    async fn invoke(&self, _: &TaskContext) -> TaskResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        TaskResult::Done
    }
}

pub(crate) fn with_deadline(task: &LeasedTask, deadline: tokio::time::Instant) -> LeasedTask {
    let token = task.lease().lock_token();
    LeasedTask::new(task.attempt().clone(), TaskLease::new(token, deadline))
}
#[cfg(feature = "worker")]
use cqrsx::task::{policy::Policies, runtime::worker_pool_config::TaskWorkerPoolConfig};
use tokio::task::Id;
pub(super) async fn wait_until(condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}
pub(super) struct IoProbe {
    pub(super) active: AtomicUsize,
    pub(super) peak: AtomicUsize,
    pub(super) built: AtomicUsize,
    pub(super) calls: std::sync::Mutex<Vec<(usize, Id, usize)>>,
    pub(super) gate: tokio::sync::Semaphore,
}
impl Default for IoProbe {
    fn default() -> Self {
        Self {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            built: AtomicUsize::new(0),
            calls: std::sync::Mutex::new(Vec::new()),
            gate: tokio::sync::Semaphore::new(0),
        }
    }
}
struct ActiveCall<'a>(&'a AtomicUsize);
impl Drop for ActiveCall<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
pub(super) struct IoAdapter {
    pub(super) probe: Arc<IoProbe>,
    pub(super) dispatcher: usize,
    pub(super) calls: AtomicUsize,
}

impl TaskAdapter for IoAdapter {
    fn formats(&self) -> Vec<TaskFormat> {
        vec![format()]
    }
    async fn invoke(&self, _: &TaskContext) -> TaskResult {
        let active = self.probe.active.fetch_add(1, Ordering::SeqCst) + 1;
        let _active = ActiveCall(&self.probe.active);
        self.probe.peak.fetch_max(active, Ordering::SeqCst);
        let calls = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.probe
            .calls
            .lock()
            .unwrap()
            .push((self.dispatcher, tokio::task::id(), calls));
        self.probe.gate.acquire().await.unwrap().forget();
        TaskResult::Done
    }
}
/// The one dispatcher of the workers over an `IoAdapter`; `probe.built` counts the dispatchers built.
pub(super) fn io_dispatcher(probe: Arc<IoProbe>) -> TaskDispatcher<IoAdapter> {
    let dispatcher = probe.built.fetch_add(1, Ordering::SeqCst);
    TaskDispatcher::try_new(vec![IoAdapter {
        probe,
        dispatcher,
        calls: AtomicUsize::new(0),
    }])
    .unwrap()
}
pub(super) fn worker_config(c: usize, q: usize, poll: Duration) -> TaskWorkerPoolConfig {
    TaskWorkerPoolConfig::try_new(c, q, poll, Duration::from_secs(30), Duration::from_secs(120)).unwrap()
}

pub(super) fn prepared_runtime<Q: TaskQueue + 'static, A: TaskAdapter + 'static>(
    queue: Arc<Q>,
    dispatcher: TaskDispatcher<A>,
    config: TaskWorkerPoolConfig,
) -> cqrsx::task::runtime::TaskRuntime<Q, A> {
    cqrsx::task::runtime::prepare(
        queue,
        Arc::new(dispatcher),
        cqrsx::task::runtime::config::TaskRuntimeConfig::try_new(TaskStorageConfig::default(), config).unwrap(),
        Policies::default(),
    )
}
