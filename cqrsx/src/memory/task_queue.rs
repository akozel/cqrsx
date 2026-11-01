//! A task queue in memory: the statements of the PostgreSQL queue (`postgres::task_queue`) over the outbox of a
//! [`MemoryEventRepository`], whose rows keep the columns of their delivery. The outbox and the queue are one table, as in
//! PostgreSQL: a task the journal publishes with its event is pending at once.
//!
//! The rules, by meaning:
//! - a task is pending from its write on, available at once, after its delay or at its time;
//! - a claim takes the pending tasks of one format whose time has come, by availability and then id, up to its limit;
//!   it leases them to the owner (a fresh token, the lease from now), counts the attempt and anchors the local
//!   deadline before it reads;
//! - a settlement needs the token of a lease that has not run out, else the ownership is lost; a completion and a
//!   permanent failure close the task, a retry makes it pending again after its pause, and on its last attempt fails it;
//!   the reason is kept, at most 2048 characters;
//! - a recovery takes the expired leases, by their end and then id, up to its limit: a task with attempts left is
//!   pending again after the queue's redelivery backoff, the others fail (`worker lease expired`).
//!
//! Where it differs from PostgreSQL:
//! - the clock is Tokio's (`tokio::time::Instant`), not the server's, so a paused test clock moves the pauses and the
//!   leases; a task due at a time (`TaskSchedule::at`) gets its delay from `Utc::now()` when it is written;
//! - every statement holds the journal's lock: there are no concurrent transactions to skip (`SKIP LOCKED`), and two
//!   claims never overlap, as in PostgreSQL;
//! - a row is always a valid delivery (`NewTask` checked it), so none is left to recovery for that;
//! - nothing keeps when a task was created or finished;
//! - [`lose_settlements`](MemoryTaskQueue::lose_settlements) lets a test lose the acknowledgements, as a process that
//!   stops after the effect and before its settlement.
use crate::{
    memory::event_repository::{MemoryEventRepository, OutboxEntry, OutboxRow},
    task::{NewTask, attempt::TaskAttempt, format::TaskFormat, policy::redelivery_backoff},
    worker::queue::{
        QueueRequestError, TaskProcessingError, TaskQueue,
        lease::{LeasedTask, TaskLease},
        settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult, TaskFailure, TaskFailureCause},
    },
};
use chrono::Utc;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;
use uuid::Uuid;

/// The longest reason a settlement keeps, as the PostgreSQL queue keeps it.
const MAX_REASON_CHARS: usize = 2048;

/// The reason a recovered task keeps.
const LEASE_EXPIRED: &str = "worker lease expired";

/// Where a task is in its delivery: the `status` of the outbox row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

/// One task of the queue as a test sees it: the outbox entry and its delivery.
#[derive(Clone, Debug, PartialEq)]
pub struct QueuedTask {
    pub aggregate_type: String,
    pub entry: OutboxEntry,
    pub status: TaskStatus,
    /// Claims so far.
    pub attempts: i32,
    /// Who holds the lease of a task in processing.
    pub owner: Option<String>,
    pub last_error: Option<String>,
}

/// The lease of a task in processing: `owner`, `lease_token`, `lease_until`.
#[derive(Debug)]
struct Lease {
    owner: String,
    token: Uuid,
    until: Instant,
}

/// The delivery columns of an outbox row.
#[derive(Debug)]
pub(crate) struct Delivery {
    status: TaskStatus,
    attempts: i32,
    available_at: Instant,
    lease: Option<Lease>,
    last_error: Option<String>,
}

impl Delivery {
    /// A task written `now`: pending, available at once, after its delay or at its time.
    pub(crate) fn pending(task: &NewTask, now: Instant) -> Self {
        let delay = match (task.schedule().delay(), task.schedule().time()) {
            (Some(delay), _) => delay,
            (None, Some(at)) => (at - Utc::now()).to_std().unwrap_or(Duration::ZERO),
            (None, None) => Duration::ZERO,
        };
        Self {
            status: TaskStatus::Pending,
            attempts: 0,
            available_at: now + delay,
            lease: None,
            last_error: None,
        }
    }
}

/// The task queue over the outbox of `journal`: see the [module](self). Clones share the journal and the switch of
/// [`lose_settlements`](Self::lose_settlements).
#[derive(Clone, Debug)]
pub struct MemoryTaskQueue {
    journal: MemoryEventRepository,
    lose: Arc<AtomicBool>,
}

impl MemoryTaskQueue {
    /// The queue of the tasks `journal` publishes.
    pub fn new(journal: MemoryEventRepository) -> Self {
        Self {
            journal,
            lose: Arc::default(),
        }
    }

    /// Every task of the outbox, in the order of the writes, with its delivery.
    pub fn tasks(&self) -> Vec<QueuedTask> {
        self.journal.outbox_rows(|rows| {
            rows.iter()
                .map(|row| QueuedTask {
                    aggregate_type: row.aggregate_type.clone(),
                    entry: row.entry.clone(),
                    status: row.delivery.status,
                    attempts: row.delivery.attempts,
                    owner: row.delivery.lease.as_ref().map(|lease| lease.owner.clone()),
                    last_error: row.delivery.last_error.clone(),
                })
                .collect()
        })
    }

    /// While `lose` holds, a settlement never reaches the queue: the task stays leased until its lease runs out and a
    /// recovery returns it, as when the process stops after the effect and before the acknowledgement. The executor gets
    /// `TaskProcessingError::Unavailable`.
    pub fn lose_settlements(&self, lose: bool) {
        self.lose.store(lose, Ordering::SeqCst);
    }
}

/// A limit the PostgreSQL queue binds as a `bigint`.
fn checked_limit(limit: usize) -> Result<usize, TaskProcessingError> {
    i64::try_from(limit).map_err(|_| QueueRequestError::InvalidLimit)?;
    Ok(limit)
}

/// The `rows` that `selected` keeps, ordered by `key` and cut to `limit`.
fn take<K: Ord>(
    rows: &mut [OutboxRow],
    selected: impl Fn(&OutboxRow) -> bool,
    key: impl Fn(&OutboxRow) -> K,
    limit: usize,
) -> Vec<&mut OutboxRow> {
    let mut taken: Vec<&mut OutboxRow> = rows.iter_mut().filter(|row| selected(row)).collect();
    taken.sort_by_key(|row| key(row));
    taken.truncate(limit);
    taken
}

fn reason(text: &str) -> Option<String> {
    Some(text.chars().take(MAX_REASON_CHARS).collect())
}

fn failure(attempt: &TaskAttempt, cause: TaskFailureCause) -> SettlementResult {
    SettlementResult::Failed(TaskFailure {
        task_id: attempt.task_id(),
        format: attempt.format().clone(),
        attempt_number: attempt.number(),
        cause,
    })
}

impl TaskQueue for MemoryTaskQueue {
    async fn claim(
        &self,
        format: &TaskFormat,
        owner: &str,
        limit: usize,
        lease_duration: Duration,
    ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
        if lease_duration.is_zero() {
            return Err(QueueRequestError::InvalidLeaseDuration.into());
        }
        let now = Instant::now();
        let until = now
            .checked_add(lease_duration)
            .ok_or(QueueRequestError::InvalidLeaseDuration)?;
        let limit = checked_limit(limit)?;
        self.journal.outbox_rows(|rows| {
            let ready = |row: &OutboxRow| {
                row.delivery.status == TaskStatus::Pending
                    && row.entry.task.format() == format
                    && row.delivery.available_at <= now
            };
            let order = |row: &OutboxRow| (row.delivery.available_at, row.entry.task.id());
            take(rows, ready, order, limit)
                .into_iter()
                .map(|row| {
                    let (task, delivery) = (&row.entry.task, &mut row.delivery);
                    let token = Uuid::new_v4();
                    delivery.status = TaskStatus::Processing;
                    delivery.attempts += 1;
                    delivery.lease = Some(Lease {
                        owner: owner.to_owned(),
                        token,
                        until,
                    });
                    let attempt = TaskAttempt::try_new(
                        task.id(),
                        task.format().clone(),
                        task.payload().clone(),
                        task.metadata().clone(),
                        delivery.attempts,
                    )
                    .map_err(|_| TaskProcessingError::InvalidDelivery)?;
                    Ok(LeasedTask::new(attempt, TaskLease::new(token, until)))
                })
                .collect()
        })
    }

    async fn settle(&self, task: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
        if self.lose.load(Ordering::SeqCst) {
            return Err(TaskProcessingError::Unavailable);
        }
        if let SettlementRequest::Retry { delay, .. } = &request {
            i64::try_from(delay.as_millis()).map_err(|_| QueueRequestError::InvalidRetryDelay)?;
        }
        let now = Instant::now();
        let attempt = task.attempt();
        Ok(self.journal.outbox_rows(|rows| {
            let owned = rows.iter_mut().find(|row| {
                row.entry.task.id() == attempt.task_id()
                    && row
                        .delivery
                        .lease
                        .as_ref()
                        .is_some_and(|lease| lease.token == task.lease().token() && lease.until > now)
            });
            let Some(row) = owned else {
                return SettlementResult::OwnershipLost;
            };
            let delivery = &mut row.delivery;
            delivery.lease = None;
            match request {
                SettlementRequest::Completed => {
                    delivery.status = TaskStatus::Completed;
                    delivery.last_error = None;
                    SettlementResult::Completed
                }
                SettlementRequest::Failed { reason: text } => {
                    delivery.status = TaskStatus::Failed;
                    delivery.last_error = reason(&text);
                    failure(attempt, TaskFailureCause::HandlerFailure)
                }
                SettlementRequest::Retry { delay, reason: text } => {
                    delivery.last_error = reason(&text);
                    if delivery.attempts >= i32::from(row.entry.task.max_attempts().get()) {
                        delivery.status = TaskStatus::Failed;
                        failure(attempt, TaskFailureCause::AttemptLimitReached)
                    } else {
                        delivery.status = TaskStatus::Pending;
                        delivery.available_at = now + delay;
                        SettlementResult::RetryScheduled
                    }
                }
            }
        }))
    }

    async fn recover_expired(&self, limit: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
        let limit = checked_limit(limit)?;
        let now = Instant::now();
        Ok(self.journal.outbox_rows(|rows| {
            let expired = |row: &OutboxRow| row.delivery.lease.as_ref().is_some_and(|lease| lease.until <= now);
            let order = |row: &OutboxRow| (row.delivery.lease.as_ref().map(|lease| lease.until), row.entry.task.id());
            let (mut retried, mut failures) = (0, Vec::new());
            let taken = take(rows, expired, order, limit);
            for row in taken {
                let (task, delivery) = (&row.entry.task, &mut row.delivery);
                delivery.lease = None;
                delivery.last_error = Some(LEASE_EXPIRED.to_owned());
                if delivery.attempts >= i32::from(task.max_attempts().get()) {
                    delivery.status = TaskStatus::Failed;
                    failures.push(TaskFailure {
                        task_id: task.id(),
                        format: task.format().clone(),
                        attempt_number: delivery.attempts,
                        cause: TaskFailureCause::LeaseExpired,
                    });
                } else {
                    delivery.status = TaskStatus::Pending;
                    delivery.available_at = now + redelivery_backoff(delivery.attempts);
                    retried += 1;
                }
            }
            LeaseRecoveryReport::new(retried, failures)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            aggregate::tests::{Delta, Ledger},
            event::EventEnvelope,
        },
        journal::{repository::EventRepository, serialized_event::SerializedEvent},
        task::TaskSchedule,
    };
    use serde_json::json;
    use std::{collections::HashMap, num::NonZeroU16};

    const LEASE: Duration = Duration::from_secs(60);

    fn format(name: &str) -> TaskFormat {
        TaskFormat::try_new(name, 1).unwrap()
    }

    fn task(name: &str, n: u8) -> NewTask {
        NewTask::from_payload(format(name), json!({ "n": n }))
            .unwrap()
            .with_metadata(serde_json::from_value(json!({ "correlation_id": "c-1" })).unwrap())
    }

    /// The journal and its queue, with the tasks published by a write of `aggregate` at `sequence`.
    async fn publish(journal: &MemoryEventRepository, aggregate: &str, sequence: usize, tasks: &[NewTask]) {
        let event = SerializedEvent::try_from(&EventEnvelope::<Ledger> {
            aggregate_id: aggregate.into(),
            sequence,
            payload: Delta(1),
            metadata: HashMap::new(),
        })
        .unwrap();
        journal.persist_with_tasks::<Ledger>(&[event], None, tasks).await.unwrap();
    }

    fn ids(claimed: &[LeasedTask]) -> Vec<Uuid> {
        claimed.iter().map(|task| task.attempt().task_id()).collect()
    }

    fn statuses(queue: &MemoryTaskQueue) -> Vec<(TaskStatus, i32)> {
        queue.tasks().iter().map(|task| (task.status, task.attempts)).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn a_task_is_pending_from_its_write_and_a_delayed_one_once_its_time_has_come() {
        let journal = MemoryEventRepository::default();
        let queue = MemoryTaskQueue::new(journal.clone());
        let now = task("notify", 1);
        let after = task("notify", 2).with_schedule(TaskSchedule::after(Duration::from_secs(10)).unwrap());
        let at = task("notify", 3).with_schedule(TaskSchedule::at(Utc::now() + chrono::TimeDelta::seconds(20)));
        let past = task("notify", 4).with_schedule(TaskSchedule::at(Utc::now() - chrono::TimeDelta::hours(1)));
        publish(&journal, "a", 1, &[now.clone(), after.clone(), at.clone(), past.clone()]).await;
        assert_eq!(statuses(&queue), [(TaskStatus::Pending, 0); 4]);
        let mut due = vec![now.id(), past.id()];
        due.sort();
        let claimed = queue.claim(&format("notify"), "w", 10, LEASE).await.unwrap();
        assert_eq!(ids(&claimed), due, "due at once: by availability, then id");
        tokio::time::advance(Duration::from_secs(10)).await;
        let claimed = queue.claim(&format("notify"), "w", 10, LEASE).await.unwrap();
        assert_eq!(ids(&claimed), [after.id()]);
        tokio::time::advance(Duration::from_secs(10)).await;
        let claimed = queue.claim(&format("notify"), "w", 10, LEASE).await.unwrap();
        assert_eq!(ids(&claimed), [at.id()]);
        assert_eq!(statuses(&queue), [(TaskStatus::Processing, 1); 4]);
        let queued = &queue.tasks()[0];
        assert_eq!(
            (
                queued.aggregate_type.as_str(),
                queued.owner.as_deref(),
                &queued.entry,
                &queued.last_error
            ),
            ("ledger", Some("w"), &journal.outbox::<Ledger>()[0], &None)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_claim_takes_the_ready_tasks_of_its_format_by_availability_then_id_up_to_its_limit() {
        let journal = MemoryEventRepository::default();
        let queue = MemoryTaskQueue::new(journal.clone());
        let first = task("notify", 1);
        publish(&journal, "a", 1, std::slice::from_ref(&first)).await;
        tokio::time::advance(Duration::from_secs(1)).await;
        let second = task("notify", 2);
        publish(&journal, "b", 1, std::slice::from_ref(&second)).await;
        tokio::time::advance(Duration::from_secs(1)).await;
        let (third, fourth, other) = (task("notify", 3), task("notify", 4), task("other", 5));
        publish(&journal, "c", 1, &[third.clone(), fourth.clone(), other.clone()]).await;
        let claimed = queue.claim(&format("notify"), "w", 2, LEASE).await.unwrap();
        assert_eq!(
            ids(&claimed),
            [first.id(), second.id()],
            "the older first, whatever their ids"
        );
        let mut tied = vec![third.id(), fourth.id()];
        tied.sort();
        let claimed = queue.claim(&format("notify"), "w", 10, LEASE).await.unwrap();
        assert_eq!(ids(&claimed), tied, "written at once: by id");
        assert!(queue.claim(&format("notify"), "w", 10, LEASE).await.unwrap().is_empty());
        assert_eq!(
            ids(&queue.claim(&format("other"), "w", 10, LEASE).await.unwrap()),
            [other.id()]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_claim_leases_its_tasks_counts_the_attempt_and_issues_a_fresh_token() {
        let journal = MemoryEventRepository::default();
        let queue = MemoryTaskQueue::new(journal.clone());
        let published = task("notify", 1);
        publish(&journal, "a", 1, std::slice::from_ref(&published)).await;
        let before = Instant::now();
        let first = &queue.claim(&format("notify"), "w-1", 1, LEASE).await.unwrap().remove(0);
        let attempt = first.attempt();
        assert_eq!(
            (attempt.number(), attempt.format(), attempt.payload(), attempt.metadata()),
            (1, published.format(), published.payload(), published.metadata())
        );
        assert_eq!(first.lease().deadline(), before + LEASE);
        let retry = SettlementRequest::Retry {
            delay: Duration::ZERO,
            reason: "busy".into(),
        };
        assert_eq!(queue.settle(first, retry).await.unwrap(), SettlementResult::RetryScheduled);
        let second = &queue.claim(&format("notify"), "w-2", 1, LEASE).await.unwrap().remove(0);
        assert_eq!(second.attempt().number(), 2);
        assert_ne!(second.lease().token(), first.lease().token());
        assert_eq!(queue.tasks()[0].owner.as_deref(), Some("w-2"));
        assert_eq!(queue.tasks()[0].last_error.as_deref(), Some("busy"));
    }

    #[tokio::test]
    async fn unrepresentable_leases_limits_and_pauses_are_refused() {
        let journal = MemoryEventRepository::default();
        let queue = MemoryTaskQueue::new(journal.clone());
        publish(&journal, "a", 1, &[task("notify", 1)]).await;
        let notify = format("notify");
        for lease in [Duration::ZERO, Duration::MAX] {
            assert_eq!(
                queue.claim(&notify, "w", 1, lease).await.unwrap_err(),
                QueueRequestError::InvalidLeaseDuration.into()
            );
        }
        let too_many = usize::MAX;
        assert_eq!(
            queue.claim(&notify, "w", too_many, LEASE).await.unwrap_err(),
            QueueRequestError::InvalidLimit.into()
        );
        assert_eq!(
            queue.recover_expired(too_many).await.unwrap_err(),
            QueueRequestError::InvalidLimit.into()
        );
        let claimed = queue.claim(&notify, "w", 1, LEASE).await.unwrap().remove(0);
        let forever = SettlementRequest::Retry {
            delay: Duration::MAX,
            reason: "later".into(),
        };
        assert_eq!(
            queue.settle(&claimed, forever).await.unwrap_err(),
            QueueRequestError::InvalidRetryDelay.into()
        );
        assert_eq!(statuses(&queue), [(TaskStatus::Processing, 1)], "nothing was settled");
    }

    #[tokio::test(start_paused = true)]
    async fn a_settlement_needs_the_token_of_a_live_lease() {
        let journal = MemoryEventRepository::default();
        let queue = MemoryTaskQueue::new(journal.clone());
        publish(&journal, "a", 1, &[task("notify", 1)]).await;
        tokio::time::advance(Duration::from_secs(1)).await;
        publish(&journal, "b", 1, &[task("notify", 2)]).await;
        let claimed = queue.claim(&format("notify"), "w", 2, LEASE).await.unwrap();
        let forged = LeasedTask::new(
            claimed[0].attempt().clone(),
            TaskLease::new(Uuid::new_v4(), claimed[0].lease().deadline()),
        );
        assert_eq!(
            queue.settle(&forged, SettlementRequest::Completed).await.unwrap(),
            SettlementResult::OwnershipLost
        );
        assert_eq!(
            queue.settle(&claimed[0], SettlementRequest::Completed).await.unwrap(),
            SettlementResult::Completed
        );
        assert_eq!(
            queue.settle(&claimed[0], SettlementRequest::Completed).await.unwrap(),
            SettlementResult::OwnershipLost,
            "a settled task has no lease any more"
        );
        tokio::time::advance(LEASE).await;
        assert_eq!(
            queue.settle(&claimed[1], SettlementRequest::Completed).await.unwrap(),
            SettlementResult::OwnershipLost,
            "the lease ran out"
        );
        assert_eq!(statuses(&queue), [(TaskStatus::Completed, 1), (TaskStatus::Processing, 1)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_completion_and_a_permanent_failure_close_the_task() {
        let journal = MemoryEventRepository::default();
        let queue = MemoryTaskQueue::new(journal.clone());
        publish(&journal, "a", 1, &[task("notify", 1)]).await;
        tokio::time::advance(Duration::from_secs(1)).await;
        publish(&journal, "b", 1, &[task("notify", 2)]).await;
        let claimed = queue.claim(&format("notify"), "w", 2, LEASE).await.unwrap();
        let retry = SettlementRequest::Retry {
            delay: Duration::ZERO,
            reason: "busy".into(),
        };
        queue.settle(&claimed[0], retry).await.unwrap();
        let again = queue.claim(&format("notify"), "w", 1, LEASE).await.unwrap().remove(0);
        assert_eq!(
            queue.settle(&again, SettlementRequest::Completed).await.unwrap(),
            SettlementResult::Completed
        );
        let failed = queue
            .settle(
                &claimed[1],
                SettlementRequest::Failed {
                    reason: "x".repeat(3000),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            failed
                .failure()
                .map(|failure| (failure.task_id, failure.attempt_number, failure.cause)),
            Some((claimed[1].attempt().task_id(), 1, TaskFailureCause::HandlerFailure))
        );
        let tasks = queue.tasks();
        let closed: Vec<_> = tasks
            .iter()
            .map(|task| (task.status, task.owner.clone(), task.last_error.as_ref().map(String::len)))
            .collect();
        assert_eq!(
            closed,
            [(TaskStatus::Completed, None, None), (TaskStatus::Failed, None, Some(2048))],
            "a completion clears the last error, a reason keeps 2048 characters"
        );
        assert!(queue.claim(&format("notify"), "w", 2, LEASE).await.unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_waits_its_pause_and_the_last_attempt_fails_the_task() {
        let journal = MemoryEventRepository::default();
        let queue = MemoryTaskQueue::new(journal.clone());
        publish(
            &journal,
            "a",
            1,
            &[task("notify", 1).with_max_attempts(NonZeroU16::new(2).unwrap())],
        )
        .await;
        let retry = || SettlementRequest::Retry {
            delay: Duration::from_secs(5),
            reason: "busy".into(),
        };
        let first = queue.claim(&format("notify"), "w", 1, LEASE).await.unwrap().remove(0);
        assert_eq!(queue.settle(&first, retry()).await.unwrap(), SettlementResult::RetryScheduled);
        tokio::time::advance(Duration::from_millis(4999)).await;
        assert!(queue.claim(&format("notify"), "w", 1, LEASE).await.unwrap().is_empty());
        tokio::time::advance(Duration::from_millis(1)).await;
        let second = queue.claim(&format("notify"), "w", 1, LEASE).await.unwrap().remove(0);
        let exhausted = queue.settle(&second, retry()).await.unwrap();
        assert_eq!(
            exhausted.failure().map(|failure| (failure.attempt_number, failure.cause)),
            Some((2, TaskFailureCause::AttemptLimitReached))
        );
        assert_eq!(statuses(&queue), [(TaskStatus::Failed, 2)]);
        assert_eq!(queue.tasks()[0].last_error.as_deref(), Some("busy"));
    }

    #[tokio::test(start_paused = true)]
    async fn expired_leases_come_back_after_the_redelivery_backoff_or_fail_on_the_last_attempt() {
        let journal = MemoryEventRepository::default();
        let queue = MemoryTaskQueue::new(journal.clone());
        let (last, again) = (
            task("notify", 1).with_max_attempts(NonZeroU16::new(1).unwrap()),
            task("notify", 2),
        );
        publish(&journal, "a", 1, std::slice::from_ref(&last)).await;
        tokio::time::advance(Duration::from_secs(1)).await;
        publish(&journal, "b", 1, std::slice::from_ref(&again)).await;
        let late = task("notify", 3);
        tokio::time::advance(Duration::from_secs(1)).await;
        publish(&journal, "c", 1, std::slice::from_ref(&late)).await;
        queue.claim(&format("notify"), "w", 2, LEASE).await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        queue.claim(&format("notify"), "w", 1, LEASE).await.unwrap();
        tokio::time::advance(LEASE - Duration::from_secs(1)).await;
        assert_eq!(
            queue.recover_expired(10).await.unwrap().processed_count(),
            2,
            "the third lease runs a second longer"
        );
        let report = queue.recover_expired(10).await.unwrap();
        assert_eq!(report.processed_count(), 0);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(queue.recover_expired(10).await.unwrap().retry_scheduled_count(), 1);
        let tasks = queue.tasks();
        let recovered: Vec<_> = tasks
            .iter()
            .map(|task| (task.status, task.owner.is_none(), task.last_error.as_deref()))
            .collect();
        assert_eq!(
            recovered,
            [
                (TaskStatus::Failed, true, Some("worker lease expired")),
                (TaskStatus::Pending, true, Some("worker lease expired")),
                (TaskStatus::Pending, true, Some("worker lease expired"))
            ]
        );
        assert!(
            queue.claim(&format("notify"), "w", 2, LEASE).await.unwrap().is_empty(),
            "the backoff of a first attempt is at least 4 s"
        );
        tokio::time::advance(Duration::from_secs(6)).await;
        let redelivered = queue.claim(&format("notify"), "w", 2, LEASE).await.unwrap();
        assert_eq!(redelivered.len(), 2);
        assert!(redelivered.iter().all(|task| task.attempt().number() == 2));
    }

    #[tokio::test(start_paused = true)]
    async fn a_recovery_takes_the_leases_that_ran_out_first_up_to_its_limit_and_reports_the_failures() {
        let journal = MemoryEventRepository::default();
        let queue = MemoryTaskQueue::new(journal.clone());
        let (first, second) = (
            task("notify", 1).with_max_attempts(NonZeroU16::new(1).unwrap()),
            task("notify", 2),
        );
        publish(&journal, "a", 1, std::slice::from_ref(&first)).await;
        tokio::time::advance(Duration::from_secs(1)).await;
        publish(&journal, "b", 1, std::slice::from_ref(&second)).await;
        queue.claim(&format("notify"), "w", 1, LEASE).await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        queue.claim(&format("notify"), "w", 1, LEASE).await.unwrap();
        tokio::time::advance(LEASE).await;
        let report = queue.recover_expired(1).await.unwrap();
        let failed: Vec<_> = report
            .failures()
            .iter()
            .map(|failure| (failure.task_id, failure.attempt_number, failure.cause))
            .collect();
        assert_eq!(
            (report.retry_scheduled_count(), failed),
            (0, vec![(first.id(), 1, TaskFailureCause::LeaseExpired)]),
            "the lease that ran out first"
        );
        assert_eq!(statuses(&queue), [(TaskStatus::Failed, 1), (TaskStatus::Processing, 1)]);
    }

    #[tokio::test(start_paused = true)]
    async fn lost_settlements_leave_the_task_leased_until_its_lease_runs_out() {
        let journal = MemoryEventRepository::default();
        let queue = MemoryTaskQueue::new(journal.clone());
        publish(&journal, "a", 1, &[task("notify", 1)]).await;
        let claimed = queue.claim(&format("notify"), "w", 1, LEASE).await.unwrap().remove(0);
        queue.clone().lose_settlements(true);
        assert_eq!(
            queue.settle(&claimed, SettlementRequest::Completed).await.unwrap_err(),
            TaskProcessingError::Unavailable
        );
        assert_eq!(
            statuses(&queue),
            [(TaskStatus::Processing, 1)],
            "the acknowledgement never arrived"
        );
        queue.lose_settlements(false);
        tokio::time::advance(LEASE).await;
        assert_eq!(queue.recover_expired(10).await.unwrap().retry_scheduled_count(), 1);
        tokio::time::advance(Duration::from_secs(6)).await;
        let again = queue.claim(&format("notify"), "w", 1, LEASE).await.unwrap().remove(0);
        assert_eq!(again.attempt().number(), 2);
        assert_eq!(
            queue.settle(&again, SettlementRequest::Completed).await.unwrap(),
            SettlementResult::Completed
        );
        assert_eq!(statuses(&queue), [(TaskStatus::Completed, 2)]);
    }
}
