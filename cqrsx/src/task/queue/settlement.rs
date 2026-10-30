//! Requested transitions and confirmed settlement/recovery outcomes.
use crate::task::format::TaskFormat;
use std::time::Duration;
use uuid::Uuid;
/// Requested transition; storage may reject it or exhaust the attempt budget.
#[derive(Clone, Debug, PartialEq)]
pub enum SettlementRequest {
    Completed,
    Retry { delay: Duration, reason: String },
    Failed { reason: String },
}

/// Fixed safe diagnostics, never a handler's arbitrary error text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TaskFailureCause {
    PermanentRejection,
    AttemptLimitReached,
    LeaseExpired,
}
impl TaskFailureCause {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PermanentRejection => "permanent failure",
            Self::AttemptLimitReached => "retry limit reached",
            Self::LeaseExpired => "worker lease expired",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskFailure {
    pub task_id: Uuid,
    pub format: TaskFormat,
    pub attempt_number: i32,
    pub cause: TaskFailureCause,
}

/// Only confirmed transitions are returned, never before transaction commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SettlementResult {
    OwnershipLost,
    Completed,
    RetryScheduled,
    Failed(TaskFailure),
}
impl SettlementResult {
    /// Applied also includes a durable retry, not just completed work.
    pub fn is_applied(&self) -> bool {
        match self {
            Self::OwnershipLost => false,
            Self::Completed | Self::RetryScheduled | Self::Failed(_) => true,
        }
    }
    pub fn failure(&self) -> Option<&TaskFailure> {
        match self {
            Self::Failed(failure) => Some(failure),
            Self::OwnershipLost | Self::Completed | Self::RetryScheduled => None,
        }
    }
}

/// Recovery report after commit. Total is derived, not a redundant counter.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LeaseRecoveryReport {
    retry_scheduled_count: usize,
    failures: Vec<TaskFailure>,
}
impl LeaseRecoveryReport {
    pub fn new(retry_scheduled_count: usize, failures: Vec<TaskFailure>) -> Self {
        Self {
            retry_scheduled_count,
            failures,
        }
    }
    pub fn processed_count(&self) -> usize {
        self.retry_scheduled_count + self.failures.len()
    }
    pub fn retry_scheduled_count(&self) -> usize {
        self.retry_scheduled_count
    }
    pub fn failures(&self) -> &[TaskFailure] {
        &self.failures
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn confirmed_results_and_recovery_counts_are_unambiguous() {
        let failure = TaskFailure {
            task_id: Uuid::new_v4(),
            format: TaskFormat::new("test", 1).unwrap(),
            attempt_number: 1,
            cause: TaskFailureCause::AttemptLimitReached,
        };
        for result in [SettlementResult::Completed, SettlementResult::RetryScheduled] {
            assert!(result.is_applied());
            assert!(result.failure().is_none());
        }
        assert!(!SettlementResult::OwnershipLost.is_applied());
        assert!(SettlementResult::OwnershipLost.failure().is_none());
        assert!(
            SettlementResult::Failed(failure.clone()).is_applied(),
            "a failure is a confirmed transition"
        );
        assert_eq!(SettlementResult::Failed(failure.clone()).failure(), Some(&failure));
        let report = LeaseRecoveryReport::new(2, vec![failure]);
        assert_eq!(report.processed_count(), 3);
        assert_eq!(report.retry_scheduled_count(), 2);
    }
}
