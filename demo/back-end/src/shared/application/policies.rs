//! The names of the task policies: what a task of each type waits before its next attempt and how many attempts it
//! gets. The values come from the configuration, and the composition binds every task type to one of them
//! ([`policies`](crate::bootstrap::tasks::policies)) until the modules register their tasks themselves (backlog 05/11).
//! A reaction never names a policy: the type of its task has one; nor does a handler: the task runtime pauses a retry
//! by the policy of its task's type.
use cqrsx::task::policy::PolicyName;
use std::time::Duration;

/// A step of the chain between the aggregates (`encore_record_ticket`, `encore_record_cancellation`): one second
/// between attempts ([`CHAIN_RETRY_AFTER`]).
pub const STEP: PolicyName = PolicyName::new("step");

/// The pause of [`STEP`] before every next attempt.
pub const CHAIN_RETRY_AFTER: Duration = Duration::from_secs(1);

/// An e-mail (`encore_send_e_ticket`, `encore_issue_refund`): from `ENCORE_TASK_RETRY_BASE_MS`, doubling up to
/// `ENCORE_TASK_RETRY_MAX_MS`.
pub const MAIL: PolicyName = PolicyName::new("mail");
