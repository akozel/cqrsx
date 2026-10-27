//! The names of the task policies: what a task of each type waits before its next attempt and how many attempts it
//! gets. The `fan` module declares their values from its variables and binds every task type to one of them
//! ([`fan::module`](crate::fan::module)).
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
