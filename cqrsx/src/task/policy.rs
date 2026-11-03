//! Task policies: how long a task waits before its next attempt and how many attempts it gets.
//!
//! A policy is a value ([`TaskPolicy`]: a [`RetryPause`] and an attempt budget), `Copy`: types and modules that behave
//! alike share one by passing the same value. [`Policies`] is the table "task type -> policy" that the planner of the
//! reactions (the budget, written into the task when the command commits) and the executor of the task runtime (the
//! pause before a retry, [`Policies::pause`]) read; a reaction or a handler never names a policy. A type nobody
//! registered gets [`TaskPolicy::default`]: [`DEFAULT_MAX_ATTEMPTS`] attempts and the queue's own redelivery backoff
//! ([`RetryPause::redelivery`]). The table is shared (`Arc`), so a clone costs nothing.
//!
//! ```
//! use cqrsx::task::{DEFAULT_MAX_ATTEMPTS, Task, format::TaskFormat, policy::{Policies, PolicyError, RetryPause, TaskPolicy}};
//! use serde::Serialize;
//! use std::{num::NonZeroU16, time::Duration};
//!
//! #[derive(Serialize, Task)]
//! #[task(name = "issue_refund", version = 1)]
//! struct IssueRefund {
//!     cancellation_id: String,
//! }
//!
//! #[derive(Serialize, Task)]
//! #[task(name = "audit", version = 1)]
//! struct Audit {}
//!
//! #[derive(Serialize, Task)]
//! #[task(name = "unlisted", version = 1)]
//! struct Unlisted {}
//!
//! // E-mails back off from one second to eight and get six attempts; an audit keeps the defaults.
//! let mail = RetryPause::backoff(Duration::from_secs(1), Duration::from_secs(8));
//! let policy = TaskPolicy::new(mail, NonZeroU16::new(6).unwrap());
//! let policies = Policies::builder()
//!     .task::<IssueRefund>(policy)
//!     .task::<Audit>(TaskPolicy::default())
//!     .build()?;
//! let refund = TaskFormat::of::<IssueRefund>();
//! assert_eq!(policies.policy_for(&refund), policy);
//! assert_eq!(policies.max_attempts(&refund).get(), 6);
//! let pauses: Vec<u64> = (1..=5).map(|attempt| policies.pause(&refund, attempt).as_secs()).collect();
//! assert_eq!(pauses, [1, 2, 4, 8, 8]);
//! // A type nobody registered gets the default policy, the one `TaskPolicy::default()` registers.
//! let unlisted = TaskFormat::of::<Unlisted>();
//! assert_eq!(policies.policy_for(&unlisted), TaskPolicy::default());
//! assert_eq!(policies.max_attempts(&unlisted), DEFAULT_MAX_ATTEMPTS);
//! // A type is registered once.
//! let twice = Policies::builder().task::<Audit>(policy).task::<Audit>(policy).build();
//! assert_eq!(twice.unwrap_err(), PolicyError::TaskTwice(TaskFormat::of::<Audit>()));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
use crate::task::{DEFAULT_MAX_ATTEMPTS, Task, format::TaskFormat};
use std::{
    collections::{HashMap, hash_map::Entry},
    num::NonZeroU16,
    sync::Arc,
    time::Duration,
};

/// The pause before a task's next attempt, by the number of the attempt that failed: the same every time
/// ([`fixed`](Self::fixed)), doubling from a base up to a maximum ([`backoff`](Self::backoff)), or the queue's own
/// redelivery backoff ([`redelivery`](Self::redelivery)). The first two have no jitter: the pause of attempt `n` is
/// always the same.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPause(Pause);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pause {
    Fixed(Duration),
    Backoff { base: Duration, max: Duration },
    Redelivery,
}

impl RetryPause {
    /// `pause` after every attempt.
    pub const fn fixed(pause: Duration) -> Self {
        Self(Pause::Fixed(pause))
    }

    /// `min(base · 2^(n−1), max)` after attempt `n`; an attempt below 1 counts as the first, the doubling stops at
    /// 2^16 and the product saturates. A base above `max` pauses `max` every time.
    pub const fn backoff(base: Duration, max: Duration) -> Self {
        Self(Pause::Backoff { base, max })
    }

    /// The queue's redelivery backoff, the one a lease that ran out or a handler that timed out waits as well
    /// ([`redelivery_backoff`]): 5 s · 2^(n−1) up to 300 s, each pause drawn within ±20 % (a new draw on every call).
    pub const fn redelivery() -> Self {
        Self(Pause::Redelivery)
    }

    /// The pause after attempt `attempt` failed (the first attempt is 1). [`redelivery`](Self::redelivery) draws
    /// anew on every call.
    #[must_use]
    pub fn pause(&self, attempt: i32) -> Duration {
        match self.0 {
            Pause::Fixed(pause) => pause,
            Pause::Backoff { base, max } => {
                let exponent = u32::try_from(attempt.max(1) - 1).unwrap_or(u32::MAX).min(16);
                base.saturating_mul(2u32.saturating_pow(exponent)).min(max)
            }
            Pause::Redelivery => redelivery_backoff(attempt),
        }
    }
}

/// What a policy decides for the tasks of a type: the pause before the next attempt and the attempt budget, the
/// first attempt included. The budget is positive by its type and fits the queue's `max_attempts` column (`INTEGER`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskPolicy {
    retry: RetryPause,
    max_attempts: NonZeroU16,
}

impl TaskPolicy {
    pub fn new(retry: RetryPause, max_attempts: NonZeroU16) -> Self {
        Self { retry, max_attempts }
    }

    pub fn retry(&self) -> RetryPause {
        self.retry
    }

    pub fn max_attempts(&self) -> NonZeroU16 {
        self.max_attempts
    }
}

/// The framework's defaults: [`DEFAULT_MAX_ATTEMPTS`] attempts and the queue's [`RetryPause::redelivery`].
impl Default for TaskPolicy {
    fn default() -> Self {
        Self::new(RetryPause::redelivery(), DEFAULT_MAX_ATTEMPTS)
    }
}

/// The policy of every task type, by format. Shared (`Arc`): the planner of the reactions and the executor of the
/// task runtime read it, and a clone is cheap. The default table is empty: every type has [`TaskPolicy::default`].
#[derive(Clone, Debug, Default)]
pub struct Policies(Arc<HashMap<TaskFormat, TaskPolicy>>);

impl Policies {
    /// A builder of the table: [`task`](PoliciesBuilder::task) per type, checked when
    /// [`build`](PoliciesBuilder::build) runs.
    pub fn builder() -> PoliciesBuilder {
        PoliciesBuilder::default()
    }

    /// The policy of the task type `format`; [`TaskPolicy::default`] for a type nobody registered.
    pub fn policy_for(&self, format: &TaskFormat) -> TaskPolicy {
        self.0.get(format).copied().unwrap_or_default()
    }

    /// The attempt budget of the task type `format`: its policy's.
    #[must_use]
    pub fn max_attempts(&self, format: &TaskFormat) -> NonZeroU16 {
        self.policy_for(format).max_attempts()
    }

    /// The pause after attempt `attempt` of the task type `format` failed: its policy's (for the default policy a new
    /// draw of [`redelivery_backoff`] on every call).
    #[must_use]
    pub fn pause(&self, format: &TaskFormat, attempt: i32) -> Duration {
        self.policy_for(format).retry().pause(attempt)
    }
}

/// The builder of [`Policies`]: keeps the registrations in the order of the calls; [`build`](Self::build) refuses a
/// type registered twice.
#[derive(Clone, Debug, Default)]
pub struct PoliciesBuilder {
    tasks: Vec<(TaskFormat, TaskPolicy)>,
}

impl PoliciesBuilder {
    /// The task type `T` follows `policy`; a type is registered once.
    #[must_use]
    pub fn task<T: Task>(mut self, policy: TaskPolicy) -> Self {
        self.tasks.push((TaskFormat::of::<T>(), policy));
        self
    }

    /// The table, or the first type that was registered twice.
    pub fn build(self) -> Result<Policies, PolicyError> {
        let mut table = HashMap::with_capacity(self.tasks.len());
        for (format, policy) in self.tasks {
            match table.entry(format) {
                Entry::Occupied(entry) => return Err(PolicyError::TaskTwice(entry.key().clone())),
                Entry::Vacant(entry) => entry.insert(policy),
            };
        }
        Ok(Policies(Arc::new(table)))
    }
}

/// Jittered exponential redelivery backoff, also used after lease expiry.
pub fn redelivery_backoff(attempt: i32) -> Duration {
    let exponent = attempt.saturating_sub(1).clamp(0, 6) as u32;
    let base = (5_u64 * 2_u64.pow(exponent)).min(300);
    let jitter = rand::random_range(0.8..=1.2);
    Duration::from_secs_f64((base as f64 * jitter).min(300.0))
}

/// Why a table of policies was refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PolicyError {
    #[error("the task type `{0}` is registered twice")]
    TaskTwice(TaskFormat),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Step {}

    impl Task for Step {
        const TYPE: &'static str = "step";
        const VERSION: i32 = 1;
    }

    #[derive(Serialize)]
    struct Mail {}

    impl Task for Mail {
        const TYPE: &'static str = "mail";
        const VERSION: i32 = 1;
    }

    #[derive(Serialize)]
    struct Unbound {}

    impl Task for Unbound {
        const TYPE: &'static str = "unbound";
        const VERSION: i32 = 1;
    }

    fn seconds(retry: RetryPause, attempts: impl IntoIterator<Item = i32>) -> Vec<u64> {
        attempts.into_iter().map(|attempt| retry.pause(attempt).as_secs()).collect()
    }

    #[test]
    fn a_fixed_retry_pauses_alike_after_every_attempt() {
        let step = RetryPause::fixed(Duration::from_secs(1));
        assert_eq!(seconds(step, [i32::MIN, -1, 0]), [1, 1, 1]);
        assert_eq!(seconds(step, 1..=8), [1; 8]);
        assert_eq!(step.pause(i32::MAX), Duration::from_secs(1));
    }

    #[test]
    fn a_backoff_doubles_from_the_base_and_is_capped() {
        let mail = RetryPause::backoff(Duration::from_secs(1), Duration::from_secs(8));
        assert_eq!(seconds(mail, 1..=8), [1, 2, 4, 8, 8, 8, 8, 8]);
        assert_eq!(
            seconds(mail, [i32::MIN, -1, 0]),
            [1, 1, 1],
            "an attempt below 1 counts as the first"
        );
        assert_eq!(mail.pause(i32::MAX), Duration::from_secs(8));
        let long = RetryPause::backoff(Duration::from_millis(3), Duration::from_secs(3600));
        assert_eq!(long.pause(17), Duration::from_millis(3 * 65_536));
        assert_eq!(
            long.pause(18),
            Duration::from_millis(3 * 65_536),
            "the doubling stops at 2^16"
        );
        let huge = RetryPause::backoff(Duration::MAX, Duration::MAX);
        assert_eq!(huge.pause(5), Duration::MAX, "the product saturates");
        let inverted = RetryPause::backoff(Duration::from_secs(5), Duration::from_secs(2));
        assert_eq!(
            seconds(inverted, 1..=3),
            [2, 2, 2],
            "a base above the maximum pauses the maximum"
        );
    }

    fn attempts(n: u16) -> NonZeroU16 {
        NonZeroU16::new(n).unwrap()
    }

    #[test]
    fn a_policy_keeps_its_retry_and_budget() {
        let retry = RetryPause::fixed(Duration::from_secs(1));
        let policy = TaskPolicy::new(retry, attempts(1));
        assert_eq!((policy.retry(), policy.max_attempts()), (retry, attempts(1)));
    }

    fn step() -> TaskPolicy {
        TaskPolicy::new(RetryPause::fixed(Duration::from_secs(1)), attempts(6))
    }

    fn mail() -> TaskPolicy {
        TaskPolicy::new(
            RetryPause::backoff(Duration::from_secs(1), Duration::from_secs(8)),
            attempts(3),
        )
    }

    fn policies() -> Policies {
        Policies::builder().task::<Step>(step()).task::<Mail>(mail()).build().unwrap()
    }

    fn within_redelivery_bounds(pause: Duration, attempt: i32) -> bool {
        let (low, high) = match attempt {
            1 => (4.0, 6.0),
            3 => (16.0, 24.0),
            _ => (240.0, 300.0),
        };
        (low..=high).contains(&pause.as_secs_f64())
    }

    #[test]
    fn policies_hold_a_policy_per_task_type_and_a_type_nobody_registered_gets_the_default() {
        let shared = policies().clone();
        let (step_format, mail_format) = (TaskFormat::of::<Step>(), TaskFormat::of::<Mail>());
        let unbound = TaskFormat::of::<Unbound>();
        assert_eq!(
            (
                shared.policy_for(&step_format),
                shared.policy_for(&mail_format),
                shared.policy_for(&unbound)
            ),
            (step(), mail(), TaskPolicy::default())
        );
        assert_eq!(
            (
                shared.max_attempts(&step_format),
                shared.max_attempts(&mail_format),
                shared.max_attempts(&unbound)
            ),
            (attempts(6), attempts(3), DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(Policies::default().policy_for(&step_format), TaskPolicy::default());
        assert_eq!(Policies::default().max_attempts(&step_format), DEFAULT_MAX_ATTEMPTS);
    }

    #[test]
    fn a_type_pauses_by_its_policy_and_a_type_without_one_by_the_redelivery_backoff() {
        let policies = policies();
        let (step_format, mail_format) = (TaskFormat::of::<Step>(), TaskFormat::of::<Mail>());
        let pauses = |format: &TaskFormat| (1..=6).map(|n| policies.pause(format, n).as_secs()).collect::<Vec<_>>();
        assert_eq!(pauses(&step_format), [1; 6]);
        assert_eq!(pauses(&mail_format), [1, 2, 4, 8, 8, 8]);
        let unbound = TaskFormat::of::<Unbound>();
        for attempt in [1, 3, 7] {
            let pause = policies.pause(&unbound, attempt);
            assert!(within_redelivery_bounds(pause, attempt), "attempt {attempt}: {pause:?}");
        }
    }

    #[test]
    fn the_redelivery_pause_is_the_queues_jittered_backoff() {
        let redelivery = RetryPause::redelivery();
        for attempt in [1, 3, 7, 8, i32::MAX] {
            for _ in 0..100 {
                assert!(
                    within_redelivery_bounds(redelivery.pause(attempt), attempt),
                    "attempt {attempt}"
                );
            }
        }
        for attempt in [i32::MIN, -1, 0] {
            let pause = redelivery.pause(attempt);
            assert!(
                (Duration::from_secs(4)..=Duration::from_secs(6)).contains(&pause),
                "{pause:?}"
            );
        }
        let draws: Vec<Duration> = (0..50).map(|_| redelivery.pause(3)).collect();
        assert!(draws.iter().any(|draw| *draw != draws[0]), "a new draw on every call");
        assert_eq!(redelivery, RetryPause::redelivery());
        assert_ne!(redelivery, RetryPause::fixed(Duration::from_secs(5)));
        assert_ne!(
            redelivery,
            RetryPause::backoff(Duration::from_secs(5), Duration::from_secs(300))
        );
    }

    #[test]
    fn the_default_policy_is_the_frameworks_defaults() {
        let policy = TaskPolicy::default();
        assert_eq!(policy, TaskPolicy::new(RetryPause::redelivery(), DEFAULT_MAX_ATTEMPTS));
        assert_eq!((policy.retry(), policy.max_attempts().get()), (RetryPause::redelivery(), 10));
    }

    #[test]
    fn a_type_registered_with_the_default_policy_behaves_as_an_unregistered_one() {
        let registered = Policies::builder().task::<Step>(TaskPolicy::default()).build().unwrap();
        let (format, unregistered) = (TaskFormat::of::<Step>(), Policies::default());
        assert_eq!(registered.policy_for(&format), unregistered.policy_for(&format));
        assert_eq!(registered.max_attempts(&format), unregistered.max_attempts(&format));
        for table in [&registered, &unregistered] {
            let pause = table.pause(&format, 3);
            assert!(within_redelivery_bounds(pause, 3), "{pause:?}");
        }
    }

    #[test]
    fn a_task_type_registered_twice_is_refused() {
        let step = step();
        let twice = Policies::builder().task::<Step>(step).task::<Step>(step).build().unwrap_err();
        assert_eq!(twice, PolicyError::TaskTwice(TaskFormat::of::<Step>()));
        assert_eq!(twice.to_string(), "the task type `step v1` is registered twice");
        let other = Policies::builder()
            .task::<Step>(step)
            .task::<Mail>(mail())
            .task::<Mail>(step)
            .build();
        assert_eq!(other.unwrap_err(), PolicyError::TaskTwice(TaskFormat::of::<Mail>()));
        assert!(Policies::builder().task::<Step>(step).build().is_ok());
    }

    #[test]
    fn backoff_is_bounded_for_every_attempt_range() {
        for attempt in [i32::MIN, 0, 1, 2, 6, 7, 10, i32::MAX] {
            for _ in 0..100 {
                let delay = redelivery_backoff(attempt);
                assert!((Duration::from_secs(4)..=Duration::from_secs(300)).contains(&delay));
                if attempt >= 7 {
                    assert!(delay >= Duration::from_secs(240));
                }
            }
        }
    }
}
