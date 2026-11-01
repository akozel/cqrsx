//! Task policies: how long a task waits before its next attempt and how many attempts it gets.
//!
//! A policy has a name the application chooses ([`PolicyName`], a constant); its value ([`TaskPolicy`]: a [`RetryPause`]
//! and an attempt budget) comes from the application's configuration. [`Policies`] holds the values by name and binds
//! every task type to one of them, so a reaction that plans a task never names a policy: the task's type has one. A
//! type bound to no policy keeps the engine's defaults: [`DEFAULT_MAX_ATTEMPTS`] attempts and the queue's own
//! redelivery backoff. The planner of the reactions reads the budget from here, the executor of the task runtime the
//! pause before a retry ([`Policies::pause`]); the registry is shared (`Arc`), so a clone costs nothing.
//!
//! ```
//! use cqrsx::task::{DEFAULT_MAX_ATTEMPTS, Task, format::TaskFormat, policy::{Policies, PolicyName, RetryPause, TaskPolicy}};
//! use serde::Serialize;
//! use std::{num::NonZeroU16, time::Duration};
//!
//! /// E-mails back off from one second to eight.
//! const MAIL: PolicyName = PolicyName::try_new("mail").unwrap();
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
//! let mail = RetryPause::backoff(Duration::from_secs(1), Duration::from_secs(8));
//! let policies = Policies::builder()
//!     .policy(MAIL, TaskPolicy::new(mail, NonZeroU16::new(6).unwrap()))
//!     .bind::<IssueRefund>(MAIL)
//!     .build()?;
//! let refund = TaskFormat::of::<IssueRefund>();
//! assert_eq!(policies.name_of(&refund), Some(MAIL));
//! assert_eq!(policies.max_attempts(&refund).get(), 6);
//! let pauses: Vec<u64> = (1..=5).map(|attempt| mail.pause(attempt).as_secs()).collect();
//! assert_eq!(pauses, [1, 2, 4, 8, 8]);
//! // A type bound to no policy keeps the engine's defaults.
//! assert_eq!(policies.max_attempts(&TaskFormat::of::<Audit>()), DEFAULT_MAX_ATTEMPTS);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! A policy name is never empty: [`PolicyName::try_new`] is `None` for an empty one, so a constant with an empty one
//! does not compile:
//!
//! ```compile_fail
//! use cqrsx::task::policy::PolicyName;
//!
//! const NONE: PolicyName = PolicyName::try_new("").unwrap();
//!
//! fn main() {
//!     println!("{NONE}");
//! }
//! ```
use crate::task::{DEFAULT_MAX_ATTEMPTS, Task, format::TaskFormat};
use std::{
    collections::{HashMap, hash_map::Entry},
    fmt,
    num::NonZeroU16,
    sync::Arc,
    time::Duration,
};

/// The name of a task policy: a constant of the application (`const MAIL: PolicyName = PolicyName::try_new("mail").unwrap();`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PolicyName(&'static str);

impl PolicyName {
    /// A name that is not empty, `None` for an empty one. A constant unwraps it, so an empty name does not compile.
    pub const fn try_new(name: &'static str) -> Option<Self> {
        if name.is_empty() { None } else { Some(Self(name)) }
    }

    pub const fn as_str(&self) -> &'static str {
        self.0
    }
}

impl fmt::Display for PolicyName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// The pause before a task's next attempt, by the number of the attempt that failed: the same every time, or doubling
/// from a base up to a maximum. No jitter: the pause of attempt `n` is always the same.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPause(Pause);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pause {
    Fixed(Duration),
    Backoff { base: Duration, max: Duration },
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

    /// The pause after attempt `attempt` failed (the first attempt is 1).
    #[must_use]
    pub fn pause(&self, attempt: i32) -> Duration {
        match self.0 {
            Pause::Fixed(pause) => pause,
            Pause::Backoff { base, max } => {
                let exponent = u32::try_from(attempt.max(1) - 1).unwrap_or(u32::MAX).min(16);
                base.saturating_mul(2u32.saturating_pow(exponent)).min(max)
            }
        }
    }
}

/// What a policy decides for the tasks of its types: the pause before the next attempt and the attempt budget, the
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

/// The policies of an application by name and the policy of every task type bound to one. Shared (`Arc`): the
/// planner of the reactions reads the budget of a task's type here, and a clone is cheap.
#[derive(Clone, Debug, Default)]
pub struct Policies(Arc<PolicyTable>);

#[derive(Clone, Debug, Default)]
struct PolicyTable {
    policies: HashMap<PolicyName, TaskPolicy>,
    bindings: HashMap<TaskFormat, PolicyName>,
}

impl Policies {
    /// A builder of the policies: the steps are checked when [`build`](PoliciesBuilder::build) replays them.
    pub fn builder() -> PoliciesBuilder {
        PoliciesBuilder::default()
    }

    /// Defines the policy `name`; a name is defined once.
    fn define(&mut self, name: PolicyName, policy: TaskPolicy) -> Result<(), PolicyError> {
        match Arc::make_mut(&mut self.0).policies.entry(name) {
            Entry::Occupied(_) => return Err(PolicyError::DefinedTwice(name)),
            Entry::Vacant(entry) => entry.insert(policy),
        };
        Ok(())
    }

    /// Binds the task type `format` to the policy `name`, which must be defined already; a type is bound once.
    fn bind(&mut self, format: TaskFormat, name: PolicyName) -> Result<(), PolicyError> {
        let table = Arc::make_mut(&mut self.0);
        if !table.policies.contains_key(&name) {
            return Err(PolicyError::Unknown(name));
        }
        match table.bindings.entry(format) {
            Entry::Occupied(entry) => return Err(PolicyError::BoundTwice(entry.key().clone())),
            Entry::Vacant(entry) => entry.insert(name),
        };
        Ok(())
    }

    /// The policy named `name`.
    pub fn policy(&self, name: PolicyName) -> Option<&TaskPolicy> {
        self.0.policies.get(&name)
    }

    /// The name of the policy the task type `format` is bound to.
    pub fn name_of(&self, format: &TaskFormat) -> Option<PolicyName> {
        self.0.bindings.get(format).copied()
    }

    /// The policy of the task type `format`; `None` for a type bound to none.
    pub fn policy_for(&self, format: &TaskFormat) -> Option<&TaskPolicy> {
        self.name_of(format).and_then(|name| self.policy(name))
    }

    /// The attempt budget of the task type `format`: its policy's, or [`DEFAULT_MAX_ATTEMPTS`] without one.
    #[must_use]
    pub fn max_attempts(&self, format: &TaskFormat) -> NonZeroU16 {
        self.policy_for(format).map_or(DEFAULT_MAX_ATTEMPTS, TaskPolicy::max_attempts)
    }

    /// The pause after attempt `attempt` of the task type `format` failed: its policy's, or the queue's jittered
    /// [`redelivery_backoff`] without one (a new draw on every call).
    #[must_use]
    pub fn pause(&self, format: &TaskFormat, attempt: i32) -> Duration {
        self.policy_for(format)
            .map_or_else(|| redelivery_backoff(attempt), |policy| policy.retry().pause(attempt))
    }
}

/// The builder of [`Policies`]: keeps the policies and the bindings in the order of the calls, and
/// [`build`](Self::build) replays them with the checks of each.
#[derive(Clone, Debug, Default)]
pub struct PoliciesBuilder {
    steps: Vec<Step>,
}

#[derive(Clone, Debug)]
enum Step {
    Policy(PolicyName, TaskPolicy),
    Bind(TaskFormat, PolicyName),
}

impl PoliciesBuilder {
    /// Defines the policy `name`; a name is defined once.
    #[must_use]
    pub fn policy(mut self, name: PolicyName, policy: TaskPolicy) -> Self {
        self.steps.push(Step::Policy(name, policy));
        self
    }

    /// Binds the task type `T` to the policy `name`, which must be defined by an earlier call; a type is bound once.
    #[must_use]
    pub fn bind<T: Task>(mut self, name: PolicyName) -> Self {
        self.steps.push(Step::Bind(TaskFormat::of::<T>(), name));
        self
    }

    /// The policies, or the first step that was refused: a name defined twice, a binding to a name no earlier call
    /// defined, a type bound twice.
    pub fn build(self) -> Result<Policies, PolicyError> {
        let mut policies = Policies::default();
        for step in self.steps {
            match step {
                Step::Policy(name, policy) => policies.define(name, policy)?,
                Step::Bind(format, name) => policies.bind(format, name)?,
            }
        }
        Ok(policies)
    }
}

/// Jittered exponential redelivery backoff, also used after lease expiry.
pub fn redelivery_backoff(attempt: i32) -> Duration {
    let exponent = attempt.saturating_sub(1).clamp(0, 6) as u32;
    let base = (5_u64 * 2_u64.pow(exponent)).min(300);
    let jitter = rand::random_range(0.8..=1.2);
    Duration::from_secs_f64((base as f64 * jitter).min(300.0))
}

/// Why a policy or a binding was refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PolicyError {
    #[error("the task policy `{0}` is defined twice")]
    DefinedTwice(PolicyName),
    #[error("no task policy is named `{0}`")]
    Unknown(PolicyName),
    #[error("the task type `{0}` is bound to a policy twice")]
    BoundTwice(TaskFormat),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    const STEP: PolicyName = PolicyName::try_new("step").unwrap();
    const MAIL: PolicyName = PolicyName::try_new("mail").unwrap();

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

    #[test]
    fn policies_bind_task_types_to_named_policies_and_a_type_without_one_keeps_the_engine_default() {
        let step = TaskPolicy::new(RetryPause::fixed(Duration::from_secs(1)), attempts(6));
        let mail = TaskPolicy::new(
            RetryPause::backoff(Duration::from_secs(1), Duration::from_secs(8)),
            attempts(3),
        );
        let policies = Policies::builder()
            .policy(STEP, step)
            .policy(MAIL, mail)
            .bind::<Step>(STEP)
            .bind::<Mail>(MAIL)
            .build()
            .unwrap();
        let shared = policies.clone();
        let (step_format, mail_format) = (TaskFormat::of::<Step>(), TaskFormat::of::<Mail>());
        let unbound = TaskFormat::of::<Unbound>();
        assert_eq!((shared.policy(STEP), shared.policy(MAIL)), (Some(&step), Some(&mail)));
        assert_eq!(
            (
                shared.name_of(&step_format),
                shared.name_of(&mail_format),
                shared.name_of(&unbound)
            ),
            (Some(STEP), Some(MAIL), None)
        );
        assert_eq!(
            (
                shared.policy_for(&step_format),
                shared.policy_for(&mail_format),
                shared.policy_for(&unbound)
            ),
            (Some(&step), Some(&mail), None)
        );
        assert_eq!(
            (
                shared.max_attempts(&step_format),
                shared.max_attempts(&mail_format),
                shared.max_attempts(&unbound)
            ),
            (attempts(6), attempts(3), DEFAULT_MAX_ATTEMPTS)
        );
        assert_eq!(Policies::default().max_attempts(&step_format), DEFAULT_MAX_ATTEMPTS);
    }

    #[test]
    fn a_type_pauses_by_its_policy_and_a_type_without_one_by_the_redelivery_backoff() {
        let step = TaskPolicy::new(RetryPause::fixed(Duration::from_secs(1)), attempts(6));
        let mail = TaskPolicy::new(
            RetryPause::backoff(Duration::from_secs(1), Duration::from_secs(8)),
            attempts(6),
        );
        let policies = Policies::builder()
            .policy(STEP, step)
            .policy(MAIL, mail)
            .bind::<Step>(STEP)
            .bind::<Mail>(MAIL)
            .build()
            .unwrap();
        let (step_format, mail_format) = (TaskFormat::of::<Step>(), TaskFormat::of::<Mail>());
        let pauses = |format: &TaskFormat| (1..=6).map(|n| policies.pause(format, n).as_secs()).collect::<Vec<_>>();
        assert_eq!(pauses(&step_format), [1; 6]);
        assert_eq!(pauses(&mail_format), [1, 2, 4, 8, 8, 8]);
        let unbound = TaskFormat::of::<Unbound>();
        for (attempt, low, high) in [(1, 4.0, 6.0), (3, 16.0, 24.0), (7, 240.0, 300.0)] {
            let pause = policies.pause(&unbound, attempt).as_secs_f64();
            assert!((low..=high).contains(&pause), "attempt {attempt}: {pause}");
        }
    }

    #[test]
    fn policies_refuse_a_name_twice_an_unknown_name_and_a_type_bound_twice() {
        let policy = TaskPolicy::new(RetryPause::fixed(Duration::from_secs(1)), attempts(6));
        let defined = || Policies::builder().policy(STEP, policy);
        let twice = defined().policy(STEP, policy).build().unwrap_err();
        assert_eq!(twice, PolicyError::DefinedTwice(STEP));
        assert_eq!(twice.to_string(), "the task policy `step` is defined twice");
        let unknown = defined().bind::<Mail>(MAIL).build().unwrap_err();
        assert_eq!(unknown, PolicyError::Unknown(MAIL));
        assert_eq!(unknown.to_string(), "no task policy is named `mail`");
        let before = Policies::builder()
            .bind::<Step>(STEP)
            .policy(STEP, policy)
            .build()
            .unwrap_err();
        assert_eq!(
            before,
            PolicyError::Unknown(STEP),
            "a binding needs a policy defined by an earlier call"
        );
        assert!(defined().bind::<Step>(STEP).build().is_ok());
        let again = defined().bind::<Step>(STEP).bind::<Step>(STEP).build().unwrap_err();
        assert_eq!(again, PolicyError::BoundTwice(TaskFormat::of::<Step>()));
        assert_eq!(again.to_string(), "the task type `step v1` is bound to a policy twice");
    }

    #[test]
    fn a_policy_name_is_its_text() {
        assert_eq!((MAIL.as_str(), MAIL.to_string()), ("mail", "mail".to_owned()));
        assert_eq!(
            PolicyName::try_new(std::hint::black_box("mail")),
            Some(MAIL),
            "built at run time too"
        );
        assert_ne!(MAIL, STEP);
    }

    #[test]
    fn an_empty_policy_name_is_refused() {
        assert_eq!(PolicyName::try_new(std::hint::black_box("")), None);
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
