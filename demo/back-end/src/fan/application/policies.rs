//! The policies of the ledger's task types: what a task waits before its next attempt and how many attempts it gets.
//! A reaction never names a policy and nor does a handler: the module root gives each task type its policy
//! ([`fan::module`](crate::fan::module)) and the task runtime pauses a retry by it.
use cqrsx::task::policy::{RetryPause, TaskPolicy};
use cqrsx_engine::setup::{Setup, SetupError};
use std::{num::NonZeroU16, time::Duration};

/// The pause of the step policy before every next attempt.
pub const CHAIN_RETRY_AFTER: Duration = Duration::from_secs(1);

/// The attempt budget of both policies without `ENCORE_TASK_MAX_ATTEMPTS`.
const DEFAULT_TASK_MAX_ATTEMPTS: NonZeroU16 = NonZeroU16::new(6).unwrap();

/// The policies of the ledger: a step of the chain and an e-mail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policies {
    /// A step of the chain between the aggregates (`encore_record_ticket`, `encore_record_cancellation`): one second
    /// ([`CHAIN_RETRY_AFTER`]) between attempts.
    pub step: TaskPolicy,
    /// An e-mail (`encore_send_e_ticket`, `encore_issue_refund`): backs off from `ENCORE_TASK_RETRY_BASE_MS`, doubling
    /// up to `ENCORE_TASK_RETRY_MAX_MS`.
    pub mail: TaskPolicy,
}

impl Policies {
    /// Both policies from the variables `ENCORE_TASK_MAX_ATTEMPTS` (positive, 6 unless set), `ENCORE_TASK_RETRY_BASE_MS`
    /// (1000) and `ENCORE_TASK_RETRY_MAX_MS` (8000); the base must not exceed the maximum.
    pub fn from_setup(setup: &Setup) -> Result<Self, SetupError> {
        let attempts: NonZeroU16 = setup.env("TASK_MAX_ATTEMPTS", DEFAULT_TASK_MAX_ATTEMPTS)?;
        let (base, max) = (
            setup.millis("TASK_RETRY_BASE_MS", 1000)?,
            setup.millis("TASK_RETRY_MAX_MS", 8000)?,
        );
        if base > max {
            let message = format!("must not exceed {}", setup.variable("TASK_RETRY_MAX_MS"));
            return Err(setup.invalid("TASK_RETRY_BASE_MS", message));
        }
        Ok(Self {
            step: TaskPolicy::new(RetryPause::fixed(CHAIN_RETRY_AFTER), attempts),
            mail: TaskPolicy::new(RetryPause::backoff(base, max), attempts),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The policies under the variables `pairs`, with the settings of the demo's engine.
    fn from_pairs(pairs: &'static [(&'static str, &'static str)]) -> Result<Policies, SetupError> {
        let lookup = move |name: &str| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        };
        Policies::from_setup(&Setup::new("ENCORE", lookup, crate::testing::settings()))
    }

    fn attempts(count: u16) -> NonZeroU16 {
        NonZeroU16::new(count).unwrap()
    }

    #[test]
    fn the_policies_default_to_a_second_step_and_a_backing_off_mail_with_six_attempts() {
        assert_eq!(
            from_pairs(&[]).unwrap(),
            Policies {
                step: TaskPolicy::new(RetryPause::fixed(Duration::from_secs(1)), attempts(6)),
                mail: TaskPolicy::new(
                    RetryPause::backoff(Duration::from_secs(1), Duration::from_secs(8)),
                    attempts(6)
                ),
            }
        );
    }

    #[test]
    fn the_policies_come_from_their_variables() {
        let policies = from_pairs(&[
            ("ENCORE_TASK_MAX_ATTEMPTS", "3"),
            ("ENCORE_TASK_RETRY_BASE_MS", "300"),
            ("ENCORE_TASK_RETRY_MAX_MS", "5000"),
        ])
        .unwrap();
        assert_eq!(
            policies,
            Policies {
                step: TaskPolicy::new(RetryPause::fixed(Duration::from_secs(1)), attempts(3)),
                mail: TaskPolicy::new(
                    RetryPause::backoff(Duration::from_millis(300), Duration::from_secs(5)),
                    attempts(3)
                ),
            }
        );
    }

    #[test]
    fn a_zero_attempt_budget_is_refused() {
        let refused = from_pairs(&[("ENCORE_TASK_MAX_ATTEMPTS", "0")]).unwrap_err();
        assert_eq!(
            refused.to_string(),
            r#"ENCORE_TASK_MAX_ATTEMPTS: cannot parse "0": number would be zero for non-zero type"#,
            "an attempt budget is positive"
        );
    }

    #[test]
    fn a_base_above_the_maximum_is_refused_and_a_base_equal_to_it_is_not() {
        let refused = from_pairs(&[("ENCORE_TASK_RETRY_BASE_MS", "5000"), ("ENCORE_TASK_RETRY_MAX_MS", "2000")]);
        assert_eq!(
            refused.unwrap_err().to_string(),
            "ENCORE_TASK_RETRY_BASE_MS: must not exceed ENCORE_TASK_RETRY_MAX_MS"
        );
        let equal = from_pairs(&[("ENCORE_TASK_RETRY_BASE_MS", "2000"), ("ENCORE_TASK_RETRY_MAX_MS", "2000")]).unwrap();
        assert_eq!(
            equal.mail.retry(),
            RetryPause::backoff(Duration::from_secs(2), Duration::from_secs(2))
        );
    }
}
