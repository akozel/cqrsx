use crate::cqrs::AggregateError;
use std::{error::Error, future::Future, num::NonZeroUsize, time::Duration};
use thiserror::Error;
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum AggregateConflictRetryPolicyError {
    #[error("conflict retry requires at least one attempt")]
    InvalidAttemptLimit,
    #[error("conflict backoff must be ordered and representable")]
    InvalidBackoff,
    #[error("jitter ratio must be finite and between zero and one")]
    InvalidJitter,
}
/// Controls the bounded, in-process retry of one unchanged aggregate command.
///
/// `max_attempts` counts the initial execution. Only optimistic aggregate
/// conflicts are retried; failures with an unknown commit outcome are returned.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AggregateConflictRetryPolicy {
    pub(super) max_attempts: NonZeroUsize,
    pub(super) initial_backoff: Duration,
    pub(super) max_backoff: Duration,
    pub(super) jitter_ratio: f64,
}

impl AggregateConflictRetryPolicy {
    /// Creates an explicit retry policy. `jitter_ratio` is symmetric, so `0.2`
    /// means ±20%.
    pub fn new(
        max_attempts: usize,
        initial_backoff: Duration,
        max_backoff: Duration,
        jitter_ratio: f64,
    ) -> Result<Self, AggregateConflictRetryPolicyError> {
        let max_attempts = NonZeroUsize::new(max_attempts).ok_or(AggregateConflictRetryPolicyError::InvalidAttemptLimit)?;
        if initial_backoff > max_backoff || max_backoff.as_millis() > i64::MAX as u128 {
            return Err(AggregateConflictRetryPolicyError::InvalidBackoff);
        }
        if !jitter_ratio.is_finite() || !(0.0..=1.0).contains(&jitter_ratio) {
            return Err(AggregateConflictRetryPolicyError::InvalidJitter);
        }
        Ok(Self {
            max_attempts,
            initial_backoff,
            max_backoff,
            jitter_ratio,
        })
    }

    /// The bounds of the pause before the `retry`-th retry, counted from 1 (0 is taken as 1).
    pub(super) fn backoff_bounds(self, retry: usize) -> (Duration, Duration) {
        let exponent = u32::try_from(retry.saturating_sub(1)).unwrap_or(u32::MAX);
        let multiplier = 2_u32.checked_pow(exponent).unwrap_or(u32::MAX);
        let backoff = self.initial_backoff.saturating_mul(multiplier).min(self.max_backoff);
        let lower = backoff.mul_f64(1.0 - self.jitter_ratio).min(backoff);
        let upper = backoff.mul_f64(1.0 + self.jitter_ratio).min(self.max_backoff).max(backoff);
        (lower, upper)
    }

    pub(super) fn backoff(self, retry: usize) -> Duration {
        let (lower, upper) = self.backoff_bounds(retry);
        if lower == upper {
            lower
        } else {
            Duration::from_secs_f64(rand::random_range(lower.as_secs_f64()..=upper.as_secs_f64())).clamp(lower, upper)
        }
    }
}

const DEFAULT_MAX_ATTEMPTS: NonZeroUsize = NonZeroUsize::new(3).unwrap();
const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(5);
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_millis(50);
const DEFAULT_JITTER_RATIO: f64 = 0.2;

impl Default for AggregateConflictRetryPolicy {
    /// Three attempts, pauses from 5 ms doubling up to 50 ms, ±20 % jitter.
    fn default() -> Self {
        Self {
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            initial_backoff: DEFAULT_INITIAL_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
            jitter_ratio: DEFAULT_JITTER_RATIO,
        }
    }
}

pub(super) async fn retry_on_aggregate_conflict<T, D, Operation, OperationFuture>(
    policy: AggregateConflictRetryPolicy,
    mut operation: Operation,
) -> Result<T, AggregateError<D>>
where
    D: Error,
    Operation: FnMut() -> OperationFuture,
    OperationFuture: Future<Output = Result<T, AggregateError<D>>>,
{
    let mut attempt = 1;
    loop {
        let result = operation().await;
        if !matches!(result, Err(AggregateError::AggregateConflict)) || attempt >= policy.max_attempts.get() {
            return result;
        }
        tokio::time::sleep(policy.backoff(attempt)).await;
        attempt += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn conflicts_wait_for_the_configured_backoff_and_stop_after_the_last_attempt() {
        let policy = AggregateConflictRetryPolicy::new(4, Duration::from_millis(5), Duration::from_millis(12), 0.0).unwrap();
        let started = tokio::time::Instant::now();
        let mut calls = Vec::new();
        let result = retry_on_aggregate_conflict::<(), std::io::Error, _, _>(policy, || {
            calls.push(started.elapsed());
            std::future::ready(Err(AggregateError::AggregateConflict))
        })
        .await;
        assert!(matches!(result, Err(AggregateError::AggregateConflict)));
        assert_eq!(calls, [0, 5, 15, 27].map(Duration::from_millis));
        assert_eq!(started.elapsed(), Duration::from_millis(27));
    }

    #[tokio::test(start_paused = true)]
    async fn a_reloaded_domain_or_unknown_commit_outcome_stops_conflict_retries_immediately() {
        let policy = AggregateConflictRetryPolicy::new(5, Duration::from_secs(1), Duration::from_secs(1), 0.0).unwrap();
        for terminal in [
            Ok(42),
            Err(AggregateError::Domain(std::io::Error::other("reloaded domain rejection"))),
            Err(AggregateError::DatabaseConnectionError(Box::new(std::io::Error::other(
                "lost reply",
            )))),
            Err(AggregateError::DeserializationError(Box::new(std::io::Error::other(
                "invalid state",
            )))),
            Err(AggregateError::UnexpectedError(Box::new(std::io::Error::other(
                "unknown commit",
            )))),
        ] {
            let started = tokio::time::Instant::now();
            let mut calls = 0;
            let mut terminal = Some(terminal);
            let result = retry_on_aggregate_conflict(policy, || {
                calls += 1;
                std::future::ready(if calls == 1 {
                    Err(AggregateError::AggregateConflict)
                } else {
                    terminal.take().expect("terminal outcome must never be retried")
                })
            })
            .await;
            assert_eq!(calls, 2);
            assert_eq!(started.elapsed(), Duration::from_secs(1));
            assert!(!matches!(result, Err(AggregateError::AggregateConflict)));
        }
    }

    #[test]
    fn default_policy_has_bounded_exponential_jitter() {
        let policy = AggregateConflictRetryPolicy::default();

        assert_eq!(
            policy,
            AggregateConflictRetryPolicy::new(3, Duration::from_millis(5), Duration::from_millis(50), 0.2).unwrap()
        );
        assert_eq!(
            policy.backoff_bounds(0),
            policy.backoff_bounds(1),
            "retry 0 is taken as the first"
        );
        assert_eq!(policy.backoff_bounds(1), (Duration::from_millis(4), Duration::from_millis(6)));
        assert_eq!(
            policy.backoff_bounds(2),
            (Duration::from_millis(8), Duration::from_millis(12))
        );
        assert_eq!(
            policy.backoff_bounds(5),
            (Duration::from_millis(40), Duration::from_millis(50))
        );
        for retry in 1..=5 {
            let bounds = policy.backoff_bounds(retry);
            for _ in 0..100 {
                assert!((bounds.0..=bounds.1).contains(&policy.backoff(retry)));
            }
        }
    }

    #[test]
    fn nonzero_jitter_does_not_collapse_to_a_constant_delay() {
        let policy = AggregateConflictRetryPolicy::new(2, Duration::from_secs(2), Duration::from_secs(8), 0.5).unwrap();
        let (lower, upper) = policy.backoff_bounds(1);
        assert_eq!((lower, upper), (Duration::from_secs(1), Duration::from_secs(3)));
        let samples: std::collections::HashSet<_> = (0..128).map(|_| policy.backoff(1)).collect();
        assert!(samples.iter().all(|delay| (lower..=upper).contains(delay)));
        // A two-second interval has about two billion nanosecond outcomes;
        // 128 samples distinguish jitter from a constant endpoint without
        // relying on elapsed wall-clock time or scheduler timing.
        assert!(samples.len() > 1, "nonzero jitter must vary the retry delay: {samples:?}");
    }

    #[test]
    fn retry_policy_rejects_zero_attempts() {
        assert!(AggregateConflictRetryPolicy::new(0, Duration::ZERO, Duration::ZERO, 0.0).is_err());
        assert!(AggregateConflictRetryPolicy::new(1, Duration::MAX, Duration::MAX, 0.2).is_err());
        assert!(AggregateConflictRetryPolicy::new(1, Duration::ZERO, Duration::ZERO, f64::NAN).is_err());
        assert!(AggregateConflictRetryPolicy::new(1, Duration::from_secs(2), Duration::from_secs(1), 0.2).is_err());
    }

    #[test]
    fn large_representable_backoff_and_tiny_jitter_remain_bounded() {
        let cap = Duration::from_millis(i64::MAX as u64);
        for jitter in [0.0, 1e-20, 0.2, 1.0] {
            let policy = AggregateConflictRetryPolicy::new(2, cap, cap, jitter).unwrap();
            let (lower, upper) = policy.backoff_bounds(usize::MAX);
            assert!(lower <= upper && upper <= cap);
            for _ in 0..100 {
                assert!((lower..=upper).contains(&policy.backoff(usize::MAX)));
            }
        }
    }
}
