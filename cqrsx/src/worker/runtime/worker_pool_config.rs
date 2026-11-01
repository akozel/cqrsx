//! The limits of the process's task workers: how many run, how many claimed tasks wait, how long a lease lasts.
use crate::worker::queue::lease::LEASE_SAFETY_MARGIN;
use std::time::Duration;
use thiserror::Error;

// Preserve the existing configuration range independently of Tokio internals.
const MAX_BUFFER_CAPACITY: usize = usize::MAX >> 3;
const DEFAULT_CONCURRENCY: u64 = 128;
const DEFAULT_BUFFER_CAPACITY: u64 = 128;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(1_000);
const DEFAULT_HANDLER_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_LEASE: Duration = Duration::from_secs(60);
// A claim once fewer than a quarter of the buffer waits: chosen by measurement (ticket 04/19).
const DEFAULT_BUFFER_REFILL_PERCENT: u64 = 25;

/// Independent process-wide execution and waiting limits, with validated local values.
/// Defaults: C=128, Q=128, poll=1s, handler=15s, lease=60s, refill=25%.
/// Validate the combined queued-wave lease against the actual executor.
#[derive(Clone, Debug)]
pub struct TaskWorkerPoolConfig {
    concurrency: usize,
    buffer_capacity: usize,
    poll_interval: Duration,
    handler_timeout: Duration,
    lease: Duration,
    buffer_refill_percent: u8,
}
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum TaskWorkerPoolConfigError {
    #[error("invalid integer setting: {0}")]
    NotAnInteger(&'static str),
    #[error("worker count must be positive")]
    ZeroConcurrency,
    #[error("buffer capacity must be positive")]
    ZeroBufferCapacity,
    #[error("buffer capacity must not exceed {max}, the bound of the channel")]
    BufferCapacityTooLarge { max: usize },
    #[error("buffer refill percent must be at least 1")]
    ZeroBufferRefillPercent,
    #[error("buffer refill percent must be at most 100")]
    BufferRefillPercentTooLarge,
    #[error("poll interval must be positive")]
    ZeroPollInterval,
    #[error("poll interval is too long to represent")]
    PollIntervalTooLong,
    #[error("handler timeout must be positive")]
    ZeroHandlerTimeout,
    #[error("handler timeout must be shorter than the lease")]
    HandlerTimeoutTooLong,
    #[error("lease duration must be positive")]
    ZeroLease,
    #[error("lease duration is too long to represent")]
    LeaseTooLong,
    #[error("storage timeout must be positive")]
    ZeroStorageTimeout,
    #[error("lease budget overflows")]
    LeaseBudgetOverflow,
    #[error("lease must exceed claim + queued waves + handler + settlement + 5-second safety margin")]
    LeaseTooShort,
}
impl Default for TaskWorkerPoolConfig {
    /// The defaults above, valid as [`build`](TaskWorkerPoolConfigBuilder::build) checks them (a test does).
    fn default() -> Self {
        Self {
            concurrency: DEFAULT_CONCURRENCY as usize,
            buffer_capacity: DEFAULT_BUFFER_CAPACITY as usize,
            poll_interval: DEFAULT_POLL_INTERVAL,
            handler_timeout: DEFAULT_HANDLER_TIMEOUT,
            lease: DEFAULT_LEASE,
            buffer_refill_percent: DEFAULT_BUFFER_REFILL_PERCENT as u8,
        }
    }
}

/// The builder of [`TaskWorkerPoolConfig`]: every limit starts at its default, [`build`](Self::build) checks them
/// together. The full lease is validated separately with [`TaskWorkerPoolConfig::validate_for_storage_timeout`].
#[derive(Clone, Debug, Default)]
pub struct TaskWorkerPoolConfigBuilder {
    config: TaskWorkerPoolConfig,
}

impl TaskWorkerPoolConfigBuilder {
    /// Permanent Tokio workers; positive.
    #[must_use]
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.config.concurrency = concurrency;
        self
    }

    /// Claimed deliveries waiting locally; positive, at most what the bounded channel supports.
    #[must_use]
    pub fn buffer_capacity(mut self, buffer_capacity: usize) -> Self {
        self.config.buffer_capacity = buffer_capacity;
        self
    }

    /// The pause between claims of an idle producer; positive and representable in milliseconds.
    #[must_use]
    pub fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.config.poll_interval = poll_interval;
        self
    }

    /// The time a handler gets; positive and shorter than the lease.
    #[must_use]
    pub fn handler_timeout(mut self, handler_timeout: Duration) -> Self {
        self.config.handler_timeout = handler_timeout;
        self
    }

    /// How long a claim lasts; positive and representable in milliseconds.
    #[must_use]
    pub fn lease(mut self, lease: Duration) -> Self {
        self.config.lease = lease;
        self
    }

    /// The producer claims only once fewer deliveries than this share of the buffer wait, and then
    /// fills the buffer: one claim per refill instead of one per freed slot. 100 claims for every
    /// freed slot. Between 1 and 100.
    #[must_use]
    pub fn buffer_refill_percent(mut self, percent: u8) -> Self {
        self.config.buffer_refill_percent = percent;
        self
    }

    /// The limits, or the first one that is refused.
    pub fn build(self) -> Result<TaskWorkerPoolConfig, TaskWorkerPoolConfigError> {
        let config = self.config;
        if config.concurrency == 0 {
            return Err(TaskWorkerPoolConfigError::ZeroConcurrency);
        }
        if config.buffer_capacity == 0 {
            return Err(TaskWorkerPoolConfigError::ZeroBufferCapacity);
        }
        if config.buffer_capacity > MAX_BUFFER_CAPACITY {
            return Err(TaskWorkerPoolConfigError::BufferCapacityTooLarge {
                max: MAX_BUFFER_CAPACITY,
            });
        }
        if config.poll_interval.is_zero() {
            return Err(TaskWorkerPoolConfigError::ZeroPollInterval);
        }
        if config.poll_interval.as_millis() > i64::MAX as u128 {
            return Err(TaskWorkerPoolConfigError::PollIntervalTooLong);
        }
        if config.lease.is_zero() {
            return Err(TaskWorkerPoolConfigError::ZeroLease);
        }
        if config.lease.as_millis() > i64::MAX as u128 {
            return Err(TaskWorkerPoolConfigError::LeaseTooLong);
        }
        if config.handler_timeout.is_zero() {
            return Err(TaskWorkerPoolConfigError::ZeroHandlerTimeout);
        }
        if config.handler_timeout >= config.lease {
            return Err(TaskWorkerPoolConfigError::HandlerTimeoutTooLong);
        }
        if config.buffer_refill_percent == 0 {
            return Err(TaskWorkerPoolConfigError::ZeroBufferRefillPercent);
        }
        if config.buffer_refill_percent > 100 {
            return Err(TaskWorkerPoolConfigError::BufferRefillPercentTooLarge);
        }
        Ok(config)
    }
}

impl TaskWorkerPoolConfig {
    /// A builder of explicit limits; every limit it does not set keeps its default.
    pub fn builder() -> TaskWorkerPoolConfigBuilder {
        TaskWorkerPoolConfigBuilder::default()
    }
    /// Permanent Tokio workers; each executes one delivery including settlement.
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }
    /// Claimed deliveries waiting locally; independent of running worker count.
    pub fn buffer_capacity(&self) -> usize {
        self.buffer_capacity
    }
    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }
    pub fn handler_timeout(&self) -> Duration {
        self.handler_timeout
    }
    pub fn lease(&self) -> Duration {
        self.lease
    }
    pub fn buffer_refill_percent(&self) -> u8 {
        self.buffer_refill_percent
    }
    /// Waiting deliveries below which the producer claims: ceil(Q * percent / 100), from 1 to Q.
    pub(super) fn buffer_refill_level(&self) -> usize {
        let level = (self.buffer_capacity as u128 * u128::from(self.buffer_refill_percent)).div_ceil(100);
        // At most the capacity, as the percent is at most 100.
        usize::try_from(level).map_or(self.buffer_capacity, |level| level.min(self.buffer_capacity))
    }
    /// [`from_lookup`](Self::from_lookup) over the process environment.
    pub fn from_env() -> Result<Self, TaskWorkerPoolConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }
    /// The limits from the variables, each an unsigned integer with its default: `OUTBOX_WORKER_CONCURRENCY` (128),
    /// `OUTBOX_QUEUE_CAPACITY` (128), `OUTBOX_POLL_INTERVAL_MS` (1 000), `OUTBOX_HANDLER_TIMEOUT_SECS` (15),
    /// `OUTBOX_LEASE_SECS` (60), `OUTBOX_QUEUE_REFILL_PERCENT` (25). A missing variable takes its default, one that is
    /// not an unsigned integer is `NotAnInteger` with its name; the values are then checked as
    /// [`build`](TaskWorkerPoolConfigBuilder::build) checks them.
    /// The lease against the storage timeout is checked by [`TaskRuntimeConfig`](super::config::TaskRuntimeConfig).
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, TaskWorkerPoolConfigError> {
        let mut read_u64 = |name, default| {
            crate::env::read_u64(&mut lookup, name, default).map_err(|_| TaskWorkerPoolConfigError::NotAnInteger(name))
        };
        let concurrency = usize::try_from(read_u64("OUTBOX_WORKER_CONCURRENCY", DEFAULT_CONCURRENCY)?)
            .map_err(|_| TaskWorkerPoolConfigError::NotAnInteger("OUTBOX_WORKER_CONCURRENCY"))?;
        let buffer_capacity = usize::try_from(read_u64("OUTBOX_QUEUE_CAPACITY", DEFAULT_BUFFER_CAPACITY)?).map_err(|_| {
            TaskWorkerPoolConfigError::BufferCapacityTooLarge {
                max: MAX_BUFFER_CAPACITY,
            }
        })?;
        let poll_interval = Duration::from_millis(read_u64("OUTBOX_POLL_INTERVAL_MS", DEFAULT_POLL_INTERVAL.as_millis() as u64)?);
        let handler_timeout = Duration::from_secs(read_u64("OUTBOX_HANDLER_TIMEOUT_SECS", DEFAULT_HANDLER_TIMEOUT.as_secs())?);
        let lease = Duration::from_secs(read_u64("OUTBOX_LEASE_SECS", DEFAULT_LEASE.as_secs())?);
        let refill = u8::try_from(read_u64("OUTBOX_QUEUE_REFILL_PERCENT", DEFAULT_BUFFER_REFILL_PERCENT)?)
            .map_err(|_| TaskWorkerPoolConfigError::BufferRefillPercentTooLarge)?;
        Self::builder()
            .concurrency(concurrency)
            .buffer_capacity(buffer_capacity)
            .poll_interval(poll_interval)
            .handler_timeout(handler_timeout)
            .lease(lease)
            .buffer_refill_percent(refill)
            .build()
    }
    /// Conservative healthy-pool budget: lease > 2*S + H + margin + ceil(Q/C)*(H+S).
    /// Queue time consumes the original lease; per-delivery deadline checks remain mandatory.
    pub fn validate_for_storage_timeout(&self, storage_timeout: Duration) -> Result<(), TaskWorkerPoolConfigError> {
        if storage_timeout.is_zero() {
            return Err(TaskWorkerPoolConfigError::ZeroStorageTimeout);
        }
        let waves = self.buffer_capacity.div_ceil(self.concurrency);
        let invocation = self
            .handler_timeout
            .checked_add(storage_timeout)
            .ok_or(TaskWorkerPoolConfigError::LeaseBudgetOverflow)?;
        let queued_nanos = invocation
            .as_nanos()
            .checked_mul(waves as u128)
            .ok_or(TaskWorkerPoolConfigError::LeaseBudgetOverflow)?;
        let queued_seconds =
            u64::try_from(queued_nanos / 1_000_000_000).map_err(|_| TaskWorkerPoolConfigError::LeaseBudgetOverflow)?;
        let queued = Duration::new(queued_seconds, (queued_nanos % 1_000_000_000) as u32);
        let required = storage_timeout
            .checked_mul(2)
            .and_then(|budget| budget.checked_add(self.handler_timeout))
            .and_then(|budget| budget.checked_add(LEASE_SAFETY_MARGIN))
            .and_then(|budget| budget.checked_add(queued))
            .ok_or(TaskWorkerPoolConfigError::LeaseBudgetOverflow)?;
        if self.lease <= required {
            return Err(TaskWorkerPoolConfigError::LeaseTooShort);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The limits of the five settings the old constructor took, the refill percent at its default.
    fn pool(
        concurrency: usize,
        buffer_capacity: usize,
        poll_interval: Duration,
        handler_timeout: Duration,
        lease: Duration,
    ) -> Result<TaskWorkerPoolConfig, TaskWorkerPoolConfigError> {
        TaskWorkerPoolConfig::builder()
            .concurrency(concurrency)
            .buffer_capacity(buffer_capacity)
            .poll_interval(poll_interval)
            .handler_timeout(handler_timeout)
            .lease(lease)
            .build()
    }

    #[test]
    fn from_env_validates_independent_worker_limits_in_an_isolated_process() {
        use crate::env::test_support;
        if let Some(scenario) = test_support::scenario() {
            match scenario.as_str() {
                "defaults" => {
                    let config = TaskWorkerPoolConfig::from_env().unwrap();
                    assert_eq!(config.concurrency(), 128);
                    assert_eq!(config.buffer_capacity(), 128);
                }
                "override" => {
                    let config = TaskWorkerPoolConfig::from_env().unwrap();
                    assert_eq!(config.concurrency(), 2);
                    assert_eq!(config.buffer_capacity(), 128);
                }
                "invalid" => assert_eq!(
                    TaskWorkerPoolConfig::from_env().unwrap_err(),
                    TaskWorkerPoolConfigError::NotAnInteger("OUTBOX_WORKER_CONCURRENCY")
                ),
                "zero" => assert_eq!(
                    TaskWorkerPoolConfig::from_env().unwrap_err(),
                    TaskWorkerPoolConfigError::ZeroConcurrency
                ),
                _ => panic!("unknown test scenario"),
            }
            return;
        }
        for (scenario, value) in [
            ("defaults", None),
            ("override", Some("2")),
            ("invalid", Some("invalid")),
            ("zero", Some("0")),
        ] {
            test_support::isolated(
                concat!(
                    module_path!(),
                    "::from_env_validates_independent_worker_limits_in_an_isolated_process"
                ),
                scenario,
                |child| {
                    if let Some(value) = value {
                        child.env("OUTBOX_WORKER_CONCURRENCY", value);
                    }
                },
            );
        }
    }

    #[test]
    fn defaults_and_independent_environment_overrides() {
        let default = TaskWorkerPoolConfig::from_lookup(|_| None).unwrap();
        let built = TaskWorkerPoolConfig::default();
        assert_eq!(
            format!("{built:?}"),
            format!("{default:?}"),
            "Default builds what the validated defaults give"
        );
        assert_eq!(default.concurrency(), 128);
        assert_eq!(default.buffer_capacity(), 128);
        assert_eq!(default.lease(), Duration::from_secs(60));
        assert_eq!(default.handler_timeout(), Duration::from_secs(15));
        assert_eq!(default.buffer_refill_percent(), 25);
        default.validate_for_storage_timeout(Duration::from_secs(6)).unwrap();
        let custom = TaskWorkerPoolConfig::from_lookup(|name| {
            Some(
                match name {
                    "OUTBOX_WORKER_CONCURRENCY" => "2",
                    "OUTBOX_QUEUE_CAPACITY" => "3",
                    "OUTBOX_POLL_INTERVAL_MS" => "50",
                    "OUTBOX_HANDLER_TIMEOUT_SECS" => "10",
                    "OUTBOX_LEASE_SECS" => "120",
                    "OUTBOX_QUEUE_REFILL_PERCENT" => "100",
                    _ => panic!("unexpected variable {name}"),
                }
                .into(),
            )
        })
        .unwrap();
        assert_eq!(custom.concurrency(), 2);
        assert_eq!(custom.buffer_capacity(), 3);
        assert_eq!(custom.poll_interval(), Duration::from_millis(50));
        assert_eq!(custom.handler_timeout(), Duration::from_secs(10));
        assert_eq!(custom.buffer_refill_percent(), 100);
        let only_c =
            TaskWorkerPoolConfig::from_lookup(|name| (name == "OUTBOX_WORKER_CONCURRENCY").then(|| "1000".into())).unwrap();
        assert_eq!(only_c.buffer_capacity(), 128);
        let only_q = TaskWorkerPoolConfig::from_lookup(|name| (name == "OUTBOX_QUEUE_CAPACITY").then(|| "256".into())).unwrap();
        assert_eq!(only_q.concurrency(), 128);
        let only_refill =
            TaskWorkerPoolConfig::from_lookup(|name| (name == "OUTBOX_QUEUE_REFILL_PERCENT").then(|| "1".into())).unwrap();
        assert_eq!(only_refill.buffer_refill_percent(), 1);
        assert_eq!(only_refill.buffer_capacity(), 128);
    }

    #[test]
    fn invalid_environment_and_structural_values_are_typed() {
        for (key, value) in [
            ("OUTBOX_WORKER_CONCURRENCY", "garbage"),
            ("OUTBOX_WORKER_CONCURRENCY", "0"),
            ("OUTBOX_QUEUE_CAPACITY", "0"),
            ("OUTBOX_QUEUE_CAPACITY", "18446744073709551615"),
            ("OUTBOX_POLL_INTERVAL_MS", "0"),
            ("OUTBOX_HANDLER_TIMEOUT_SECS", "0"),
            ("OUTBOX_HANDLER_TIMEOUT_SECS", "120"),
            ("OUTBOX_LEASE_SECS", "0"),
            ("OUTBOX_LEASE_SECS", "18446744073709551615"),
            ("OUTBOX_POLL_INTERVAL_MS", "18446744073709551615"),
            ("OUTBOX_QUEUE_REFILL_PERCENT", "garbage"),
            ("OUTBOX_QUEUE_REFILL_PERCENT", "0"),
            ("OUTBOX_QUEUE_REFILL_PERCENT", "101"),
            ("OUTBOX_QUEUE_REFILL_PERCENT", "256"),
        ] {
            assert!(
                TaskWorkerPoolConfig::from_lookup(|name| (name == key).then(|| value.into())).is_err(),
                "{key}={value}"
            );
        }
        let refill = |value: &'static str| {
            TaskWorkerPoolConfig::from_lookup(|name| (name == "OUTBOX_QUEUE_REFILL_PERCENT").then(|| value.into())).unwrap_err()
        };
        assert_eq!(
            refill("garbage"),
            TaskWorkerPoolConfigError::NotAnInteger("OUTBOX_QUEUE_REFILL_PERCENT")
        );
        assert_eq!(refill("0"), TaskWorkerPoolConfigError::ZeroBufferRefillPercent);
        for value in ["101", "256"] {
            assert_eq!(
                refill(value),
                TaskWorkerPoolConfigError::BufferRefillPercentTooLarge,
                "{value}"
            );
        }
        for (percent, error) in [
            (0, TaskWorkerPoolConfigError::ZeroBufferRefillPercent),
            (101, TaskWorkerPoolConfigError::BufferRefillPercentTooLarge),
        ] {
            assert_eq!(
                TaskWorkerPoolConfig::builder()
                    .buffer_refill_percent(percent)
                    .build()
                    .unwrap_err(),
                error
            );
        }
        let (second, minute, two_minutes) = (Duration::from_secs(1), Duration::from_secs(30), Duration::from_secs(120));
        let refused = |c, q, poll, handler, lease| pool(c, q, poll, handler, lease).unwrap_err();
        assert_eq!(
            refused(0, 1, second, minute, two_minutes),
            TaskWorkerPoolConfigError::ZeroConcurrency
        );
        assert_eq!(
            refused(1, 0, second, minute, two_minutes),
            TaskWorkerPoolConfigError::ZeroBufferCapacity
        );
        assert_eq!(
            refused(1, MAX_BUFFER_CAPACITY + 1, second, minute, two_minutes),
            TaskWorkerPoolConfigError::BufferCapacityTooLarge {
                max: MAX_BUFFER_CAPACITY
            }
        );
        assert_eq!(
            refused(1, 1, Duration::ZERO, minute, two_minutes),
            TaskWorkerPoolConfigError::ZeroPollInterval
        );
        assert_eq!(
            refused(1, 1, Duration::MAX, minute, two_minutes),
            TaskWorkerPoolConfigError::PollIntervalTooLong
        );
        assert_eq!(
            refused(1, 1, second, minute, Duration::ZERO),
            TaskWorkerPoolConfigError::ZeroLease
        );
        assert_eq!(
            refused(1, 1, second, minute, Duration::MAX),
            TaskWorkerPoolConfigError::LeaseTooLong
        );
        assert_eq!(
            refused(1, 1, second, Duration::ZERO, two_minutes),
            TaskWorkerPoolConfigError::ZeroHandlerTimeout
        );
        assert_eq!(
            refused(1, 1, second, two_minutes, two_minutes),
            TaskWorkerPoolConfigError::HandlerTimeoutTooLong
        );
    }

    #[test]
    fn each_refusal_names_its_defect() {
        for (error, text) in [
            (
                TaskWorkerPoolConfigError::ZeroBufferCapacity,
                "buffer capacity must be positive",
            ),
            (
                TaskWorkerPoolConfigError::BufferCapacityTooLarge { max: 7 },
                "buffer capacity must not exceed 7, the bound of the channel",
            ),
            (
                TaskWorkerPoolConfigError::ZeroBufferRefillPercent,
                "buffer refill percent must be at least 1",
            ),
            (
                TaskWorkerPoolConfigError::BufferRefillPercentTooLarge,
                "buffer refill percent must be at most 100",
            ),
            (TaskWorkerPoolConfigError::ZeroPollInterval, "poll interval must be positive"),
            (
                TaskWorkerPoolConfigError::PollIntervalTooLong,
                "poll interval is too long to represent",
            ),
            (
                TaskWorkerPoolConfigError::ZeroHandlerTimeout,
                "handler timeout must be positive",
            ),
            (
                TaskWorkerPoolConfigError::HandlerTimeoutTooLong,
                "handler timeout must be shorter than the lease",
            ),
            (TaskWorkerPoolConfigError::ZeroLease, "lease duration must be positive"),
            (
                TaskWorkerPoolConfigError::LeaseTooLong,
                "lease duration is too long to represent",
            ),
        ] {
            assert_eq!(error.to_string(), text);
        }
    }

    #[test]
    fn buffer_refill_level_rounds_up_and_stays_within_capacity() {
        let level = |q, percent| {
            TaskWorkerPoolConfig::builder()
                .concurrency(1)
                .buffer_capacity(q)
                .buffer_refill_percent(percent)
                .build()
                .unwrap()
                .buffer_refill_level()
        };
        for (q, percent, expected) in [
            (1, 50, 1),
            (2, 50, 1),
            (3, 50, 2),
            (4, 50, 2),
            (8, 50, 4),
            (32, 50, 16),
            (7, 1, 1),
            (7, 100, 7),
            (MAX_BUFFER_CAPACITY, 100, MAX_BUFFER_CAPACITY),
            (MAX_BUFFER_CAPACITY, 1, MAX_BUFFER_CAPACITY.div_ceil(100)),
        ] {
            assert_eq!(level(q, percent), expected, "Q={q} percent={percent}");
        }
    }

    #[test]
    fn buffer_waves_budget_boundaries_and_storage_overflow() {
        let build = |c, q, lease| {
            pool(
                c,
                q,
                Duration::from_secs(1),
                Duration::from_secs(30),
                Duration::from_secs(lease),
            )
            .unwrap()
        };
        for (q, required) in [(128, 80), (256, 115), (257, 150)] {
            assert_eq!(
                build(128, q, required).validate_for_storage_timeout(Duration::from_secs(5)),
                Err(TaskWorkerPoolConfigError::LeaseTooShort)
            );
            assert!(
                build(128, q, required + 1)
                    .validate_for_storage_timeout(Duration::from_secs(5))
                    .is_ok()
            );
        }
        assert!(build(2, 3, 120).validate_for_storage_timeout(Duration::from_secs(5)).is_ok());
        assert!(
            build(1000, 1000, 120)
                .validate_for_storage_timeout(Duration::from_secs(5))
                .is_ok()
        );
        assert!(
            build(128, 256, 120)
                .validate_for_storage_timeout(Duration::from_secs(10))
                .is_err()
        );
        assert_eq!(
            build(1, 1, 120).validate_for_storage_timeout(Duration::ZERO),
            Err(TaskWorkerPoolConfigError::ZeroStorageTimeout)
        );
        assert_eq!(
            build(1, 1, 120).validate_for_storage_timeout(Duration::MAX),
            Err(TaskWorkerPoolConfigError::LeaseBudgetOverflow)
        );
        assert_eq!(
            build(1, MAX_BUFFER_CAPACITY, 120).validate_for_storage_timeout(Duration::from_secs(5)),
            Err(TaskWorkerPoolConfigError::LeaseBudgetOverflow)
        );
        // Individually representable durations can overflow multiplication by
        // buffer waves, or overflow the final sum even with one queued wave.
        assert_eq!(
            build(1, MAX_BUFFER_CAPACITY, 120).validate_for_storage_timeout(Duration::from_millis(i64::MAX as u64)),
            Err(TaskWorkerPoolConfigError::LeaseBudgetOverflow)
        );
        assert_eq!(
            build(1, 1, 120).validate_for_storage_timeout(Duration::from_secs(u64::MAX / 2)),
            Err(TaskWorkerPoolConfigError::LeaseBudgetOverflow)
        );
    }
    #[test]
    fn default_lease_budget_has_the_expected_boundary_and_rejects_an_extra_buffer_wave() {
        let build = |queue, lease| {
            pool(
                128,
                queue,
                Duration::from_secs(1),
                Duration::from_secs(15),
                Duration::from_secs(lease),
            )
            .unwrap()
        };
        assert_eq!(
            build(128, 53).validate_for_storage_timeout(Duration::from_secs(6)),
            Err(TaskWorkerPoolConfigError::LeaseTooShort)
        );
        build(128, 54).validate_for_storage_timeout(Duration::from_secs(6)).unwrap();
        build(128, 60).validate_for_storage_timeout(Duration::from_secs(6)).unwrap();
        assert_eq!(
            build(256, 60).validate_for_storage_timeout(Duration::from_secs(6)),
            Err(TaskWorkerPoolConfigError::LeaseTooShort)
        );
        build(256, 75).validate_for_storage_timeout(Duration::from_secs(6)).unwrap();
    }
}
