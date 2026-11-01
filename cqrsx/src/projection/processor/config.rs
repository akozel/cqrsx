//! The checked configuration of a projection processor and its defaults.
use std::time::Duration;
use thiserror::Error;

/// How long a processor waits for the next page when no commit woke it, and between claims of a held row.
pub const DEFAULT_PROCESSOR_POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Events per journal page.
pub const DEFAULT_PROCESSOR_PAGE_SIZE: usize = 1000;
/// Keys per page of a rebuild, between two live pages.
pub const DEFAULT_PROCESSOR_REBUILD_KEYS: usize = 100;
/// The lease of a checkpoint; a killed owner is replaced after it.
pub const DEFAULT_PROCESSOR_LEASE: Duration = Duration::from_secs(10);
/// The shortest lease: the owner extends every third of it.
pub const MIN_PROCESSOR_LEASE: Duration = Duration::from_secs(1);
/// The pause after the first failed round; it doubles with every failure in a row.
pub const DEFAULT_PROCESSOR_RETRY_BASE: Duration = Duration::from_millis(500);
/// The longest pause after a failed round.
pub const DEFAULT_PROCESSOR_RETRY_MAX: Duration = Duration::from_secs(10);
/// Documents written at once: live batches and rebuild writes together.
pub const DEFAULT_PROCESSOR_WRITE_CONCURRENCY: usize = 16;
/// How long a processor waits after a commit woke it before it reads the journal: the commits of a burst land
/// in one page, one read and one write per document instead of one per commit. Zero reads at once.
pub const DEFAULT_PROCESSOR_WAKE_DELAY: Duration = Duration::ZERO;
/// Jobs one key may hold in the delivery: reserved, queued or in the handler.
pub const DEFAULT_DELIVERY_CAPACITY: usize = 64;
/// The smallest capacity: one live page in flight and one rebuild job per key.
pub const MIN_DELIVERY_CAPACITY: usize = 2;
/// Live jobs of one key that one handler call takes at most.
pub const DEFAULT_DELIVERY_MAX_BATCH: usize = 32;

/// Checked bounds of the processor's delivery per aggregate key: how many jobs the key's buffer holds and how
/// many live jobs one handler call takes. A live job is the events of one key in one journal page; a rebuild
/// job rebuilds the key's document.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeliveryLimits {
    capacity: usize,
    max_batch: usize,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum DeliveryLimitsError {
    #[error("the delivery capacity must be at least {min}: one live page in flight and one rebuild job per key")]
    CapacityTooSmall { min: usize },
    #[error("the largest delivery batch must be positive")]
    ZeroMaxBatch,
}

impl DeliveryLimits {
    pub fn try_new(capacity: usize, max_batch: usize) -> Result<Self, DeliveryLimitsError> {
        if capacity < MIN_DELIVERY_CAPACITY {
            return Err(DeliveryLimitsError::CapacityTooSmall {
                min: MIN_DELIVERY_CAPACITY,
            });
        }
        if max_batch == 0 {
            return Err(DeliveryLimitsError::ZeroMaxBatch);
        }
        Ok(Self { capacity, max_batch })
    }

    /// How many jobs of one key may be reserved, queued or in the handler at once: the live pages in flight
    /// ([`ProcessorConfig::pages_in_flight`]) and the place of one rebuild job. The next page of the key
    /// waits for space, so a slow handler slows its key down instead of growing memory.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// How many queued live jobs one handler call takes at most. A batch is taken from what has already
    /// gathered and never waits for more; `1` hands every job over on its own.
    pub fn max_batch(&self) -> usize {
        self.max_batch
    }
}

impl Default for DeliveryLimits {
    /// [`DEFAULT_DELIVERY_CAPACITY`] and [`DEFAULT_DELIVERY_MAX_BATCH`].
    fn default() -> Self {
        Self {
            capacity: DEFAULT_DELIVERY_CAPACITY,
            max_batch: DEFAULT_DELIVERY_MAX_BATCH,
        }
    }
}

/// Checked bounds of a projection processor, built by [`ProcessorConfig::builder`]: every bound it does not set keeps
/// the default above.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessorConfig {
    poll_interval: Duration,
    page_size: usize,
    lease: Duration,
    delivery_limits: DeliveryLimits,
    rebuild_keys: usize,
    retry_base: Duration,
    retry_max: Duration,
    write_concurrency: usize,
    wake_delay: Duration,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ProcessorConfigError {
    #[error("the poll interval must be positive")]
    ZeroPollInterval,
    #[error("the page size must be positive")]
    ZeroPageSize,
    #[error("the rebuild page must be positive")]
    ZeroRebuildKeys,
    #[error("the lease must be at least {min:?}")]
    LeaseTooShort { min: Duration },
    #[error("the retry base must be positive")]
    ZeroRetryBase,
    #[error("the retry base must be at most the retry maximum {max:?}")]
    RetryBaseTooLong { max: Duration },
    #[error("the concurrency must be positive")]
    ZeroWriteConcurrency,
    #[error("the settle time must be at most the poll interval {max:?}")]
    WakeDelayTooLong { max: Duration },
}

/// The builder of [`ProcessorConfig`]: every bound starts at its default, [`build`](Self::build) checks them together.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProcessorConfigBuilder {
    config: ProcessorConfig,
}

impl ProcessorConfigBuilder {
    /// How long the processor waits for the next page when no commit woke it; positive.
    #[must_use]
    pub fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.config.poll_interval = poll_interval;
        self
    }

    /// Events per journal page; positive.
    #[must_use]
    pub fn page_size(mut self, page_size: usize) -> Self {
        self.config.page_size = page_size;
        self
    }

    /// The lease of a checkpoint; at least [`MIN_PROCESSOR_LEASE`].
    #[must_use]
    pub fn lease(mut self, lease: Duration) -> Self {
        self.config.lease = lease;
        self
    }

    /// `delivery_limits` bound the buffer of every key: its `capacity` minus one is how many live pages may be
    /// in flight at once, its `max_batch` how many of them one handler call may take for a key.
    #[must_use]
    pub fn delivery_limits(mut self, delivery_limits: DeliveryLimits) -> Self {
        self.config.delivery_limits = delivery_limits;
        self
    }

    /// The pause after a failed round: `base` after the first failure, doubling up to `max`. The base is positive
    /// and at most `max`.
    #[must_use]
    pub fn retry(mut self, base: Duration, max: Duration) -> Self {
        self.config.retry_base = base;
        self.config.retry_max = max;
        self
    }

    /// Keys per page of a rebuild; positive.
    #[must_use]
    pub fn rebuild_keys(mut self, keys: usize) -> Self {
        self.config.rebuild_keys = keys;
        self
    }

    /// Documents written at once; positive.
    #[must_use]
    pub fn write_concurrency(mut self, documents: usize) -> Self {
        self.config.write_concurrency = documents;
        self
    }

    /// The pause between a commit's wake-up and the read of the journal: the commits of a burst share a page.
    /// At most the poll interval, past which the wake-up would be slower than the poll it replaces.
    #[must_use]
    pub fn wake_delay(mut self, wake_delay: Duration) -> Self {
        self.config.wake_delay = wake_delay;
        self
    }

    /// The configuration, or the first bound that is refused.
    pub fn build(self) -> Result<ProcessorConfig, ProcessorConfigError> {
        let config = self.config;
        if config.poll_interval.is_zero() {
            return Err(ProcessorConfigError::ZeroPollInterval);
        }
        if config.page_size == 0 {
            return Err(ProcessorConfigError::ZeroPageSize);
        }
        if config.lease < MIN_PROCESSOR_LEASE {
            return Err(ProcessorConfigError::LeaseTooShort {
                min: MIN_PROCESSOR_LEASE,
            });
        }
        if config.retry_base.is_zero() {
            return Err(ProcessorConfigError::ZeroRetryBase);
        }
        if config.retry_base > config.retry_max {
            return Err(ProcessorConfigError::RetryBaseTooLong { max: config.retry_max });
        }
        if config.rebuild_keys == 0 {
            return Err(ProcessorConfigError::ZeroRebuildKeys);
        }
        if config.write_concurrency == 0 {
            return Err(ProcessorConfigError::ZeroWriteConcurrency);
        }
        if config.wake_delay > config.poll_interval {
            return Err(ProcessorConfigError::WakeDelayTooLong {
                max: config.poll_interval,
            });
        }
        Ok(config)
    }
}

impl ProcessorConfig {
    /// A builder; every bound it does not set keeps its default.
    pub fn builder() -> ProcessorConfigBuilder {
        ProcessorConfigBuilder::default()
    }

    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub fn lease(&self) -> Duration {
        self.lease
    }

    pub fn delivery_limits(&self) -> DeliveryLimits {
        self.delivery_limits
    }

    pub fn rebuild_keys(&self) -> usize {
        self.rebuild_keys
    }

    pub fn retry_base(&self) -> Duration {
        self.retry_base
    }

    pub fn retry_max(&self) -> Duration {
        self.retry_max
    }

    pub fn write_concurrency(&self) -> usize {
        self.write_concurrency
    }

    pub fn wake_delay(&self) -> Duration {
        self.wake_delay
    }

    /// Live pages whose receipts may be in flight at once: the key buffer's capacity less the one place a
    /// rebuild job of the key may take.
    pub fn pages_in_flight(&self) -> usize {
        self.delivery_limits.capacity() - 1
    }

    /// The pause after `failures` failed rounds in a row: `retry_base · 2^(failures − 1)`, at most `retry_max`.
    pub fn pause(&self, failures: u32) -> Duration {
        let doubling = 2u32.saturating_pow(failures.saturating_sub(1));
        self.retry_base.saturating_mul(doubling).min(self.retry_max)
    }
}

impl Default for ProcessorConfig {
    /// The constants of this module and [`DeliveryLimits::default`].
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_PROCESSOR_POLL_INTERVAL,
            page_size: DEFAULT_PROCESSOR_PAGE_SIZE,
            lease: DEFAULT_PROCESSOR_LEASE,
            delivery_limits: DeliveryLimits::default(),
            rebuild_keys: DEFAULT_PROCESSOR_REBUILD_KEYS,
            retry_base: DEFAULT_PROCESSOR_RETRY_BASE,
            retry_max: DEFAULT_PROCESSOR_RETRY_MAX,
            write_concurrency: DEFAULT_PROCESSOR_WRITE_CONCURRENCY,
            wake_delay: DEFAULT_PROCESSOR_WAKE_DELAY,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seconds(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    #[test]
    fn defaults_equal_the_constants() {
        let config = ProcessorConfig::default();
        assert_eq!(ProcessorConfig::builder().build().unwrap(), config);
        assert_eq!(config.poll_interval(), Duration::from_millis(250));
        assert_eq!(
            (config.page_size(), config.rebuild_keys(), config.write_concurrency()),
            (1000, 100, 16)
        );
        assert_eq!(
            (config.lease(), config.retry_base(), config.retry_max()),
            (seconds(10), Duration::from_millis(500), seconds(10))
        );
        assert_eq!(config.delivery_limits(), DeliveryLimits::default());
        assert_eq!(config.pages_in_flight(), 63);
    }

    #[test]
    fn every_bound_is_checked_with_a_readable_message() {
        let delivery = DeliveryLimits::try_new(4, 2).unwrap();
        let valid = || {
            ProcessorConfig::builder()
                .poll_interval(seconds(1))
                .page_size(10)
                .lease(seconds(1))
                .delivery_limits(delivery)
        };
        assert_eq!(valid().build().unwrap().pages_in_flight(), 3);
        for (config, error) in [
            (
                ProcessorConfig::builder().poll_interval(seconds(0)).build(),
                ProcessorConfigError::ZeroPollInterval,
            ),
            (
                ProcessorConfig::builder().page_size(0).build(),
                ProcessorConfigError::ZeroPageSize,
            ),
            (
                ProcessorConfig::builder().lease(Duration::from_millis(999)).build(),
                ProcessorConfigError::LeaseTooShort { min: seconds(1) },
            ),
            (
                valid().retry(seconds(0), seconds(1)).build(),
                ProcessorConfigError::ZeroRetryBase,
            ),
            (
                valid().retry(seconds(2), seconds(1)).build(),
                ProcessorConfigError::RetryBaseTooLong { max: seconds(1) },
            ),
            (valid().rebuild_keys(0).build(), ProcessorConfigError::ZeroRebuildKeys),
            (
                valid().write_concurrency(0).build(),
                ProcessorConfigError::ZeroWriteConcurrency,
            ),
            (
                valid().wake_delay(Duration::from_millis(1001)).build(),
                ProcessorConfigError::WakeDelayTooLong { max: seconds(1) },
            ),
        ] {
            assert_eq!(config, Err(error));
        }
        let tuned = valid()
            .retry(seconds(1), seconds(1))
            .rebuild_keys(7)
            .write_concurrency(3)
            .wake_delay(seconds(1))
            .build()
            .unwrap();
        assert_eq!(
            (
                tuned.retry_base(),
                tuned.retry_max(),
                tuned.rebuild_keys(),
                tuned.write_concurrency(),
                tuned.wake_delay()
            ),
            (seconds(1), seconds(1), 7, 3, seconds(1))
        );
        assert_eq!(valid().build().unwrap().wake_delay(), DEFAULT_PROCESSOR_WAKE_DELAY);
        assert_eq!(
            ProcessorConfigError::ZeroPollInterval.to_string(),
            "the poll interval must be positive"
        );
        assert_eq!(
            ProcessorConfigError::ZeroPageSize.to_string(),
            "the page size must be positive"
        );
        assert_eq!(
            ProcessorConfigError::ZeroRebuildKeys.to_string(),
            "the rebuild page must be positive"
        );
        assert_eq!(
            ProcessorConfigError::LeaseTooShort { min: seconds(1) }.to_string(),
            "the lease must be at least 1s"
        );
        assert_eq!(
            ProcessorConfigError::ZeroRetryBase.to_string(),
            "the retry base must be positive"
        );
        assert_eq!(
            ProcessorConfigError::RetryBaseTooLong { max: seconds(1) }.to_string(),
            "the retry base must be at most the retry maximum 1s"
        );
        assert_eq!(
            ProcessorConfigError::ZeroWriteConcurrency.to_string(),
            "the concurrency must be positive"
        );
        assert_eq!(
            ProcessorConfigError::WakeDelayTooLong { max: seconds(1) }.to_string(),
            "the settle time must be at most the poll interval 1s"
        );
    }

    #[test]
    fn delivery_limits_reject_a_small_capacity_and_no_batch_and_default_to_the_constants() {
        assert_eq!(
            DeliveryLimits::try_new(1, 1),
            Err(DeliveryLimitsError::CapacityTooSmall { min: 2 })
        );
        assert_eq!(DeliveryLimits::try_new(2, 0), Err(DeliveryLimitsError::ZeroMaxBatch));
        let limits = DeliveryLimits::try_new(MIN_DELIVERY_CAPACITY, 1).unwrap();
        assert_eq!((limits.capacity(), limits.max_batch()), (2, 1));
        let defaults = DeliveryLimits::default();
        assert_eq!(
            (defaults.capacity(), defaults.max_batch()),
            (DEFAULT_DELIVERY_CAPACITY, DEFAULT_DELIVERY_MAX_BATCH)
        );
        assert_eq!((defaults.capacity(), defaults.max_batch()), (64, 32));
        assert_eq!(
            DeliveryLimitsError::CapacityTooSmall { min: 2 }.to_string(),
            "the delivery capacity must be at least 2: one live page in flight and one rebuild job per key"
        );
        assert_eq!(
            DeliveryLimitsError::ZeroMaxBatch.to_string(),
            "the largest delivery batch must be positive"
        );
    }

    #[test]
    fn the_pause_doubles_from_the_base_up_to_the_maximum_without_overflowing() {
        let config = ProcessorConfig::default();
        let ms = |n| Duration::from_millis(n);
        assert_eq!(
            (0..=6).map(|n| config.pause(n)).collect::<Vec<_>>(),
            [ms(500), ms(500), ms(1000), ms(2000), ms(4000), ms(8000), ms(10_000)]
        );
        assert_eq!(config.pause(u32::MAX), seconds(10));
        let capped = ProcessorConfig::builder().retry(seconds(3), seconds(3)).build().unwrap();
        assert_eq!((capped.pause(1), capped.pause(9)), (seconds(3), seconds(3)));
    }
}
