//! The checked configuration of a projection processor and its defaults.
use std::time::Duration;
use thiserror::Error;

/// How long a processor waits for the next page when no commit woke it, and between claims of a held row.
pub const DEFAULT_PROCESSOR_POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Events per journal page.
pub const DEFAULT_PROCESSOR_PAGE_SIZE: usize = 1000;
/// Keys per page of a rebuild, between two live pages.
pub const DEFAULT_PROCESSOR_REBUILD_PAGE: usize = 100;
/// The lease of a checkpoint; a killed owner is replaced after it.
pub const DEFAULT_PROCESSOR_LEASE: Duration = Duration::from_secs(10);
/// The shortest lease: the owner extends every third of it.
pub const MIN_PROCESSOR_LEASE: Duration = Duration::from_secs(1);
/// The pause after the first failed round; it doubles with every failure in a row.
pub const DEFAULT_PROCESSOR_RETRY_BASE: Duration = Duration::from_millis(500);
/// The longest pause after a failed round.
pub const DEFAULT_PROCESSOR_RETRY_MAX: Duration = Duration::from_secs(10);
/// Documents written at once: live batches and rebuild writes together.
pub const DEFAULT_PROCESSOR_CONCURRENCY: usize = 16;
/// How long a processor waits after a commit woke it before it reads the journal: the commits of a burst land
/// in one page, one read and one write per document instead of one per commit. Zero reads at once.
pub const DEFAULT_PROCESSOR_SETTLE: Duration = Duration::ZERO;
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
    pub fn new(capacity: usize, max_batch: usize) -> Result<Self, DeliveryLimitsError> {
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

/// Checked bounds of a projection processor. `new` takes what every deployment sets; the rest keep the
/// defaults above until a fallible `with_*` changes them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessorConfig {
    poll_interval: Duration,
    page_size: usize,
    lease: Duration,
    delivery_limits: DeliveryLimits,
    rebuild_page: usize,
    retry_base: Duration,
    retry_max: Duration,
    concurrency: usize,
    settle: Duration,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ProcessorConfigError {
    #[error("the poll interval must be positive")]
    ZeroPollInterval,
    #[error("the page size must be positive")]
    ZeroPageSize,
    #[error("the rebuild page must be positive")]
    ZeroRebuildPage,
    #[error("the lease must be at least {min:?}")]
    LeaseTooShort { min: Duration },
    #[error("the retry base must be positive and at most the retry maximum")]
    InvalidRetry,
    #[error("the concurrency must be positive")]
    ZeroConcurrency,
    #[error("the settle time must be at most the poll interval {max:?}")]
    SettleTooLong { max: Duration },
}

impl ProcessorConfig {
    /// `delivery_limits` bound the buffer of every key: its `capacity` minus one is how many live pages may be
    /// in flight at once, its `max_batch` how many of them one handler call may take for a key.
    pub fn new(
        poll_interval: Duration,
        page_size: usize,
        lease: Duration,
        delivery_limits: DeliveryLimits,
    ) -> Result<Self, ProcessorConfigError> {
        if poll_interval.is_zero() {
            return Err(ProcessorConfigError::ZeroPollInterval);
        }
        if page_size == 0 {
            return Err(ProcessorConfigError::ZeroPageSize);
        }
        if lease < MIN_PROCESSOR_LEASE {
            return Err(ProcessorConfigError::LeaseTooShort {
                min: MIN_PROCESSOR_LEASE,
            });
        }
        Ok(Self {
            poll_interval,
            page_size,
            lease,
            delivery_limits,
            rebuild_page: DEFAULT_PROCESSOR_REBUILD_PAGE,
            retry_base: DEFAULT_PROCESSOR_RETRY_BASE,
            retry_max: DEFAULT_PROCESSOR_RETRY_MAX,
            concurrency: DEFAULT_PROCESSOR_CONCURRENCY,
            settle: DEFAULT_PROCESSOR_SETTLE,
        })
    }

    /// The pause after a failed round: `base` after the first failure, doubling up to `max`.
    pub fn with_retry(mut self, base: Duration, max: Duration) -> Result<Self, ProcessorConfigError> {
        if base.is_zero() || base > max {
            return Err(ProcessorConfigError::InvalidRetry);
        }
        self.retry_base = base;
        self.retry_max = max;
        Ok(self)
    }

    /// Keys per page of a rebuild.
    pub fn with_rebuild_page(mut self, keys: usize) -> Result<Self, ProcessorConfigError> {
        if keys == 0 {
            return Err(ProcessorConfigError::ZeroRebuildPage);
        }
        self.rebuild_page = keys;
        Ok(self)
    }

    /// Documents written at once.
    pub fn with_concurrency(mut self, keys: usize) -> Result<Self, ProcessorConfigError> {
        if keys == 0 {
            return Err(ProcessorConfigError::ZeroConcurrency);
        }
        self.concurrency = keys;
        Ok(self)
    }

    /// The pause between a commit's wake-up and the read of the journal: the commits of a burst share a page.
    /// At most the poll interval, past which the wake-up would be slower than the poll it replaces.
    pub fn with_settle(mut self, settle: Duration) -> Result<Self, ProcessorConfigError> {
        if settle > self.poll_interval {
            return Err(ProcessorConfigError::SettleTooLong { max: self.poll_interval });
        }
        self.settle = settle;
        Ok(self)
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

    pub fn rebuild_page(&self) -> usize {
        self.rebuild_page
    }

    pub fn retry_base(&self) -> Duration {
        self.retry_base
    }

    pub fn retry_max(&self) -> Duration {
        self.retry_max
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub fn settle(&self) -> Duration {
        self.settle
    }

    /// Live pages whose receipts may be in flight at once: the key buffer's capacity less the one place a
    /// rebuild job of the key may take.
    pub fn pages_in_flight(&self) -> usize {
        self.delivery_limits.capacity() - 1
    }

    /// The pause after `failures` failed rounds in a row: `retry_base · 2^(failures − 1)`, at most `retry_max`.
    pub fn backoff(&self, failures: u32) -> Duration {
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
            rebuild_page: DEFAULT_PROCESSOR_REBUILD_PAGE,
            retry_base: DEFAULT_PROCESSOR_RETRY_BASE,
            retry_max: DEFAULT_PROCESSOR_RETRY_MAX,
            concurrency: DEFAULT_PROCESSOR_CONCURRENCY,
            settle: DEFAULT_PROCESSOR_SETTLE,
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
        assert_eq!(
            ProcessorConfig::new(
                seconds(0) + DEFAULT_PROCESSOR_POLL_INTERVAL,
                1000,
                seconds(10),
                DeliveryLimits::default()
            )
            .unwrap(),
            config
        );
        assert_eq!(config.poll_interval(), Duration::from_millis(250));
        assert_eq!(
            (config.page_size(), config.rebuild_page(), config.concurrency()),
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
        let delivery = DeliveryLimits::new(4, 2).unwrap();
        let valid = ProcessorConfig::new(seconds(1), 10, seconds(1), delivery).unwrap();
        assert_eq!(valid.pages_in_flight(), 3);
        for (config, error) in [
            (
                ProcessorConfig::new(seconds(0), 10, seconds(1), delivery),
                ProcessorConfigError::ZeroPollInterval,
            ),
            (
                ProcessorConfig::new(seconds(1), 0, seconds(1), delivery),
                ProcessorConfigError::ZeroPageSize,
            ),
            (
                ProcessorConfig::new(seconds(1), 10, Duration::from_millis(999), delivery),
                ProcessorConfigError::LeaseTooShort { min: seconds(1) },
            ),
            (valid.with_retry(seconds(0), seconds(1)), ProcessorConfigError::InvalidRetry),
            (valid.with_retry(seconds(2), seconds(1)), ProcessorConfigError::InvalidRetry),
            (valid.with_rebuild_page(0), ProcessorConfigError::ZeroRebuildPage),
            (valid.with_concurrency(0), ProcessorConfigError::ZeroConcurrency),
            (
                valid.with_settle(Duration::from_millis(1001)),
                ProcessorConfigError::SettleTooLong { max: seconds(1) },
            ),
        ] {
            assert_eq!(config, Err(error));
        }
        let tuned = valid
            .with_retry(seconds(1), seconds(1))
            .unwrap()
            .with_rebuild_page(7)
            .unwrap()
            .with_concurrency(3)
            .unwrap()
            .with_settle(seconds(1))
            .unwrap();
        assert_eq!(
            (
                tuned.retry_base(),
                tuned.retry_max(),
                tuned.rebuild_page(),
                tuned.concurrency(),
                tuned.settle()
            ),
            (seconds(1), seconds(1), 7, 3, seconds(1))
        );
        assert_eq!(valid.settle(), DEFAULT_PROCESSOR_SETTLE);
        assert_eq!(
            ProcessorConfigError::ZeroPollInterval.to_string(),
            "the poll interval must be positive"
        );
        assert_eq!(
            ProcessorConfigError::ZeroPageSize.to_string(),
            "the page size must be positive"
        );
        assert_eq!(
            ProcessorConfigError::ZeroRebuildPage.to_string(),
            "the rebuild page must be positive"
        );
        assert_eq!(
            ProcessorConfigError::LeaseTooShort { min: seconds(1) }.to_string(),
            "the lease must be at least 1s"
        );
        assert_eq!(
            ProcessorConfigError::InvalidRetry.to_string(),
            "the retry base must be positive and at most the retry maximum"
        );
        assert_eq!(
            ProcessorConfigError::ZeroConcurrency.to_string(),
            "the concurrency must be positive"
        );
        assert_eq!(
            ProcessorConfigError::SettleTooLong { max: seconds(1) }.to_string(),
            "the settle time must be at most the poll interval 1s"
        );
    }

    #[test]
    fn delivery_limits_reject_a_small_capacity_and_no_batch_and_default_to_the_constants() {
        assert_eq!(
            DeliveryLimits::new(1, 1),
            Err(DeliveryLimitsError::CapacityTooSmall { min: 2 })
        );
        assert_eq!(DeliveryLimits::new(2, 0), Err(DeliveryLimitsError::ZeroMaxBatch));
        let limits = DeliveryLimits::new(MIN_DELIVERY_CAPACITY, 1).unwrap();
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
    fn the_backoff_doubles_from_the_base_up_to_the_maximum_without_overflowing() {
        let config = ProcessorConfig::default();
        let ms = |n| Duration::from_millis(n);
        assert_eq!(
            (0..=6).map(|n| config.backoff(n)).collect::<Vec<_>>(),
            [ms(500), ms(500), ms(1000), ms(2000), ms(4000), ms(8000), ms(10_000)]
        );
        assert_eq!(config.backoff(u32::MAX), seconds(10));
        let capped = ProcessorConfig::default().with_retry(seconds(3), seconds(3)).unwrap();
        assert_eq!((capped.backoff(1), capped.backoff(9)), (seconds(3), seconds(3)));
    }
}
