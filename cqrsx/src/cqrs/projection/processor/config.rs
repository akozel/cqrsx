//! The checked configuration of a projection processor and its defaults.
use crate::cqrs::event_handler::delivery::DeliveryConfig;
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

/// Checked bounds of a projection processor. `new` takes what every deployment sets; the rest keep the
/// defaults above until a fallible `with_*` changes them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessorConfig {
    poll_interval: Duration,
    page_size: usize,
    lease: Duration,
    delivery: DeliveryConfig,
    rebuild_page: usize,
    retry_base: Duration,
    retry_max: Duration,
    concurrency: usize,
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
    #[error("the delivery capacity must be at least 2: one live page in flight and one rebuild job per key")]
    DeliveryCapacityTooSmall,
}

impl ProcessorConfig {
    /// `delivery` bounds the buffer of every key: its `capacity` minus one is how many live pages may be in
    /// flight at once, its `max_batch` how many of them one handler call may take for a key.
    pub fn new(
        poll_interval: Duration,
        page_size: usize,
        lease: Duration,
        delivery: DeliveryConfig,
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
        if delivery.capacity() < 2 {
            return Err(ProcessorConfigError::DeliveryCapacityTooSmall);
        }
        Ok(Self {
            poll_interval,
            page_size,
            lease,
            delivery,
            rebuild_page: DEFAULT_PROCESSOR_REBUILD_PAGE,
            retry_base: DEFAULT_PROCESSOR_RETRY_BASE,
            retry_max: DEFAULT_PROCESSOR_RETRY_MAX,
            concurrency: DEFAULT_PROCESSOR_CONCURRENCY,
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

    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub fn lease(&self) -> Duration {
        self.lease
    }

    pub fn delivery(&self) -> DeliveryConfig {
        self.delivery
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

    /// Live pages whose receipts may be in flight at once: the key buffer's capacity less the one place a
    /// rebuild job of the key may take.
    pub fn pages_in_flight(&self) -> usize {
        self.delivery.capacity() - 1
    }

    /// The pause after `failures` failed rounds in a row: `retry_base · 2^(failures − 1)`, at most `retry_max`.
    pub fn backoff(&self, failures: u32) -> Duration {
        let doubling = 2u32.saturating_pow(failures.saturating_sub(1));
        self.retry_base.saturating_mul(doubling).min(self.retry_max)
    }
}

impl Default for ProcessorConfig {
    /// The constants of this module and [`DeliveryConfig::default`].
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_PROCESSOR_POLL_INTERVAL,
            page_size: DEFAULT_PROCESSOR_PAGE_SIZE,
            lease: DEFAULT_PROCESSOR_LEASE,
            delivery: DeliveryConfig::default(),
            rebuild_page: DEFAULT_PROCESSOR_REBUILD_PAGE,
            retry_base: DEFAULT_PROCESSOR_RETRY_BASE,
            retry_max: DEFAULT_PROCESSOR_RETRY_MAX,
            concurrency: DEFAULT_PROCESSOR_CONCURRENCY,
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
                DeliveryConfig::default()
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
        assert_eq!(config.delivery(), DeliveryConfig::default());
        assert_eq!(config.pages_in_flight(), 63);
    }

    #[test]
    fn every_bound_is_checked_with_a_readable_message() {
        let delivery = DeliveryConfig::new(4, 2).unwrap();
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
            (
                ProcessorConfig::new(seconds(1), 10, seconds(1), DeliveryConfig::new(1, 1).unwrap()),
                ProcessorConfigError::DeliveryCapacityTooSmall,
            ),
            (valid.with_retry(seconds(0), seconds(1)), ProcessorConfigError::InvalidRetry),
            (valid.with_retry(seconds(2), seconds(1)), ProcessorConfigError::InvalidRetry),
            (valid.with_rebuild_page(0), ProcessorConfigError::ZeroRebuildPage),
            (valid.with_concurrency(0), ProcessorConfigError::ZeroConcurrency),
        ] {
            assert_eq!(config, Err(error));
        }
        let tuned = valid
            .with_retry(seconds(1), seconds(1))
            .unwrap()
            .with_rebuild_page(7)
            .unwrap()
            .with_concurrency(3)
            .unwrap();
        assert_eq!(
            (
                tuned.retry_base(),
                tuned.retry_max(),
                tuned.rebuild_page(),
                tuned.concurrency()
            ),
            (seconds(1), seconds(1), 7, 3)
        );
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
            ProcessorConfigError::DeliveryCapacityTooSmall.to_string(),
            "the delivery capacity must be at least 2: one live page in flight and one rebuild job per key"
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
