//! Process configuration from environment variables (see `demo/.env.example`).
//!
//! Parsing happens once at start-up, so this is the one place where a hard failure with a clear
//! message is acceptable; everything after `Config::from_env` is free of `unwrap`/`expect`.
use crate::application::operations::rebuild::{DEFAULT_MAX_PACE, DEFAULT_SLOW_PACE, DEFAULT_STATUS_TIMEOUT};
use cqrsx::{
    cqrs::{
        command::locks::{DEFAULT_LOCK_MAX_WAIT, DEFAULT_LOCK_MAX_WAITERS, LockLimits, LockLimitsError},
        projection::processor::config::{
            DEFAULT_DELIVERY_CAPACITY, DEFAULT_DELIVERY_MAX_BATCH, DEFAULT_PROCESSOR_CONCURRENCY, DEFAULT_PROCESSOR_LEASE,
            DEFAULT_PROCESSOR_PAGE_SIZE, DEFAULT_PROCESSOR_POLL_INTERVAL, DEFAULT_PROCESSOR_REBUILD_PAGE,
            DEFAULT_PROCESSOR_RETRY_BASE, DEFAULT_PROCESSOR_RETRY_MAX, DeliveryLimits, DeliveryLimitsError, ProcessorConfig,
            ProcessorConfigError,
        },
    },
    postgres::{event_repository::DEFAULT_COUNT_AFTER_CAP, pool::StatementTimeout},
    task::runtime::config::{TaskRuntimeConfig, TaskRuntimeConfigError},
};
use std::{env, net::SocketAddr, num::NonZeroU32, str::FromStr, time::Duration};

/// The owner name a process writes on the checkpoints it holds, unless `ENCORE_NODE_NAME` names it.
pub const DEFAULT_NODE_NAME: &str = "encore-backend";
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{name}: {message}")]
    Invalid { name: &'static str, message: String },
    #[error("task runtime configuration: {0}")]
    TaskRuntime(#[from] TaskRuntimeConfigError),
}

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    /// Connections in the main PostgreSQL pool, shared by HTTP commands, the commands task handlers
    /// run and the outbox queue: the ceiling of concurrent short database work of the process.
    pub database_pool_size: NonZeroU32,
    /// Server-side cap of every statement of the main pool (`ENCORE_DATABASE_STATEMENT_TIMEOUT_MS`:
    /// unset = the library's default, 4 s; 0 = no cap).
    pub database_statement_timeout: StatementTimeout,
    /// Connections of the history pool, which has no statement timeout: verify, the ledger totals and the
    /// time machine read whole histories. It keeps no idle connection.
    pub database_history_pool_size: NonZeroU32,
    /// Connections of the projection pool (`ENCORE_DATABASE_PROJECTION_POOL_SIZE`), under the main pool's
    /// statement timeout: the projection processors read the journal through it (pages, and on a rebuild
    /// the history of every key of a page), so a rebuild never takes the connections of the commands.
    pub database_projection_pool_size: NonZeroU32,
    pub mongodb_uri: String,
    pub read_db: String,
    pub http_addr: SocketAddr,
    /// Base URL of the external mail service the e-mail tasks call (`POST {url}/mail`), without a
    /// trailing slash. The demo's provider is hosted by the sidecar.
    pub mail_service_url: String,
    /// Claim-attempt budget of every outbox task: the chain tasks (`encore_record_*`) and the e-mails.
    pub task_max_attempts: i32,
    /// Backoff an e-mail task asks for after a refused delivery: `min(base · 2^(n−1), max)`.
    pub task_retry_base: Duration,
    pub task_retry_max: Duration,
    pub ticket_limit: usize,
    pub snapshot_every: usize,
    /// How long, and how many, commands wait for one concert's or fan ledger's lock
    /// (`ENCORE_COMMAND_LOCK_MAX_WAIT_MS`, `ENCORE_COMMAND_LOCK_MAX_WAITERS`).
    pub command_lock_limits: LockLimits,
    /// The projection processors that feed the read models from the journal (`ENCORE_PROJECTION_*`).
    pub projection: ProcessorConfig,
    /// Events counted at most for one projection's lag (`ENCORE_PROJECTION_LAG_CAP`).
    pub projection_lag_cap: u64,
    /// How long the status waits for the checkpoints before it answers with the rows it saw last
    /// (`ENCORE_PROJECTION_STATUS_TIMEOUT_MS`).
    pub projection_status_timeout: Duration,
    /// The owner this process writes on the checkpoints it holds (`ENCORE_NODE_NAME`).
    pub node_name: String,
    /// The pause between the events of one key in a slow Encore! (`ENCORE_REPLAY_SLOW_DELAY_MS`); the UI asks
    /// for it.
    pub replay_slow_delay: Duration,
    /// The longest pause a rebuild request may ask for (`ENCORE_REPLAY_MAX_PACE_MS`).
    pub replay_max_pace: Duration,
    /// The library's own `OUTBOX_*` keys: worker pool and storage timeouts.
    pub task_runtime: TaskRuntimeConfig,
}

/// Demo defaults for the library's own `OUTBOX_*` keys, used when the variable is absent.
pub(crate) fn outbox_default(name: &str) -> Option<String> {
    let value = match name {
        "OUTBOX_WORKER_CONCURRENCY" => "2",
        "OUTBOX_QUEUE_CAPACITY" => "2",
        "OUTBOX_POLL_INTERVAL_MS" => "300",
        "OUTBOX_HANDLER_TIMEOUT_SECS" => "5",
        "OUTBOX_LEASE_SECS" => "30",
        "OUTBOX_STORAGE_TIMEOUT_MS" => "3000",
        _ => return None,
    };
    Some(value.to_owned())
}

fn outbox_lookup(name: &str) -> Option<String> {
    env::var(name).ok().or_else(|| outbox_default(name))
}

fn var(name: &'static str) -> Option<String> {
    env::var(name).ok().map(|v| v.trim().to_owned()).filter(|v| !v.is_empty())
}

fn parse<T: std::str::FromStr>(name: &'static str, default: &str) -> Result<T, ConfigError>
where
    T::Err: std::fmt::Display,
{
    parse_value(name, &var(name).unwrap_or_else(|| default.to_owned()))
}

fn parse_value<T: std::str::FromStr>(name: &'static str, raw: &str) -> Result<T, ConfigError>
where
    T::Err: std::fmt::Display,
{
    raw.parse().map_err(|error: T::Err| ConfigError::Invalid {
        name,
        message: format!("cannot parse {raw:?}: {error}"),
    })
}

const LOCK_MAX_WAIT: &str = "ENCORE_COMMAND_LOCK_MAX_WAIT_MS";
const LOCK_MAX_WAITERS: &str = "ENCORE_COMMAND_LOCK_MAX_WAITERS";

/// Commands to one concert or fan ledger run one at a time under its lock, in arrival order (backlog 02/08c);
/// these bound the wait for the lock and default to the library's. A chain task waits for the lock inside its
/// handler, so the wait must end before the handler times out.
fn command_lock_limits(
    lookup: impl Fn(&'static str) -> Option<String>,
    handler_timeout: Duration,
) -> Result<LockLimits, ConfigError> {
    let max_wait = match lookup(LOCK_MAX_WAIT) {
        Some(raw) => Duration::from_millis(parse_value(LOCK_MAX_WAIT, &raw)?),
        None => DEFAULT_LOCK_MAX_WAIT,
    };
    let max_waiters = match lookup(LOCK_MAX_WAITERS) {
        Some(raw) => parse_value(LOCK_MAX_WAITERS, &raw)?,
        None => DEFAULT_LOCK_MAX_WAITERS.get(),
    };
    let limits = LockLimits::new(max_wait, max_waiters).map_err(|error| match error {
        LockLimitsError::ZeroMaxWait => invalid(LOCK_MAX_WAIT, "must be positive"),
        LockLimitsError::ZeroMaxWaiters => invalid(LOCK_MAX_WAITERS, "must be positive"),
    })?;
    if max_wait >= handler_timeout {
        return Err(invalid(LOCK_MAX_WAIT, "must be below OUTBOX_HANDLER_TIMEOUT_SECS"));
    }
    Ok(limits)
}

const PROJECTION_POLL: &str = "ENCORE_PROJECTION_POLL_MS";
const PROJECTION_PAGE: &str = "ENCORE_PROJECTION_PAGE";
const PROJECTION_LEASE: &str = "ENCORE_PROJECTION_LEASE_MS";
const PROJECTION_RETRY_BASE: &str = "ENCORE_PROJECTION_RETRY_BASE_MS";
const PROJECTION_RETRY_MAX: &str = "ENCORE_PROJECTION_RETRY_MAX_MS";
const PROJECTION_CONCURRENCY: &str = "ENCORE_PROJECTION_CONCURRENCY";
const PROJECTION_BUFFER: &str = "ENCORE_PROJECTION_BUFFER";
const PROJECTION_MAX_BATCH: &str = "ENCORE_PROJECTION_MAX_BATCH";
const PROJECTION_REBUILD_PAGE: &str = "ENCORE_PROJECTION_REBUILD_PAGE";
const PROJECTION_SETTLE: &str = "ENCORE_PROJECTION_SETTLE_MS";
/// The demo's pause between a commit's wake-up and the processor's read (`ENCORE_PROJECTION_SETTLE_MS`); the library
/// reads at once. Measured in epic 03/04: on a hot key at 700 commands a second it brings the read side's cost per
/// command back to about the cost of the handler inside the command (PostgreSQL +11 % instead of +42 %, MongoDB and the
/// back-end below it), and the read models trail the journal by about ten events. 50 ms saves no more PostgreSQL.
pub const DEFAULT_PROJECTION_SETTLE: Duration = Duration::from_millis(10);

fn number<T: FromStr>(lookup: &impl Fn(&'static str) -> Option<String>, name: &'static str, default: T) -> Result<T, ConfigError>
where
    T::Err: std::fmt::Display,
{
    match lookup(name) {
        Some(raw) => parse_value(name, &raw),
        None => Ok(default),
    }
}

fn millis(
    lookup: &impl Fn(&'static str) -> Option<String>,
    name: &'static str,
    default: Duration,
) -> Result<Duration, ConfigError> {
    match lookup(name) {
        Some(raw) => Ok(Duration::from_millis(parse_value(name, &raw)?)),
        None => Ok(default),
    }
}

/// The projection processors read the journal of their aggregate type in pages from a checkpoint they hold
/// under a lease and hand every key's events to the key's worker (epic 03). Every bound defaults to the
/// library's; the key buffer is the depth of the window of pages plus one, and holds two at least.
fn projection_config(lookup: impl Fn(&'static str) -> Option<String>) -> Result<ProcessorConfig, ConfigError> {
    let poll = millis(&lookup, PROJECTION_POLL, DEFAULT_PROCESSOR_POLL_INTERVAL)?;
    let page = number(&lookup, PROJECTION_PAGE, DEFAULT_PROCESSOR_PAGE_SIZE)?;
    let lease = millis(&lookup, PROJECTION_LEASE, DEFAULT_PROCESSOR_LEASE)?;
    let retry_base = millis(&lookup, PROJECTION_RETRY_BASE, DEFAULT_PROCESSOR_RETRY_BASE)?;
    let retry_max = millis(&lookup, PROJECTION_RETRY_MAX, DEFAULT_PROCESSOR_RETRY_MAX)?;
    let concurrency = number(&lookup, PROJECTION_CONCURRENCY, DEFAULT_PROCESSOR_CONCURRENCY)?;
    let rebuild_page = number(&lookup, PROJECTION_REBUILD_PAGE, DEFAULT_PROCESSOR_REBUILD_PAGE)?;
    let settle = millis(&lookup, PROJECTION_SETTLE, DEFAULT_PROJECTION_SETTLE)?;
    let capacity = number(&lookup, PROJECTION_BUFFER, DEFAULT_DELIVERY_CAPACITY)?;
    let max_batch = number(&lookup, PROJECTION_MAX_BATCH, DEFAULT_DELIVERY_MAX_BATCH)?;
    let delivery_limits = DeliveryLimits::new(capacity, max_batch).map_err(|error| match error {
        DeliveryLimitsError::CapacityTooSmall { min } => invalid(PROJECTION_BUFFER, format!("must be at least {min}")),
        DeliveryLimitsError::ZeroMaxBatch => invalid(PROJECTION_MAX_BATCH, "must be positive"),
    })?;
    let named = |error: ProcessorConfigError| match error {
        ProcessorConfigError::ZeroPollInterval => invalid(PROJECTION_POLL, "must be positive"),
        ProcessorConfigError::ZeroPageSize => invalid(PROJECTION_PAGE, "must be positive"),
        ProcessorConfigError::ZeroRebuildPage => invalid(PROJECTION_REBUILD_PAGE, "must be positive"),
        ProcessorConfigError::LeaseTooShort { min } => {
            invalid(PROJECTION_LEASE, format!("must be at least {} ms", min.as_millis()))
        }
        ProcessorConfigError::InvalidRetry if retry_base.is_zero() => invalid(PROJECTION_RETRY_BASE, "must be positive"),
        ProcessorConfigError::InvalidRetry => invalid(PROJECTION_RETRY_BASE, "must not exceed ENCORE_PROJECTION_RETRY_MAX_MS"),
        ProcessorConfigError::ZeroConcurrency => invalid(PROJECTION_CONCURRENCY, "must be positive"),
        ProcessorConfigError::SettleTooLong { .. } => invalid(PROJECTION_SETTLE, "must not exceed ENCORE_PROJECTION_POLL_MS"),
    };
    ProcessorConfig::new(poll, page, lease, delivery_limits)
        .and_then(|config| config.with_retry(retry_base, retry_max))
        .and_then(|config| config.with_rebuild_page(rebuild_page))
        .and_then(|config| config.with_concurrency(concurrency))
        .and_then(|config| config.with_settle(settle))
        .map_err(named)
}

const LAG_CAP: &str = "ENCORE_PROJECTION_LAG_CAP";
const STATUS_TIMEOUT: &str = "ENCORE_PROJECTION_STATUS_TIMEOUT_MS";
const SLOW_DELAY: &str = "ENCORE_REPLAY_SLOW_DELAY_MS";
const MAX_PACE: &str = "ENCORE_REPLAY_MAX_PACE_MS";

/// What the operations over the read side are bounded by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadSideSettings {
    lag_cap: u64,
    status_timeout: Duration,
    slow_delay: Duration,
    max_pace: Duration,
}

/// The lag is counted up to a cap, the status waits a while for the checkpoints, and Encore! paces between a
/// slow-motion delay the UI asks for and a maximum nobody may exceed.
fn read_side(lookup: impl Fn(&'static str) -> Option<String>) -> Result<ReadSideSettings, ConfigError> {
    let settings = ReadSideSettings {
        lag_cap: number(&lookup, LAG_CAP, DEFAULT_COUNT_AFTER_CAP)?,
        status_timeout: millis(&lookup, STATUS_TIMEOUT, DEFAULT_STATUS_TIMEOUT)?,
        slow_delay: millis(&lookup, SLOW_DELAY, DEFAULT_SLOW_PACE)?,
        max_pace: millis(&lookup, MAX_PACE, DEFAULT_MAX_PACE)?,
    };
    if settings.lag_cap == 0 {
        return Err(invalid(LAG_CAP, "must be positive"));
    }
    if settings.status_timeout.is_zero() {
        return Err(invalid(STATUS_TIMEOUT, "must be positive"));
    }
    if settings.slow_delay > settings.max_pace {
        return Err(invalid(SLOW_DELAY, "must not exceed ENCORE_REPLAY_MAX_PACE_MS"));
    }
    Ok(settings)
}

/// Unset = `StatementTimeout::Default`, `0` = `Unlimited`, any other number of milliseconds = `Custom`.
fn statement_timeout(value: Option<String>) -> Result<StatementTimeout, ConfigError> {
    const NAME: &str = "ENCORE_DATABASE_STATEMENT_TIMEOUT_MS";
    match value.map(|value| value.parse::<u32>()) {
        None => Ok(StatementTimeout::Default),
        Some(Ok(0)) => Ok(StatementTimeout::Unlimited),
        Some(Ok(millis)) if millis <= i32::MAX as u32 => Ok(StatementTimeout::Custom(Duration::from_millis(millis.into()))),
        Some(_) => Err(invalid(NAME, "must be 0 (no cap) or 1..=2147483647 milliseconds")),
    }
}

fn invalid(name: &'static str, message: impl Into<String>) -> ConfigError {
    ConfigError::Invalid {
        name,
        message: message.into(),
    }
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let task_runtime = TaskRuntimeConfig::from_lookup(outbox_lookup)?;
        let task_max_attempts: i32 = parse("ENCORE_TASK_MAX_ATTEMPTS", "6")?;
        if task_max_attempts <= 0 {
            return Err(invalid("ENCORE_TASK_MAX_ATTEMPTS", "must be positive"));
        }
        let task_retry_base = Duration::from_millis(parse("ENCORE_TASK_RETRY_BASE_MS", "1000")?);
        let task_retry_max = Duration::from_millis(parse("ENCORE_TASK_RETRY_MAX_MS", "8000")?);
        if task_retry_base > task_retry_max {
            return Err(invalid(
                "ENCORE_TASK_RETRY_BASE_MS",
                "must not exceed ENCORE_TASK_RETRY_MAX_MS",
            ));
        }
        let ticket_limit: usize = parse("ENCORE_TICKET_LIMIT", "4")?;
        if ticket_limit == 0 {
            return Err(invalid("ENCORE_TICKET_LIMIT", "must be positive"));
        }
        let snapshot_every: usize = parse("ENCORE_SNAPSHOT_EVERY", "10")?;
        if snapshot_every == 0 {
            return Err(invalid("ENCORE_SNAPSHOT_EVERY", "must be positive"));
        }
        // `NonZeroU32` rejects 0 while parsing.
        let database_pool_size: NonZeroU32 = parse("ENCORE_DATABASE_POOL_SIZE", "10")?;
        let database_history_pool_size: NonZeroU32 = parse("ENCORE_DATABASE_HISTORY_POOL_SIZE", "1")?;
        let database_projection_pool_size: NonZeroU32 = parse("ENCORE_DATABASE_PROJECTION_POOL_SIZE", "4")?;
        let database_statement_timeout = statement_timeout(var("ENCORE_DATABASE_STATEMENT_TIMEOUT_MS"))?;
        let command_lock_limits = command_lock_limits(var, task_runtime.worker_pool().handler_timeout())?;
        let projection = projection_config(var)?;
        let read_side = read_side(var)?;
        Ok(Self {
            database_url: var("ENCORE_DATABASE_URL").unwrap_or_else(|| "postgresql://cqrsx:cqrsx@localhost:5434/encore".into()),
            database_pool_size,
            database_statement_timeout,
            database_history_pool_size,
            database_projection_pool_size,
            mongodb_uri: var("ENCORE_MONGODB_URI").unwrap_or_else(|| "mongodb://localhost:27018".into()),
            read_db: var("ENCORE_READ_DB").unwrap_or_else(|| "encore_read".into()),
            http_addr: parse("ENCORE_HTTP_ADDR", "127.0.0.1:8080")?,
            mail_service_url: var("ENCORE_MAIL_SERVICE_URL")
                .unwrap_or_else(|| "http://127.0.0.1:8090".into())
                .trim_end_matches('/')
                .to_owned(),
            task_max_attempts,
            task_retry_base,
            task_retry_max,
            ticket_limit,
            snapshot_every,
            command_lock_limits,
            projection,
            projection_lag_cap: read_side.lag_cap,
            projection_status_timeout: read_side.status_timeout,
            node_name: var("ENCORE_NODE_NAME").unwrap_or_else(|| DEFAULT_NODE_NAME.into()),
            replay_slow_delay: read_side.slow_delay,
            replay_max_pace: read_side.max_pace,
            task_runtime,
        })
    }

    /// A mail request must give up before the task runtime's handler timeout does, so an attempt
    /// ends with a retry of its own rather than as a timed-out handler: one second earlier, never
    /// below one second.
    pub fn mail_request_timeout(&self) -> Duration {
        self.task_runtime
            .worker_pool()
            .handler_timeout()
            .saturating_sub(Duration::from_secs(1))
            .max(Duration::from_secs(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_timeout_reads_unset_as_default_zero_as_unlimited_and_milliseconds_as_custom() {
        assert_eq!(statement_timeout(None).unwrap(), StatementTimeout::Default);
        assert_eq!(statement_timeout(Some("0".into())).unwrap(), StatementTimeout::Unlimited);
        assert_eq!(
            statement_timeout(Some("2500".into())).unwrap(),
            StatementTimeout::Custom(Duration::from_millis(2500))
        );
        for invalid in ["-1", "x", "2147483648", ""] {
            let error = statement_timeout(Some(invalid.into())).unwrap_err();
            assert!(
                error.to_string().contains("ENCORE_DATABASE_STATEMENT_TIMEOUT_MS"),
                "{invalid}"
            );
        }
    }

    #[test]
    fn the_pool_size_defaults_to_ten_and_rejects_zero() {
        assert_eq!(
            parse::<NonZeroU32>("ENCORE_DATABASE_POOL_SIZE_UNSET_IN_TESTS", "10")
                .unwrap()
                .get(),
            10
        );
        let error = parse::<NonZeroU32>("ENCORE_DATABASE_POOL_SIZE_UNSET_IN_TESTS", "0").unwrap_err();
        assert!(
            error.to_string().contains("ENCORE_DATABASE_POOL_SIZE_UNSET_IN_TESTS"),
            "{error}"
        );
    }

    fn lock_limits(pairs: &[(&'static str, &str)], handler_timeout_secs: u64) -> Result<LockLimits, ConfigError> {
        let pairs: std::collections::HashMap<_, _> = pairs.iter().map(|(k, v)| (*k, v.to_string())).collect();
        command_lock_limits(|name| pairs.get(name).cloned(), Duration::from_secs(handler_timeout_secs))
    }

    #[test]
    fn command_lock_limits_default_to_the_library_limits() {
        assert_eq!(lock_limits(&[], 5).unwrap(), LockLimits::default());
        assert_eq!(
            lock_limits(&[(LOCK_MAX_WAIT, "250"), (LOCK_MAX_WAITERS, "16")], 5).unwrap(),
            LockLimits::new(Duration::from_millis(250), 16).unwrap()
        );
    }

    #[test]
    fn command_lock_limits_name_the_variable_they_reject() {
        for (pairs, name, timeout) in [
            (vec![(LOCK_MAX_WAIT, "x")], LOCK_MAX_WAIT, 5),
            (vec![(LOCK_MAX_WAIT, "0")], LOCK_MAX_WAIT, 5),
            (vec![(LOCK_MAX_WAITERS, "-1")], LOCK_MAX_WAITERS, 5),
            (vec![(LOCK_MAX_WAITERS, "0")], LOCK_MAX_WAITERS, 5),
            // A chain task's handler would time out before its command is refused.
            (vec![], LOCK_MAX_WAIT, 1),
            (vec![(LOCK_MAX_WAIT, "5000")], LOCK_MAX_WAIT, 5),
        ] {
            let error = lock_limits(&pairs, timeout).unwrap_err().to_string();
            assert!(error.starts_with(&format!("{name}: ")), "{error}");
        }
        assert_eq!(
            lock_limits(&[(LOCK_MAX_WAIT, "5000")], 5).unwrap_err().to_string(),
            "ENCORE_COMMAND_LOCK_MAX_WAIT_MS: must be below OUTBOX_HANDLER_TIMEOUT_SECS"
        );
    }

    fn projection(pairs: &[(&'static str, &str)]) -> Result<ProcessorConfig, ConfigError> {
        let pairs: std::collections::HashMap<_, _> = pairs.iter().map(|(k, v)| (*k, v.to_string())).collect();
        projection_config(|name| pairs.get(name).cloned())
    }

    #[test]
    fn the_projection_processors_default_to_the_library_and_read_every_variable() {
        assert_eq!(
            projection(&[]).unwrap(),
            ProcessorConfig::default().with_settle(DEFAULT_PROJECTION_SETTLE).unwrap(),
            "the library's defaults, but a settle of the demo's own"
        );
        assert_eq!(projection(&[(PROJECTION_SETTLE, "0")]).unwrap(), ProcessorConfig::default());
        let config = projection(&[
            (PROJECTION_POLL, "100"),
            (PROJECTION_PAGE, "50"),
            (PROJECTION_LEASE, "2000"),
            (PROJECTION_RETRY_BASE, "100"),
            (PROJECTION_RETRY_MAX, "400"),
            (PROJECTION_CONCURRENCY, "4"),
            (PROJECTION_BUFFER, "8"),
            (PROJECTION_MAX_BATCH, "2"),
            (PROJECTION_REBUILD_PAGE, "10"),
            (PROJECTION_SETTLE, "30"),
        ])
        .unwrap();
        assert_eq!(
            (config.poll_interval(), config.page_size(), config.lease()),
            (Duration::from_millis(100), 50, Duration::from_secs(2))
        );
        assert_eq!(
            (
                config.retry_base(),
                config.retry_max(),
                config.concurrency(),
                config.rebuild_page(),
                config.settle()
            ),
            (
                Duration::from_millis(100),
                Duration::from_millis(400),
                4,
                10,
                Duration::from_millis(30)
            )
        );
        assert_eq!(config.delivery_limits(), DeliveryLimits::new(8, 2).unwrap());
    }

    #[test]
    fn the_projection_processors_name_the_variable_they_reject() {
        for (pairs, name) in [
            (vec![(PROJECTION_POLL, "x")], PROJECTION_POLL),
            (vec![(PROJECTION_POLL, "0")], PROJECTION_POLL),
            (vec![(PROJECTION_PAGE, "0")], PROJECTION_PAGE),
            (vec![(PROJECTION_LEASE, "999")], PROJECTION_LEASE),
            (vec![(PROJECTION_RETRY_BASE, "0")], PROJECTION_RETRY_BASE),
            (vec![(PROJECTION_RETRY_BASE, "20000")], PROJECTION_RETRY_BASE),
            (vec![(PROJECTION_CONCURRENCY, "0")], PROJECTION_CONCURRENCY),
            (vec![(PROJECTION_BUFFER, "0")], PROJECTION_BUFFER),
            (vec![(PROJECTION_BUFFER, "1")], PROJECTION_BUFFER),
            (vec![(PROJECTION_MAX_BATCH, "0")], PROJECTION_MAX_BATCH),
            (vec![(PROJECTION_REBUILD_PAGE, "0")], PROJECTION_REBUILD_PAGE),
            (vec![(PROJECTION_SETTLE, "x")], PROJECTION_SETTLE),
            (vec![(PROJECTION_SETTLE, "251")], PROJECTION_SETTLE),
        ] {
            let error = projection(&pairs).unwrap_err().to_string();
            assert!(error.starts_with(&format!("{name}: ")), "{pairs:?}: {error}");
        }
        assert_eq!(
            projection(&[(PROJECTION_RETRY_BASE, "0")]).unwrap_err().to_string(),
            "ENCORE_PROJECTION_RETRY_BASE_MS: must be positive"
        );
        assert_eq!(
            projection(&[(PROJECTION_BUFFER, "1")]).unwrap_err().to_string(),
            "ENCORE_PROJECTION_BUFFER: must be at least 2"
        );
        assert_eq!(
            projection(&[(PROJECTION_LEASE, "999")]).unwrap_err().to_string(),
            "ENCORE_PROJECTION_LEASE_MS: must be at least 1000 ms"
        );
    }

    fn settings(pairs: &[(&'static str, &str)]) -> Result<ReadSideSettings, ConfigError> {
        let pairs: std::collections::HashMap<_, _> = pairs.iter().map(|(k, v)| (*k, v.to_string())).collect();
        read_side(|name| pairs.get(name).cloned())
    }

    #[test]
    fn the_read_side_defaults_to_the_library_cap_and_names_the_variable_it_rejects() {
        assert_eq!(
            settings(&[]).unwrap(),
            ReadSideSettings {
                lag_cap: DEFAULT_COUNT_AFTER_CAP,
                status_timeout: DEFAULT_STATUS_TIMEOUT,
                slow_delay: DEFAULT_SLOW_PACE,
                max_pace: DEFAULT_MAX_PACE,
            }
        );
        assert_eq!(settings(&[(SLOW_DELAY, "0")]).unwrap().slow_delay, Duration::ZERO);
        for (pairs, name) in [
            (vec![(LAG_CAP, "0")], LAG_CAP),
            (vec![(LAG_CAP, "-1")], LAG_CAP),
            (vec![(STATUS_TIMEOUT, "0")], STATUS_TIMEOUT),
            (vec![(SLOW_DELAY, "x")], SLOW_DELAY),
            (vec![(SLOW_DELAY, "500"), (MAX_PACE, "100")], SLOW_DELAY),
            (vec![(MAX_PACE, "x")], MAX_PACE),
        ] {
            let error = settings(&pairs).unwrap_err().to_string();
            assert!(error.starts_with(&format!("{name}: ")), "{pairs:?}: {error}");
        }
    }

    #[test]
    fn outbox_defaults_satisfy_the_lease_budget() {
        let runtime = TaskRuntimeConfig::from_lookup(outbox_default).expect("demo OUTBOX_* defaults must validate");
        assert_eq!(runtime.worker_pool().concurrency(), 2);
        assert_eq!(runtime.worker_pool().lease_duration(), Duration::from_secs(30));
        assert_eq!(runtime.worker_pool().handler_timeout(), Duration::from_secs(5));
    }

    #[test]
    fn the_mail_request_gives_up_a_second_before_the_handler_timeout_but_never_below_a_second() {
        let mut config = crate::testing::config();
        assert_eq!(config.mail_request_timeout(), Duration::from_secs(4));
        config.task_runtime = TaskRuntimeConfig::from_lookup(|name| match name {
            "OUTBOX_HANDLER_TIMEOUT_SECS" => Some("1".to_owned()),
            other => outbox_default(other),
        })
        .expect("a one-second handler timeout validates");
        assert_eq!(config.mail_request_timeout(), Duration::from_secs(1));
    }
}
