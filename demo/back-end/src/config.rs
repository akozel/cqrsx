//! Process configuration from environment variables (see `demo/.env.example`).
//!
//! Parsing happens once at start-up, so this is the one place where a hard failure with a clear
//! message is acceptable; everything after `Config::from_env` is free of `unwrap`/`expect`.
use crate::operations::application::rebuild::{DEFAULT_MAX_PACE, DEFAULT_SLOW_PACE, DEFAULT_STATUS_TIMEOUT};
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
    postgres::{
        event_repository::DEFAULT_COUNT_AFTER_CAP,
        pool::{DEFAULT_STATEMENT_TIMEOUT, StatementTimeout},
    },
    task::{
        queue::lease::LEASE_SAFETY_MARGIN,
        runtime::config::{TaskRuntimeConfig, TaskRuntimeConfigError},
    },
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
    /// Connections of the MongoDB client (`ENCORE_MONGODB_POOL_SIZE`): the read models' documents, the
    /// processors' checkpoints and leases, and the HTTP reads of halls and wallets share it.
    pub mongodb_pool_size: NonZeroU32,
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

fn parse<T: std::str::FromStr>(
    lookup: &impl Fn(&'static str) -> Option<String>,
    name: &'static str,
    default: &str,
) -> Result<T, ConfigError>
where
    T::Err: std::fmt::Display,
{
    parse_value(name, &lookup(name).unwrap_or_else(|| default.to_owned()))
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

const MONGODB_POOL_SIZE: &str = "ENCORE_MONGODB_POOL_SIZE";
/// The demo's MongoDB client pool (`ENCORE_MONGODB_POOL_SIZE`), the driver's own default. Measured in epic 04/06
/// with two projection processors writing up to 16 documents each at once: 32 or 64 connections take no more
/// announces or bookings a second than 10, and cost MongoDB 11–17 % more CPU per command.
pub const DEFAULT_MONGODB_POOL_SIZE: NonZeroU32 = NonZeroU32::new(10).unwrap();

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

const STATEMENT_TIMEOUT: &str = "ENCORE_DATABASE_STATEMENT_TIMEOUT_MS";

/// Unset = `StatementTimeout::Default`, `0` = `Unlimited`, any other number of milliseconds = `Custom`.
fn statement_timeout(value: Option<String>) -> Result<StatementTimeout, ConfigError> {
    match value.map(|value| value.parse::<u32>()) {
        None => Ok(StatementTimeout::Default),
        Some(Ok(0)) => Ok(StatementTimeout::Unlimited),
        Some(Ok(millis)) if millis <= i32::MAX as u32 => Ok(StatementTimeout::Custom(Duration::from_millis(millis.into()))),
        Some(_) => Err(invalid(
            STATEMENT_TIMEOUT,
            "must be 0 (no cap) or 1..=2147483647 milliseconds",
        )),
    }
}

const TASK_MAX_ATTEMPTS: &str = "ENCORE_TASK_MAX_ATTEMPTS";
const TASK_RETRY_BASE: &str = "ENCORE_TASK_RETRY_BASE_MS";
const TASK_RETRY_MAX: &str = "ENCORE_TASK_RETRY_MAX_MS";
const TICKET_LIMIT: &str = "ENCORE_TICKET_LIMIT";
const SNAPSHOT_EVERY: &str = "ENCORE_SNAPSHOT_EVERY";
const DATABASE_URL: &str = "ENCORE_DATABASE_URL";
const DATABASE_POOL_SIZE: &str = "ENCORE_DATABASE_POOL_SIZE";
const DATABASE_HISTORY_POOL_SIZE: &str = "ENCORE_DATABASE_HISTORY_POOL_SIZE";
const DATABASE_PROJECTION_POOL_SIZE: &str = "ENCORE_DATABASE_PROJECTION_POOL_SIZE";
const MONGODB_URI: &str = "ENCORE_MONGODB_URI";
const READ_DB: &str = "ENCORE_READ_DB";
const HTTP_ADDR: &str = "ENCORE_HTTP_ADDR";
const MAIL_SERVICE_URL: &str = "ENCORE_MAIL_SERVICE_URL";
const NODE_NAME: &str = "ENCORE_NODE_NAME";

/// A duration as the description of the settings writes it: whole seconds as `N s` (zero too), otherwise whole
/// milliseconds as `N ms`, otherwise `N ns`.
pub fn duration_text(duration: Duration) -> String {
    if duration.subsec_nanos() == 0 {
        format!("{} s", duration.as_secs())
    } else if duration.subsec_nanos().is_multiple_of(1_000_000) {
        format!("{} ms", duration.as_millis())
    } else {
        format!("{} ns", duration.as_nanos())
    }
}

/// The cap a statement timeout puts on a statement (`Default` is the library's 4 s), or `unlimited`.
pub fn statement_timeout_text(timeout: StatementTimeout) -> String {
    match timeout {
        StatementTimeout::Default => duration_text(DEFAULT_STATEMENT_TIMEOUT),
        StatementTimeout::Custom(timeout) => duration_text(timeout),
        StatementTimeout::Unlimited => "unlimited".to_owned(),
    }
}

/// What the task lease must exceed, the bound the library checks at start-up
/// (`TaskWorkerPoolConfig::validate_for_storage_timeout`): a claim and a settlement within the storage timeout S, the
/// handler timeout H, the safety margin, and every wave of the buffer ahead of a delivery, ⌈Q/C⌉ · (H + S).
fn lease_budget(runtime: &TaskRuntimeConfig) -> Duration {
    let worker = runtime.worker_pool();
    let storage = runtime.storage().storage_timeout();
    let handler = worker.handler_timeout();
    let waves = u32::try_from(worker.buffer_capacity().div_ceil(worker.concurrency())).unwrap_or(u32::MAX);
    storage
        .saturating_mul(2)
        .saturating_add(handler)
        .saturating_add(LEASE_SAFETY_MARGIN)
        .saturating_add(handler.saturating_add(storage).saturating_mul(waves))
}

fn invalid(name: &'static str, message: impl Into<String>) -> ConfigError {
    ConfigError::Invalid {
        name,
        message: message.into(),
    }
}

impl Config {
    /// The configuration from the process environment.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    /// The configuration from `lookup` instead of the process environment: an `ENCORE_*` value is trimmed and counts as
    /// unset when empty; an `OUTBOX_*` value goes to the library as it is, with the demo's default when absent.
    /// `Config::from_lookup(|_| None)` is the configuration without a single variable.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let var = |name: &'static str| lookup(name).map(|v| v.trim().to_owned()).filter(|v| !v.is_empty());
        let task_runtime = TaskRuntimeConfig::from_lookup(|name| lookup(name).or_else(|| outbox_default(name)))?;
        let task_max_attempts: i32 = parse(&var, TASK_MAX_ATTEMPTS, "6")?;
        if task_max_attempts <= 0 {
            return Err(invalid(TASK_MAX_ATTEMPTS, "must be positive"));
        }
        let task_retry_base = Duration::from_millis(parse(&var, TASK_RETRY_BASE, "1000")?);
        let task_retry_max = Duration::from_millis(parse(&var, TASK_RETRY_MAX, "8000")?);
        if task_retry_base > task_retry_max {
            return Err(invalid(TASK_RETRY_BASE, "must not exceed ENCORE_TASK_RETRY_MAX_MS"));
        }
        let ticket_limit: usize = parse(&var, TICKET_LIMIT, "4")?;
        if ticket_limit == 0 {
            return Err(invalid(TICKET_LIMIT, "must be positive"));
        }
        let snapshot_every: usize = parse(&var, SNAPSHOT_EVERY, "10")?;
        if snapshot_every == 0 {
            return Err(invalid(SNAPSHOT_EVERY, "must be positive"));
        }
        // `NonZeroU32` rejects 0 while parsing.
        let database_pool_size: NonZeroU32 = parse(&var, DATABASE_POOL_SIZE, "10")?;
        let database_history_pool_size: NonZeroU32 = parse(&var, DATABASE_HISTORY_POOL_SIZE, "1")?;
        let database_projection_pool_size: NonZeroU32 = parse(&var, DATABASE_PROJECTION_POOL_SIZE, "4")?;
        let database_statement_timeout = statement_timeout(var(STATEMENT_TIMEOUT))?;
        let command_lock_limits = command_lock_limits(var, task_runtime.worker_pool().handler_timeout())?;
        let projection = projection_config(var)?;
        let read_side = read_side(var)?;
        Ok(Self {
            database_url: var(DATABASE_URL).unwrap_or_else(|| "postgresql://cqrsx:cqrsx@localhost:5434/encore".into()),
            database_pool_size,
            database_statement_timeout,
            database_history_pool_size,
            database_projection_pool_size,
            mongodb_uri: var(MONGODB_URI).unwrap_or_else(|| "mongodb://localhost:27018".into()),
            mongodb_pool_size: number(&var, MONGODB_POOL_SIZE, DEFAULT_MONGODB_POOL_SIZE)?,
            read_db: var(READ_DB).unwrap_or_else(|| "encore_read".into()),
            http_addr: parse(&var, HTTP_ADDR, "127.0.0.1:8080")?,
            mail_service_url: var(MAIL_SERVICE_URL)
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
            node_name: var(NODE_NAME).unwrap_or_else(|| DEFAULT_NODE_NAME.into()),
            replay_slow_delay: read_side.slow_delay,
            replay_max_pace: read_side.max_pace,
            task_runtime,
        })
    }

    /// Every setting as `(key, value)`, sorted by key. A setting read from a variable is described under the
    /// variable's name with the value the process uses, read back from the object built from it (`LockLimits`,
    /// `ProcessorConfig`, `TaskRuntimeConfig`); what has no variable of its own is described under a dotted name in
    /// lower case. Durations as [`duration_text`] writes them. `tests/golden/effective_config.txt` holds the description
    /// of the configuration without a single variable, next to the plan of the connections (`process::pool_plan`,
    /// `process::mongo_plan`).
    pub fn describe(&self) -> Vec<(String, String)> {
        let locks = &self.command_lock_limits;
        let projection = &self.projection;
        let delivery = projection.delivery_limits();
        let worker = self.task_runtime.worker_pool();
        let mut lines: Vec<(String, String)> = [
            (DATABASE_URL, self.database_url.clone()),
            (DATABASE_POOL_SIZE, self.database_pool_size.to_string()),
            (STATEMENT_TIMEOUT, statement_timeout_text(self.database_statement_timeout)),
            (DATABASE_HISTORY_POOL_SIZE, self.database_history_pool_size.to_string()),
            (DATABASE_PROJECTION_POOL_SIZE, self.database_projection_pool_size.to_string()),
            (MONGODB_URI, self.mongodb_uri.clone()),
            (MONGODB_POOL_SIZE, self.mongodb_pool_size.to_string()),
            (READ_DB, self.read_db.clone()),
            (HTTP_ADDR, self.http_addr.to_string()),
            (MAIL_SERVICE_URL, self.mail_service_url.clone()),
            (TASK_MAX_ATTEMPTS, self.task_max_attempts.to_string()),
            (TASK_RETRY_BASE, duration_text(self.task_retry_base)),
            (TASK_RETRY_MAX, duration_text(self.task_retry_max)),
            (TICKET_LIMIT, self.ticket_limit.to_string()),
            (SNAPSHOT_EVERY, self.snapshot_every.to_string()),
            (LOCK_MAX_WAIT, duration_text(locks.max_wait())),
            (LOCK_MAX_WAITERS, locks.max_waiters().to_string()),
            (PROJECTION_POLL, duration_text(projection.poll_interval())),
            (PROJECTION_PAGE, projection.page_size().to_string()),
            (PROJECTION_LEASE, duration_text(projection.lease())),
            (PROJECTION_RETRY_BASE, duration_text(projection.retry_base())),
            (PROJECTION_RETRY_MAX, duration_text(projection.retry_max())),
            (PROJECTION_CONCURRENCY, projection.concurrency().to_string()),
            (PROJECTION_BUFFER, delivery.capacity().to_string()),
            (PROJECTION_MAX_BATCH, delivery.max_batch().to_string()),
            (PROJECTION_REBUILD_PAGE, projection.rebuild_page().to_string()),
            (PROJECTION_SETTLE, duration_text(projection.settle())),
            ("projection.pages_in_flight", projection.pages_in_flight().to_string()),
            (LAG_CAP, self.projection_lag_cap.to_string()),
            (STATUS_TIMEOUT, duration_text(self.projection_status_timeout)),
            (NODE_NAME, self.node_name.clone()),
            (SLOW_DELAY, duration_text(self.replay_slow_delay)),
            (MAX_PACE, duration_text(self.replay_max_pace)),
            ("OUTBOX_WORKER_CONCURRENCY", worker.concurrency().to_string()),
            ("OUTBOX_QUEUE_CAPACITY", worker.buffer_capacity().to_string()),
            ("OUTBOX_QUEUE_REFILL_PERCENT", format!("{} %", worker.buffer_refill_percent())),
            ("OUTBOX_POLL_INTERVAL_MS", duration_text(worker.poll_interval())),
            ("OUTBOX_HANDLER_TIMEOUT_SECS", duration_text(worker.handler_timeout())),
            ("OUTBOX_LEASE_SECS", duration_text(worker.lease_duration())),
            (
                "OUTBOX_STORAGE_TIMEOUT_MS",
                duration_text(self.task_runtime.storage().storage_timeout()),
            ),
            ("outbox.lease_budget", duration_text(lease_budget(&self.task_runtime))),
            ("mail.request_timeout", duration_text(self.mail_request_timeout())),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect();
        lines.sort();
        lines
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
        let unset = |_: &'static str| None;
        assert_eq!(parse::<NonZeroU32>(&unset, DATABASE_POOL_SIZE, "10").unwrap().get(), 10);
        let error = parse::<NonZeroU32>(&unset, DATABASE_POOL_SIZE, "0").unwrap_err();
        assert!(error.to_string().contains(DATABASE_POOL_SIZE), "{error}");
    }

    fn from_pairs(pairs: &[(&str, &str)]) -> Result<Config, ConfigError> {
        let pairs: std::collections::HashMap<String, String> =
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        Config::from_lookup(|name| pairs.get(name).cloned())
    }

    #[test]
    fn from_lookup_trims_encore_variables_and_passes_outbox_variables_as_they_are() {
        let config = from_pairs(&[
            (SNAPSHOT_EVERY, " 25 "),
            (NODE_NAME, "   "),
            (MAIL_SERVICE_URL, "http://mail:8025/"),
            ("OUTBOX_WORKER_CONCURRENCY", "4"),
            ("OUTBOX_QUEUE_CAPACITY", "4"),
        ])
        .unwrap();
        assert_eq!(config.snapshot_every, 25, "an ENCORE_* value is trimmed");
        assert_eq!(config.node_name, DEFAULT_NODE_NAME, "a blank ENCORE_* value counts as unset");
        assert_eq!(config.mail_service_url, "http://mail:8025", "without the trailing slash");
        assert_eq!(config.task_runtime.worker_pool().concurrency(), 4);
        assert_eq!(
            config.task_runtime.worker_pool().lease_duration(),
            Duration::from_secs(30),
            "an absent OUTBOX_* variable takes the demo's default"
        );
        for (name, raw) in [("OUTBOX_LEASE_SECS", ""), ("OUTBOX_LEASE_SECS", " 30")] {
            assert!(
                matches!(from_pairs(&[(name, raw)]), Err(ConfigError::TaskRuntime(_))),
                "an OUTBOX_* value reaches the library untrimmed: {raw:?}"
            );
        }
        let error = from_pairs(&[(TICKET_LIMIT, "0")]).unwrap_err().to_string();
        assert_eq!(error, "ENCORE_TICKET_LIMIT: must be positive");
    }

    #[test]
    fn durations_are_whole_seconds_or_milliseconds() {
        assert_eq!(duration_text(Duration::ZERO), "0 s");
        assert_eq!(duration_text(Duration::from_secs(30)), "30 s");
        assert_eq!(duration_text(Duration::from_millis(10_000)), "10 s");
        assert_eq!(duration_text(Duration::from_millis(250)), "250 ms");
        assert_eq!(duration_text(Duration::from_millis(1500)), "1500 ms");
        assert_eq!(duration_text(Duration::from_micros(1500)), "1500000 ns");
        assert_eq!(statement_timeout_text(StatementTimeout::Default), "4 s");
        assert_eq!(
            statement_timeout_text(StatementTimeout::Custom(Duration::from_millis(2500))),
            "2500 ms"
        );
        assert_eq!(statement_timeout_text(StatementTimeout::Unlimited), "unlimited");
    }

    #[test]
    fn the_description_names_every_setting_once_in_its_unit() {
        let config = from_pairs(&[
            (STATEMENT_TIMEOUT, "0"),
            (PROJECTION_SETTLE, "0"),
            ("OUTBOX_LEASE_SECS", "45"),
        ])
        .unwrap();
        let lines = config.describe();
        let keys: std::collections::BTreeSet<&str> = lines.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(keys.len(), lines.len(), "every key once");
        assert!(lines.is_sorted(), "sorted by key");
        let value = |key: &str| lines.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());
        assert_eq!(value(STATEMENT_TIMEOUT), Some("unlimited"));
        assert_eq!(value(PROJECTION_SETTLE), Some("0 s"));
        assert_eq!(value("OUTBOX_LEASE_SECS"), Some("45 s"));
        assert_eq!(value("OUTBOX_QUEUE_REFILL_PERCENT"), Some("25 %"));
        assert_eq!(value("projection.pages_in_flight"), Some("63"));
    }

    #[test]
    fn the_lease_budget_is_the_bound_the_runtime_checks() {
        use cqrsx::task::runtime::worker_pool_config::{TaskWorkerPoolConfig, TaskWorkerPoolConfigError};
        let wider = |name: &str| match name {
            "OUTBOX_WORKER_CONCURRENCY" => Some("2".to_owned()),
            "OUTBOX_QUEUE_CAPACITY" => Some("5".to_owned()),
            "OUTBOX_HANDLER_TIMEOUT_SECS" => Some("2".to_owned()),
            "OUTBOX_STORAGE_TIMEOUT_MS" => Some("1500".to_owned()),
            "OUTBOX_LEASE_SECS" => Some("60".to_owned()),
            _ => None,
        };
        let demo = TaskRuntimeConfig::from_lookup(outbox_default).unwrap();
        assert_eq!(lease_budget(&demo), Duration::from_secs(24), "2·3 + 5 + 5 + 1·(5 + 3) s");
        let wider = TaskRuntimeConfig::from_lookup(wider).unwrap();
        assert_eq!(
            lease_budget(&wider),
            Duration::from_millis(20_500),
            "2·1.5 + 2 + 5 + 3·(2 + 1.5) s"
        );
        for runtime in [demo, wider] {
            let worker = runtime.worker_pool();
            let with_lease = |lease| {
                TaskWorkerPoolConfig::new(
                    worker.concurrency(),
                    worker.buffer_capacity(),
                    worker.poll_interval(),
                    worker.handler_timeout(),
                    lease,
                )
                .unwrap()
                .validate_for_storage_timeout(runtime.storage().storage_timeout())
            };
            let budget = lease_budget(&runtime);
            assert_eq!(with_lease(budget), Err(TaskWorkerPoolConfigError::LeaseTooShort));
            assert_eq!(with_lease(budget + Duration::from_millis(1)), Ok(()));
        }
    }

    #[test]
    fn the_mongodb_pool_takes_the_demo_default_and_rejects_zero() {
        let unset = |_: &'static str| None;
        assert_eq!(
            number(&unset, MONGODB_POOL_SIZE, DEFAULT_MONGODB_POOL_SIZE).unwrap(),
            DEFAULT_MONGODB_POOL_SIZE
        );
        let set = |raw: &'static str| move |_: &'static str| Some(raw.to_owned());
        assert_eq!(
            number(&set("64"), MONGODB_POOL_SIZE, DEFAULT_MONGODB_POOL_SIZE)
                .unwrap()
                .get(),
            64
        );
        let error = number(&set("0"), MONGODB_POOL_SIZE, DEFAULT_MONGODB_POOL_SIZE).unwrap_err();
        assert!(error.to_string().contains(MONGODB_POOL_SIZE), "{error}");
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
