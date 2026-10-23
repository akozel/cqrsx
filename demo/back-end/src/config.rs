//! Process configuration from environment variables (see `demo/.env.example`).
//!
//! Parsing happens once at start-up, so this is the one place where a hard failure with a clear
//! message is acceptable; everything after `Config::from_env` is free of `unwrap`/`expect`.
use cqrsx::{
    cqrs::{
        command::locks::{DEFAULT_LOCK_MAX_WAIT, DEFAULT_LOCK_MAX_WAITERS, LockLimits, LockLimitsError},
        event_handler::delivery::{DEFAULT_DELIVERY_CAPACITY, DEFAULT_DELIVERY_MAX_BATCH, DeliveryConfig, DeliveryConfigError},
    },
    postgres::pool::StatementTimeout,
    task::runtime::config::{TaskRuntimeConfig, TaskRuntimeConfigError},
};
use std::{env, net::SocketAddr, num::NonZeroU32, time::Duration};
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
    /// Connections of the history pool, which has no statement timeout: Encore!, verify, the ledger
    /// totals and the time machine read whole histories. It keeps no idle connection.
    pub database_history_pool_size: NonZeroU32,
    pub mongodb_uri: String,
    pub read_db: String,
    pub http_addr: SocketAddr,
    /// Base URL of the external mail service the e-mail tasks call (`POST {url}/mail`), without a
    /// trailing slash. The demo's provider is hosted by the sidecar.
    pub mail_service_url: String,
    /// Claim-attempt budget of the e-mail tasks.
    pub task_max_attempts: i32,
    /// Chain tasks (`encore_record_*`) talk to our own ledger; their budget only matters while a
    /// rebuild holds the box-office lock, so it is deliberately generous.
    pub chain_max_attempts: i32,
    /// Backoff an e-mail task asks for after a refused delivery: `min(base · 2^(n−1), max)`.
    pub task_retry_base: Duration,
    pub task_retry_max: Duration,
    pub ticket_limit: usize,
    pub snapshot_every: usize,
    /// How long, and how many, commands wait for one concert's or fan ledger's lock
    /// (`ENCORE_COMMAND_LOCK_MAX_WAIT_MS`, `ENCORE_COMMAND_LOCK_MAX_WAITERS`).
    pub command_lock_limits: LockLimits,
    /// The bounds of the ordered delivery that brings committed events to each read model
    /// (`ENCORE_PROJECTION_BUFFER`, `ENCORE_PROJECTION_MAX_BATCH`).
    pub projection_delivery: DeliveryConfig,
    pub replay_slow_delay: Duration,
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
/// these bound the wait for the lock (and, from the same arrival, for space in the projection's buffer) and
/// default to the library's. A chain task waits for the lock inside its handler, so the wait must end before
/// the handler times out.
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

const PROJECTION_BUFFER: &str = "ENCORE_PROJECTION_BUFFER";
const PROJECTION_MAX_BATCH: &str = "ENCORE_PROJECTION_MAX_BATCH";

/// A command hands its events to a buffer per concert or fan ledger and read model, applied in commit order and
/// in batches after the command has released its lock (backlog 02/08b, 08d). The bounds default to the
/// library's.
fn projection_delivery(lookup: impl Fn(&'static str) -> Option<String>) -> Result<DeliveryConfig, ConfigError> {
    let capacity = match lookup(PROJECTION_BUFFER) {
        Some(raw) => parse_value(PROJECTION_BUFFER, &raw)?,
        None => DEFAULT_DELIVERY_CAPACITY.get(),
    };
    let max_batch = match lookup(PROJECTION_MAX_BATCH) {
        Some(raw) => parse_value(PROJECTION_MAX_BATCH, &raw)?,
        None => DEFAULT_DELIVERY_MAX_BATCH.get(),
    };
    DeliveryConfig::new(capacity, max_batch).map_err(|error| match error {
        DeliveryConfigError::ZeroCapacity => invalid(PROJECTION_BUFFER, "must be positive"),
        DeliveryConfigError::ZeroMaxBatch => invalid(PROJECTION_MAX_BATCH, "must be positive"),
    })
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
        let chain_max_attempts: i32 = parse("ENCORE_CHAIN_MAX_ATTEMPTS", "60")?;
        if chain_max_attempts <= 0 {
            return Err(invalid("ENCORE_CHAIN_MAX_ATTEMPTS", "must be positive"));
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
        let database_statement_timeout = statement_timeout(var("ENCORE_DATABASE_STATEMENT_TIMEOUT_MS"))?;
        let command_lock_limits = command_lock_limits(var, task_runtime.worker_pool().handler_timeout())?;
        let projection_delivery = projection_delivery(var)?;
        Ok(Self {
            database_url: var("ENCORE_DATABASE_URL").unwrap_or_else(|| "postgresql://cqrsx:cqrsx@localhost:5434/encore".into()),
            database_pool_size,
            database_statement_timeout,
            database_history_pool_size,
            mongodb_uri: var("ENCORE_MONGODB_URI").unwrap_or_else(|| "mongodb://localhost:27018".into()),
            read_db: var("ENCORE_READ_DB").unwrap_or_else(|| "encore_read".into()),
            http_addr: parse("ENCORE_HTTP_ADDR", "127.0.0.1:8080")?,
            mail_service_url: var("ENCORE_MAIL_SERVICE_URL")
                .unwrap_or_else(|| "http://127.0.0.1:8090".into())
                .trim_end_matches('/')
                .to_owned(),
            task_max_attempts,
            chain_max_attempts,
            task_retry_base,
            task_retry_max,
            ticket_limit,
            snapshot_every,
            command_lock_limits,
            projection_delivery,
            replay_slow_delay: Duration::from_millis(parse("ENCORE_REPLAY_SLOW_DELAY_MS", "80")?),
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

    fn delivery(pairs: &[(&'static str, &str)]) -> Result<DeliveryConfig, ConfigError> {
        let pairs: std::collections::HashMap<_, _> = pairs.iter().map(|(k, v)| (*k, v.to_string())).collect();
        projection_delivery(|name| pairs.get(name).cloned())
    }

    #[test]
    fn projection_delivery_defaults_to_the_library_bounds() {
        assert_eq!(delivery(&[]).unwrap(), DeliveryConfig::default());
        assert_eq!(
            delivery(&[(PROJECTION_BUFFER, "8"), (PROJECTION_MAX_BATCH, "1")]).unwrap(),
            DeliveryConfig::new(8, 1).unwrap()
        );
    }

    #[test]
    fn projection_delivery_names_the_variable_it_rejects() {
        for (pairs, name) in [
            (vec![(PROJECTION_BUFFER, "x")], PROJECTION_BUFFER),
            (vec![(PROJECTION_BUFFER, "0")], PROJECTION_BUFFER),
            (vec![(PROJECTION_MAX_BATCH, "-1")], PROJECTION_MAX_BATCH),
            (vec![(PROJECTION_MAX_BATCH, "0")], PROJECTION_MAX_BATCH),
        ] {
            let error = delivery(&pairs).unwrap_err().to_string();
            assert!(error.starts_with(&format!("{name}: ")), "{error}");
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
