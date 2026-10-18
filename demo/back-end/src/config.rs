//! Process configuration from environment variables (see `demo/.env.example`).
//!
//! Parsing happens once at start-up, so this is the one place where a hard failure with a clear
//! message is acceptable; everything after `Config::from_env` is free of `unwrap`/`expect`.
use cqrsx::{
    postgres::task_queue::{PostgresTaskQueueConfig, PostgresTaskQueueConfigError},
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
    #[error("task queue configuration: {0}")]
    TaskQueue(#[from] PostgresTaskQueueConfigError),
}

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    /// Connections in the one PostgreSQL pool shared by HTTP, the outbox poller and the workers: the
    /// ceiling of concurrent database work of the process.
    pub database_pool_size: NonZeroU32,
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
    pub replay_slow_delay: Duration,
    /// The library's own `OUTBOX_*` keys: worker pool and storage timeouts …
    pub task_runtime: TaskRuntimeConfig,
    /// … and the PostgreSQL statement timeout of the task queue.
    pub task_queue: PostgresTaskQueueConfig,
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
        "OUTBOX_POSTGRES_STATEMENT_TIMEOUT_MS" => "4000",
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
    let raw = var(name).unwrap_or_else(|| default.to_owned());
    raw.parse().map_err(|error: T::Err| ConfigError::Invalid {
        name,
        message: format!("cannot parse {raw:?}: {error}"),
    })
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
        let task_queue = PostgresTaskQueueConfig::from_lookup(outbox_lookup)?;
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
        Ok(Self {
            database_url: var("ENCORE_DATABASE_URL").unwrap_or_else(|| "postgresql://cqrsx:cqrsx@localhost:5434/encore".into()),
            database_pool_size,
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
            replay_slow_delay: Duration::from_millis(parse("ENCORE_REPLAY_SLOW_DELAY_MS", "80")?),
            task_runtime,
            task_queue,
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

    #[test]
    fn outbox_defaults_satisfy_the_lease_budget() {
        let runtime = TaskRuntimeConfig::from_lookup(outbox_default).expect("demo OUTBOX_* defaults must validate");
        assert_eq!(runtime.worker_pool().concurrency(), 2);
        assert_eq!(runtime.worker_pool().lease_duration(), Duration::from_secs(30));
        assert_eq!(runtime.worker_pool().handler_timeout(), Duration::from_secs(5));
        PostgresTaskQueueConfig::from_lookup(outbox_default).expect("demo statement timeout must validate");
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
