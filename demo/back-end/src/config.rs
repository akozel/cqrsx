//! Process configuration from environment variables (see `demo/.env.example`).
//!
//! Parsing happens once at start-up, so this is the one place where a hard failure with a clear
//! message is acceptable; everything after `Config::from_env` is free of `unwrap`/`expect`.
use cqrsx::{
    postgres::task_queue::{PostgresTaskQueueConfig, PostgresTaskQueueConfigError},
    task::runtime::config::{TaskRuntimeConfig, TaskRuntimeConfigError},
};
use std::{env, net::SocketAddr, time::Duration};
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
    pub mongodb_uri: String,
    pub read_db: String,
    pub mail_db: String,
    pub http_addr: SocketAddr,
    /// DEMO ONLY: probability that a mail task "fails" and is retried.
    pub task_failure_rate: f64,
    pub task_latency: (Duration, Duration),
    pub task_max_attempts: i32,
    /// Chain tasks (`encore_record_*`) have no chaos; their budget only matters while a rebuild
    /// holds the box-office lock, so it is deliberately generous.
    pub chain_max_attempts: i32,
    pub task_retry_base: Duration,
    pub task_retry_max: Duration,
    pub chaos_seed: Option<u64>,
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
        let task_failure_rate: f64 = parse("ENCORE_TASK_FAILURE_RATE", "0.2")?;
        if !(0.0..=1.0).contains(&task_failure_rate) {
            return Err(invalid("ENCORE_TASK_FAILURE_RATE", "must be between 0.0 and 1.0"));
        }
        let latency_min = Duration::from_millis(parse("ENCORE_TASK_LATENCY_MIN_MS", "600")?);
        let latency_max = Duration::from_millis(parse("ENCORE_TASK_LATENCY_MAX_MS", "1800")?);
        if latency_min > latency_max {
            return Err(invalid(
                "ENCORE_TASK_LATENCY_MIN_MS",
                "must not exceed ENCORE_TASK_LATENCY_MAX_MS",
            ));
        }
        let handler_timeout = task_runtime.worker_pool().handler_timeout();
        if latency_max >= handler_timeout {
            return Err(invalid(
                "ENCORE_TASK_LATENCY_MAX_MS",
                format!("must be below OUTBOX_HANDLER_TIMEOUT_SECS ({handler_timeout:?})"),
            ));
        }
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
        // Empty or absent = random chaos; any value must be a seed.
        let chaos_seed = var("ENCORE_CHAOS_SEED")
            .map(|_| parse("ENCORE_CHAOS_SEED", "0"))
            .transpose()?;
        let ticket_limit: usize = parse("ENCORE_TICKET_LIMIT", "4")?;
        if ticket_limit == 0 {
            return Err(invalid("ENCORE_TICKET_LIMIT", "must be positive"));
        }
        let snapshot_every: usize = parse("ENCORE_SNAPSHOT_EVERY", "10")?;
        if snapshot_every == 0 {
            return Err(invalid("ENCORE_SNAPSHOT_EVERY", "must be positive"));
        }
        Ok(Self {
            database_url: var("ENCORE_DATABASE_URL").unwrap_or_else(|| "postgresql://cqrsx:cqrsx@localhost:5434/encore".into()),
            mongodb_uri: var("ENCORE_MONGODB_URI").unwrap_or_else(|| "mongodb://localhost:27018".into()),
            read_db: var("ENCORE_READ_DB").unwrap_or_else(|| "encore_read".into()),
            mail_db: var("ENCORE_MAIL_DB").unwrap_or_else(|| "mail_service".into()),
            http_addr: parse("ENCORE_HTTP_ADDR", "127.0.0.1:8080")?,
            task_failure_rate,
            task_latency: (latency_min, latency_max),
            task_max_attempts,
            chain_max_attempts,
            task_retry_base,
            task_retry_max,
            chaos_seed,
            ticket_limit,
            snapshot_every,
            replay_slow_delay: Duration::from_millis(parse("ENCORE_REPLAY_SLOW_DELAY_MS", "80")?),
            task_runtime,
            task_queue,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbox_defaults_satisfy_the_lease_budget_and_the_latency_budget() {
        let runtime = TaskRuntimeConfig::from_lookup(outbox_default).expect("demo OUTBOX_* defaults must validate");
        assert_eq!(runtime.worker_pool().concurrency(), 2);
        assert_eq!(runtime.worker_pool().lease_duration(), Duration::from_secs(30));
        assert_eq!(runtime.worker_pool().handler_timeout(), Duration::from_secs(5));
        assert!(Duration::from_millis(1800) < runtime.worker_pool().handler_timeout());
        PostgresTaskQueueConfig::from_lookup(outbox_default).expect("demo statement timeout must validate");
    }
}
