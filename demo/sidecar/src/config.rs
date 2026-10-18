//! Sidecar configuration (`SIDECAR_*`, `ENCORE_READ_DB`, `ENCORE_MAIL_DB`, `RUST_LOG`). Parsed once
//! at start-up.
use std::{env, net::SocketAddr, time::Duration};
use thiserror::Error;

#[derive(Debug, Error)]
#[error("{name}: {message}")]
pub struct ConfigError {
    name: &'static str,
    message: String,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    /// DEMO ONLY: the connection that may TRUNCATE the three tables (role `encore_demo_admin`).
    pub admin_database_url: String,
    /// The back-end whose public API announces the default concerts (seed and reset).
    pub backend_url: String,
    /// Announce the default concerts at start-up (through the back-end API, with retries).
    pub seed: bool,
    /// Expose `POST /inspect/reset`.
    pub reset_enabled: bool,
    pub mongodb_uri: String,
    pub read_db: String,
    pub mail_db: String,
    pub http_addr: SocketAddr,
    pub poll_interval: Duration,
    /// DEMO ONLY: the external mail service hosted here (`POST /mail`) — the chance that a delivery
    /// is refused (and the back-end's task retried), the simulated network latency, and an optional
    /// seed that makes the dice deterministic. The latency must stay below the back-end's mail
    /// request timeout (its `OUTBOX_HANDLER_TIMEOUT_SECS` minus one second: 4 s by default).
    pub mail_failure_rate: f64,
    pub mail_latency: (Duration, Duration),
    pub mail_chaos_seed: Option<u64>,
}

fn var(name: &str) -> Option<String> {
    env::var(name).ok().map(|v| v.trim().to_owned()).filter(|v| !v.is_empty())
}

fn parse<T: std::str::FromStr>(name: &'static str, default: &str) -> Result<T, ConfigError>
where
    T::Err: std::fmt::Display,
{
    let raw = var(name).unwrap_or_else(|| default.to_owned());
    raw.parse().map_err(|error: T::Err| ConfigError {
        name,
        message: format!("cannot parse {raw:?}: {error}"),
    })
}

fn invalid(name: &'static str, message: impl Into<String>) -> ConfigError {
    ConfigError {
        name,
        message: message.into(),
    }
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let poll_ms: u64 = parse("SIDECAR_POLL_MS", "400")?;
        if poll_ms == 0 {
            return Err(invalid("SIDECAR_POLL_MS", "must be positive"));
        }
        let mail_failure_rate: f64 = parse("SIDECAR_MAIL_FAILURE_RATE", "0.2")?;
        if !(0.0..=1.0).contains(&mail_failure_rate) {
            return Err(invalid("SIDECAR_MAIL_FAILURE_RATE", "must be between 0.0 and 1.0"));
        }
        let latency_min = Duration::from_millis(parse("SIDECAR_MAIL_LATENCY_MIN_MS", "600")?);
        let latency_max = Duration::from_millis(parse("SIDECAR_MAIL_LATENCY_MAX_MS", "1800")?);
        if latency_min > latency_max {
            return Err(invalid(
                "SIDECAR_MAIL_LATENCY_MIN_MS",
                "must not exceed SIDECAR_MAIL_LATENCY_MAX_MS",
            ));
        }
        // Empty or absent = random dice; any value must be a seed.
        let mail_chaos_seed = var("SIDECAR_MAIL_CHAOS_SEED")
            .map(|_| parse("SIDECAR_MAIL_CHAOS_SEED", "0"))
            .transpose()?;
        Ok(Self {
            database_url: var("SIDECAR_DATABASE_URL")
                .unwrap_or_else(|| "postgresql://encore_inspector:inspector@localhost:5434/encore".into()),
            admin_database_url: var("SIDECAR_ADMIN_DATABASE_URL")
                .unwrap_or_else(|| "postgresql://encore_demo_admin:demo-admin@localhost:5434/encore".into()),
            backend_url: var("SIDECAR_BACKEND_URL").unwrap_or_else(|| "http://127.0.0.1:8080".into()),
            seed: parse("SIDECAR_SEED", "true")?,
            reset_enabled: parse("SIDECAR_RESET_ENABLED", "true")?,
            mongodb_uri: var("SIDECAR_MONGODB_URI").unwrap_or_else(|| "mongodb://localhost:27018".into()),
            read_db: var("ENCORE_READ_DB").unwrap_or_else(|| "encore_read".into()),
            mail_db: var("ENCORE_MAIL_DB").unwrap_or_else(|| "mail_service".into()),
            http_addr: parse("SIDECAR_HTTP_ADDR", "127.0.0.1:8090")?,
            poll_interval: Duration::from_millis(poll_ms),
            mail_failure_rate,
            mail_latency: (latency_min, latency_max),
            mail_chaos_seed,
        })
    }
}
