//! Sidecar configuration (`SIDECAR_*`, `RUST_LOG`). Parsed once at start-up.
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
    pub mongodb_uri: String,
    pub read_db: String,
    pub mail_db: String,
    pub http_addr: SocketAddr,
    pub poll_interval: Duration,
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

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let poll_ms: u64 = parse("SIDECAR_POLL_MS", "400")?;
        if poll_ms == 0 {
            return Err(ConfigError {
                name: "SIDECAR_POLL_MS",
                message: "must be positive".into(),
            });
        }
        Ok(Self {
            database_url: var("SIDECAR_DATABASE_URL")
                .unwrap_or_else(|| "postgresql://encore_inspector:inspector@localhost:5434/encore".into()),
            mongodb_uri: var("SIDECAR_MONGODB_URI").unwrap_or_else(|| "mongodb://localhost:27018".into()),
            read_db: var("ENCORE_READ_DB").unwrap_or_else(|| "encore_read".into()),
            mail_db: var("ENCORE_MAIL_DB").unwrap_or_else(|| "mail_service".into()),
            http_addr: parse("SIDECAR_HTTP_ADDR", "127.0.0.1:8090")?,
            poll_interval: Duration::from_millis(poll_ms),
        })
    }
}
