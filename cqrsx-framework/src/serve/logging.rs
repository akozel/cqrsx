//! Explicit process logger setup; importing this module installs nothing.

use std::fmt;
use thiserror::Error;
use tracing::level_filters::{LevelFilter, ParseLevelFilterError};
use tracing_subscriber::util::{SubscriberInitExt, TryInitError};

/// Process log level. Defaults and explicit construction do not read the environment.
#[derive(Clone, Debug)]
pub struct LoggingConfig {
    level_filter: LevelFilter,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self::new(LevelFilter::INFO)
    }
}

impl LoggingConfig {
    pub fn new(level_filter: LevelFilter) -> Self {
        Self { level_filter }
    }

    /// Preserve LevelFilter parsing, including case, numeric aliases and empty input.
    pub fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, ParseLevelFilterError> {
        Ok(Self::new(lookup("RUST_LOG").as_deref().unwrap_or("info").parse()?))
    }

    pub fn from_env() -> Result<Self, ParseLevelFilterError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    #[cfg(test)]
    pub fn level_filter(&self) -> LevelFilter {
        self.level_filter
    }

    /// Explicitly install the fmt subscriber and existing log compatibility bridge.
    /// Returns an error when a global subscriber or log logger is already installed.
    pub fn try_init(&self) -> Result<(), LoggingInitError> {
        tracing_subscriber::fmt()
            .with_target(false)
            .with_thread_names(true)
            .with_max_level(self.level_filter)
            .finish()
            .try_init()
            .map_err(|source| LoggingInitError { source })
    }
}

/// Preserves the initialization cause without formatting it implicitly.
#[derive(Error)]
#[error("unable to initialize process logger")]
pub struct LoggingInitError {
    #[source]
    source: TryInitError,
}

impl fmt::Debug for LoggingInitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parsing_preserves_defaults_levels_aliases_and_rejects_directives() {
        assert_eq!(LoggingConfig::default().level_filter(), LevelFilter::INFO);
        assert_eq!(
            LoggingConfig::from_lookup(|_| None).unwrap().level_filter(),
            LevelFilter::INFO
        );
        for (name, alias, expected) in [
            ("off", "0", LevelFilter::OFF),
            ("error", "1", LevelFilter::ERROR),
            ("warn", "2", LevelFilter::WARN),
            ("info", "3", LevelFilter::INFO),
            ("debug", "4", LevelFilter::DEBUG),
            ("trace", "5", LevelFilter::TRACE),
        ] {
            for value in [name.to_owned(), name.to_uppercase(), alias.to_owned()] {
                assert_eq!(
                    LoggingConfig::from_lookup(|key| {
                        assert_eq!(key, "RUST_LOG");
                        Some(value.clone())
                    })
                    .unwrap()
                    .level_filter(),
                    expected
                );
            }
        }
        assert_eq!(
            LoggingConfig::from_lookup(|_| Some(String::new())).unwrap().level_filter(),
            LevelFilter::ERROR
        );
        for value in ["invalid", "6", "-1", " info ", "cqrsx=debug", "info,sqlx=warn"] {
            assert!(LoggingConfig::from_lookup(|_| Some(value.into())).is_err());
        }
    }

    // The process logger can be installed once and never removed: this test runs alone in a process of its own.
    #[test]
    fn logger_installation_is_explicit_and_duplicate_installation_is_fallible() {
        use crate::test_support;
        use std::error::Error;

        if test_support::scenario().is_some() {
            assert!(!tracing::dispatcher::has_been_set());
            let expected = std::env::var("RUST_LOG")
                .ok()
                .unwrap_or_else(|| "info".into())
                .parse::<tracing::level_filters::LevelFilter>();
            match (LoggingConfig::from_env(), expected) {
                (Ok(actual), Ok(expected)) => assert_eq!(actual.level_filter(), expected),
                (Err(_), Err(_)) => {}
                _ => panic!("from_env must retain level parsing"),
            }
            let config = LoggingConfig::from_lookup(|_| None).unwrap();
            assert!(!tracing::dispatcher::has_been_set());
            config.try_init().unwrap();
            assert!(tracing::dispatcher::has_been_set());
            let error = config.try_init().unwrap_err();
            assert!(error.source().is_some());
            assert_eq!(error.to_string(), "unable to initialize process logger");
            assert_eq!(format!("{error:?}"), error.to_string());
            return;
        }
        test_support::isolated(
            concat!(
                module_path!(),
                "::logger_installation_is_explicit_and_duplicate_installation_is_fallible"
            ),
            "logger",
            |_| {},
        );
    }
}
