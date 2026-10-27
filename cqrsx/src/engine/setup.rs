//! What a module gets when the engine is built: its variables of the environment and the settings of the engine.
use crate::{
    cqrs::{
        command::{locks::LockLimits, retry::AggregateConflictRetryPolicy},
        projection::processor::config::ProcessorConfig,
    },
    task::queue::storage_config::TaskStorageConfig,
};
use std::{error::Error, fmt::Display, num::NonZeroUsize, str::FromStr, sync::Arc, time::Duration};
use thiserror::Error;

/// A snapshot of an aggregate every ten events.
pub const DEFAULT_SNAPSHOT_EVERY: NonZeroUsize = NonZeroUsize::new(10).unwrap();
/// How long a task's handler may run: the task runtime's default.
pub const DEFAULT_HANDLER_TIMEOUT: Duration = Duration::from_secs(15);
/// How long a claimed task is leased: the task runtime's default.
pub const DEFAULT_TASK_LEASE: Duration = Duration::from_secs(60);

/// What an engine builds its components with; the defaults are the library's. A module reads them
/// ([`Setup::engine`]), e.g. the handler's timeout for a request of its own.
#[derive(Clone, Debug)]
pub struct Settings {
    /// Every executor: a snapshot of an aggregate every this many events.
    pub snapshot_every: NonZeroUsize,
    pub conflict_retry: AggregateConflictRetryPolicy,
    pub lock_limits: LockLimits,
    /// Every projection processor.
    pub processor: ProcessorConfig,
    /// The owner this node writes on the checkpoints of its processors; `None`: a fresh one per engine.
    pub node_name: Option<String>,
    /// Every attempt of a task: how long its handler may run, the lease of its claim, the storage budget.
    pub handler_timeout: Duration,
    pub task_lease: Duration,
    pub task_storage: TaskStorageConfig,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            snapshot_every: DEFAULT_SNAPSHOT_EVERY,
            conflict_retry: AggregateConflictRetryPolicy::default(),
            lock_limits: LockLimits::default(),
            processor: ProcessorConfig::default(),
            node_name: None,
            handler_timeout: DEFAULT_HANDLER_TIMEOUT,
            task_lease: DEFAULT_TASK_LEASE,
            task_storage: TaskStorageConfig::default(),
        }
    }
}

/// Where the variables of the environment come from: a lookup by the variable's full name.
pub(crate) type Lookup = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// What a module root gets: its variables under the application's prefix ([`env`](Self::env)) and the engine's
/// settings ([`engine`](Self::engine)). No shared resources: a module makes what it needs and provides it.
pub struct Setup {
    prefix: String,
    lookup: Lookup,
    settings: Settings,
}

/// A variable of a module that cannot be read, or a resource the module could not make.
#[derive(Debug, Error)]
pub enum SetupError {
    #[error("{variable}: {message}")]
    Invalid { variable: String, message: String },
    #[error("{0}")]
    Failed(Box<dyn Error + Send + Sync>),
}

impl SetupError {
    /// A resource the module could not make, e.g. a client of its own.
    pub fn failed(error: impl Error + Send + Sync + 'static) -> Self {
        Self::Failed(Box::new(error))
    }
}

impl Setup {
    /// The setup of a module over `lookup` under `prefix`, in an engine with `settings`: what the engine's builder
    /// gives every module, and a test of a module root on its own.
    pub fn new(
        prefix: impl Into<String>,
        lookup: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
        settings: Settings,
    ) -> Self {
        Self::over(prefix.into(), Arc::new(lookup), settings)
    }

    pub(crate) fn over(prefix: String, lookup: Lookup, settings: Settings) -> Self {
        Self {
            prefix,
            lookup,
            settings,
        }
    }

    /// The variable `name` under the prefix (`ENCORE_TICKET_LIMIT` for `TICKET_LIMIT`), trimmed and parsed; `default`
    /// when it is not set or blank. A value that does not parse names the variable.
    pub fn env<T: FromStr>(&self, name: &str, default: T) -> Result<T, SetupError>
    where
        T::Err: Display,
    {
        let variable = match self.prefix.as_str() {
            "" => name.to_owned(),
            prefix => format!("{prefix}_{name}"),
        };
        match (self.lookup)(&variable).map(|value| value.trim().to_owned()) {
            Some(value) if !value.is_empty() => value.parse().map_err(|error: T::Err| SetupError::Invalid {
                message: format!("cannot parse {value:?}: {error}"),
                variable,
            }),
            _ => Ok(default),
        }
    }

    /// The variable `name` as milliseconds; `default_ms` when it is not set.
    pub fn millis(&self, name: &str, default_ms: u64) -> Result<Duration, SetupError> {
        self.env(name, default_ms).map(Duration::from_millis)
    }

    /// The settings of the engine the module is built into.
    pub fn engine(&self) -> &Settings {
        &self.settings
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cqrs::command::locks::{DEFAULT_LOCK_MAX_WAIT, DEFAULT_LOCK_MAX_WAITERS};
    use std::collections::HashMap;

    fn setup(prefix: &str, variables: &[(&str, &str)]) -> Setup {
        let variables: HashMap<String, String> = variables
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        Setup::new(prefix, move |name| variables.get(name).cloned(), Settings::default())
    }

    #[test]
    fn a_variable_is_read_under_the_prefix_and_a_blank_one_is_unset() {
        let encore = setup(
            "ENCORE",
            &[("ENCORE_TICKET_LIMIT", " 7 "), ("ENCORE_BLANK", "  "), ("TICKET_LIMIT", "9")],
        );
        assert_eq!(encore.env("TICKET_LIMIT", 4).unwrap(), 7);
        assert_eq!(encore.env("BLANK", 4).unwrap(), 4);
        assert_eq!(encore.env("ABSENT", String::from("http://mail")).unwrap(), "http://mail");
        let bare = setup("", &[("TICKET_LIMIT", "9"), ("RETRY_MS", "250")]);
        assert_eq!(bare.env("TICKET_LIMIT", 4).unwrap(), 9, "no prefix: the name as it is");
        assert_eq!(bare.millis("RETRY_MS", 1000).unwrap(), Duration::from_millis(250));
        assert_eq!(bare.millis("ABSENT_MS", 1000).unwrap(), Duration::from_secs(1));
        assert_eq!(bare.engine().snapshot_every, DEFAULT_SNAPSHOT_EVERY);
    }

    #[test]
    fn an_unreadable_value_names_its_variable() {
        let encore = setup("ENCORE", &[("ENCORE_TICKET_LIMIT", "four")]);
        let error = r#"ENCORE_TICKET_LIMIT: cannot parse "four": invalid digit found in string"#;
        assert_eq!(encore.env::<usize>("TICKET_LIMIT", 4).unwrap_err().to_string(), error);
        assert_eq!(encore.millis("TICKET_LIMIT", 1).unwrap_err().to_string(), error);
        let failed = SetupError::failed(std::io::Error::other("no client"));
        assert_eq!(failed.to_string(), "no client");
    }

    #[test]
    fn the_default_settings_are_the_library_constants() {
        let defaults = Settings::default();
        assert_eq!(
            (defaults.snapshot_every.get(), defaults.conflict_retry, defaults.lock_limits),
            (
                10,
                AggregateConflictRetryPolicy::default(),
                LockLimits::new(DEFAULT_LOCK_MAX_WAIT, DEFAULT_LOCK_MAX_WAITERS.get()).unwrap()
            )
        );
        assert_eq!(
            (
                defaults.processor,
                defaults.node_name,
                defaults.handler_timeout,
                defaults.task_lease
            ),
            (
                ProcessorConfig::default(),
                None,
                Duration::from_secs(15),
                Duration::from_secs(60)
            )
        );
        assert_eq!(defaults.task_storage.storage_timeout(), Duration::from_secs(6));
    }

    #[cfg(feature = "worker")]
    #[test]
    fn the_task_defaults_are_the_task_runtimes() {
        let runtime = crate::task::runtime::config::TaskRuntimeConfig::default();
        let defaults = Settings::default();
        assert_eq!(
            (
                defaults.handler_timeout,
                defaults.task_lease,
                defaults.task_storage.storage_timeout()
            ),
            (
                runtime.worker_pool().handler_timeout(),
                runtime.worker_pool().lease_duration(),
                runtime.storage().storage_timeout()
            )
        );
    }
}
