//! The settings of an engine: what it builds its components with. The library's defaults are the `DEFAULT_*` constants
//! (and the defaults of the components' own settings); [`Settings::from_lookup`] reads every setting from a variable
//! under the application's prefix (`ENCORE_SNAPSHOT_EVERY` for the prefix `ENCORE`), the task runtime's from its own
//! `OUTBOX_*` variables, and takes the application's [`Defaults`] where a variable is absent.
#[cfg(feature = "mongodb")]
use crate::engine::mongodb::MongoSettings;
#[cfg(feature = "postgres")]
use crate::engine::postgres::PostgresSettings;
use crate::{
    cqrs::{
        command::{
            locks::{DEFAULT_LOCK_MAX_WAIT, DEFAULT_LOCK_MAX_WAITERS, LockLimits, LockLimitsError},
            retry::AggregateConflictRetryPolicy,
        },
        projection::processor::config::{
            DEFAULT_DELIVERY_CAPACITY, DEFAULT_DELIVERY_MAX_BATCH, DEFAULT_PROCESSOR_CONCURRENCY, DEFAULT_PROCESSOR_LEASE,
            DEFAULT_PROCESSOR_PAGE_SIZE, DEFAULT_PROCESSOR_POLL_INTERVAL, DEFAULT_PROCESSOR_REBUILD_PAGE,
            DEFAULT_PROCESSOR_RETRY_BASE, DEFAULT_PROCESSOR_RETRY_MAX, DEFAULT_PROCESSOR_SETTLE, DeliveryLimits,
            DeliveryLimitsError, ProcessorConfig, ProcessorConfigError,
        },
    },
    task::{
        queue::{lease::LEASE_SAFETY_MARGIN, storage_config::TaskStorageConfig},
        runtime::config::{TaskRuntimeConfig, TaskRuntimeConfigError},
    },
};
#[cfg(feature = "http")]
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::{fmt::Display, num::NonZeroUsize, str::FromStr, time::Duration};
use thiserror::Error;

/// A snapshot of an aggregate every ten events.
pub const DEFAULT_SNAPSHOT_EVERY: NonZeroUsize = NonZeroUsize::new(10).unwrap();
/// Events counted at most for how far a view is behind its journal: the cap of `count_after` on PostgreSQL.
pub const DEFAULT_LAG_CAP: u64 = 100_000;
/// How long an operator's status waits for the checkpoints before it answers with what it saw last.
pub const DEFAULT_STATUS_TIMEOUT: Duration = Duration::from_secs(1);
/// The name the engine gives its connections (PostgreSQL's `application_name`, MongoDB's `appName`) and its log lines.
pub const DEFAULT_APPLICATION_NAME: &str = "cqrsx";
/// Where the HTTP API of the modules listens.
#[cfg(feature = "http")]
pub const DEFAULT_HTTP_ADDR: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));

const SNAPSHOT_EVERY: &str = "SNAPSHOT_EVERY";
const LOCK_MAX_WAIT: &str = "COMMAND_LOCK_MAX_WAIT_MS";
const LOCK_MAX_WAITERS: &str = "COMMAND_LOCK_MAX_WAITERS";
const PROJECTION_POLL: &str = "PROJECTION_POLL_MS";
const PROJECTION_PAGE: &str = "PROJECTION_PAGE";
const PROJECTION_LEASE: &str = "PROJECTION_LEASE_MS";
const PROJECTION_RETRY_BASE: &str = "PROJECTION_RETRY_BASE_MS";
const PROJECTION_RETRY_MAX: &str = "PROJECTION_RETRY_MAX_MS";
const PROJECTION_CONCURRENCY: &str = "PROJECTION_CONCURRENCY";
const PROJECTION_BUFFER: &str = "PROJECTION_BUFFER";
const PROJECTION_MAX_BATCH: &str = "PROJECTION_MAX_BATCH";
const PROJECTION_REBUILD_PAGE: &str = "PROJECTION_REBUILD_PAGE";
const PROJECTION_SETTLE: &str = "PROJECTION_SETTLE_MS";
const LAG_CAP: &str = "PROJECTION_LAG_CAP";
const STATUS_TIMEOUT: &str = "PROJECTION_STATUS_TIMEOUT_MS";
const NODE_NAME: &str = "NODE_NAME";
#[cfg(feature = "http")]
const HTTP_ADDR: &str = "HTTP_ADDR";
/// The task runtime's own variables, read without the prefix and untrimmed (`TaskRuntimeConfig::from_lookup`).
const OUTBOX: [&str; 7] = [
    "OUTBOX_WORKER_CONCURRENCY",
    "OUTBOX_QUEUE_CAPACITY",
    "OUTBOX_QUEUE_REFILL_PERCENT",
    "OUTBOX_POLL_INTERVAL_MS",
    "OUTBOX_HANDLER_TIMEOUT_SECS",
    "OUTBOX_LEASE_SECS",
    "OUTBOX_STORAGE_TIMEOUT_MS",
];

/// The variables the engine reads, by their names after the prefix (`OUTBOX_*` as they are): those of the components
/// this build of the library has.
pub(crate) fn variables() -> Vec<&'static str> {
    let mut names = vec![
        SNAPSHOT_EVERY,
        LOCK_MAX_WAIT,
        LOCK_MAX_WAITERS,
        PROJECTION_POLL,
        PROJECTION_PAGE,
        PROJECTION_LEASE,
        PROJECTION_RETRY_BASE,
        PROJECTION_RETRY_MAX,
        PROJECTION_CONCURRENCY,
        PROJECTION_BUFFER,
        PROJECTION_MAX_BATCH,
        PROJECTION_REBUILD_PAGE,
        PROJECTION_SETTLE,
        LAG_CAP,
        STATUS_TIMEOUT,
        NODE_NAME,
    ];
    #[cfg(feature = "postgres")]
    names.extend(crate::engine::postgres::VARIABLES);
    #[cfg(feature = "mongodb")]
    names.extend(crate::engine::mongodb::VARIABLES);
    #[cfg(feature = "http")]
    names.push(HTTP_ADDR);
    names.extend(OUTBOX);
    names
}

/// The application's values of the engine's settings, used only where a variable is absent, and the application's
/// name, which no variable sets. A value is written as in its variable (`("PROJECTION_SETTLE_MS", "10")`,
/// `("OUTBOX_LEASE_SECS", "30")`), under the variable's name after the prefix (`OUTBOX_*` as they are), and read like
/// it; a name the engine reads no variable of is refused when the settings are read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Defaults {
    application_name: &'static str,
    variables: &'static [(&'static str, &'static str)],
}

impl Defaults {
    /// The library's own: no value of the application, the name `cqrsx`.
    pub const LIBRARY: Defaults = Defaults::new(DEFAULT_APPLICATION_NAME, &[]);

    pub const fn new(application_name: &'static str, variables: &'static [(&'static str, &'static str)]) -> Self {
        Self {
            application_name,
            variables,
        }
    }

    pub fn application_name(&self) -> &'static str {
        self.application_name
    }

    /// The application's value of the variable `name` (after the prefix).
    fn get(&self, name: &str) -> Option<&'static str> {
        self.variables
            .iter()
            .find(|(known, _)| *known == name)
            .map(|(_, value)| *value)
    }
}

impl Default for Defaults {
    fn default() -> Self {
        Self::LIBRARY
    }
}

/// A setting that cannot be read or is out of its bounds: the variable it was read from (under the prefix) and why,
/// or a task runtime the library refuses.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum SettingsError {
    #[error("{variable}: {message}")]
    Invalid { variable: String, message: String },
    #[error("task runtime configuration: {0}")]
    TaskRuntime(#[from] TaskRuntimeConfigError),
}

/// What an engine builds its components with; [`Default`] is the library's. A module reads them
/// (`Setup::engine`), e.g. the handler's timeout for a request of its own.
///
/// - Every executor: a snapshot every [`snapshot_every`](Self::snapshot_every) events, the conflict retry, the lock
///   limits.
/// - Every projection processor: the [`processor`](Self::processor) configuration, this node as the owner of its
///   checkpoint; the operations over the views count a lag up to [`lag_cap`](Self::lag_cap) and an operator's status
///   waits [`status_timeout`](Self::status_timeout) for the checkpoints.
/// - The task runtime: [`task_runtime`](Self::task_runtime).
/// - The stores: [`postgres`](Self::postgres) (pools), [`mongodb`](Self::mongodb) (the client and the database of the
///   views); the HTTP API: [`http_addr`](Self::http_addr); the name of the connections.
#[derive(Clone, Debug)]
pub struct Settings {
    prefix: String,
    application_name: String,
    snapshot_every: NonZeroUsize,
    conflict_retry: AggregateConflictRetryPolicy,
    lock_limits: LockLimits,
    processor: ProcessorConfig,
    node_name: Option<String>,
    lag_cap: u64,
    status_timeout: Duration,
    task_runtime: TaskRuntimeConfig,
    #[cfg(feature = "postgres")]
    postgres: PostgresSettings,
    #[cfg(feature = "mongodb")]
    mongodb: MongoSettings,
    #[cfg(feature = "http")]
    http_addr: SocketAddr,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            application_name: DEFAULT_APPLICATION_NAME.to_owned(),
            snapshot_every: DEFAULT_SNAPSHOT_EVERY,
            conflict_retry: AggregateConflictRetryPolicy::default(),
            lock_limits: LockLimits::default(),
            processor: ProcessorConfig::default(),
            node_name: None,
            lag_cap: DEFAULT_LAG_CAP,
            status_timeout: DEFAULT_STATUS_TIMEOUT,
            task_runtime: TaskRuntimeConfig::default(),
            #[cfg(feature = "postgres")]
            postgres: PostgresSettings::default(),
            #[cfg(feature = "mongodb")]
            mongodb: MongoSettings::default(),
            #[cfg(feature = "http")]
            http_addr: DEFAULT_HTTP_ADDR,
        }
    }
}

impl Settings {
    /// The library's settings with these: a snapshot every `snapshot_every` events (positive), the lock limits of
    /// every executor, the configuration of every processor and the task runtime. A command waits for its lock less
    /// than a handler may run, since a chain step waits for it inside its handler.
    pub fn new(
        snapshot_every: usize,
        lock_limits: LockLimits,
        processor: ProcessorConfig,
        task_runtime: TaskRuntimeConfig,
    ) -> Result<Self, SettingsError> {
        let named = Read::bare();
        let snapshot_every =
            NonZeroUsize::new(snapshot_every).ok_or_else(|| named.invalid(SNAPSHOT_EVERY, "must be positive"))?;
        below_handler_timeout(&named, lock_limits, task_runtime.worker_pool().handler_timeout())?;
        Ok(Self {
            snapshot_every,
            lock_limits,
            processor,
            task_runtime,
            ..Self::default()
        })
    }

    /// Every setting from `lookup` under `prefix` (`{prefix}_{name}`, the name alone without a prefix), with the
    /// `defaults` of the application where a variable is absent and the library's where neither is. A variable of the
    /// prefix is trimmed and counts as absent when blank; the task runtime's `OUTBOX_*` go to the library as they are.
    /// A value that does not parse or is out of its bounds names its variable.
    pub fn from_lookup(
        prefix: &str,
        lookup: impl Fn(&str) -> Option<String>,
        defaults: &Defaults,
    ) -> Result<Self, SettingsError> {
        let read = Read {
            prefix,
            lookup: &lookup,
            defaults,
        };
        let known = variables();
        if let Some((name, _)) = defaults.variables.iter().find(|(name, _)| !known.contains(name)) {
            return Err(read.invalid_named("defaults", format!("{name} is no variable of the engine")));
        }
        let task_runtime = TaskRuntimeConfig::from_lookup(|name| lookup(name).or_else(|| defaults.get(name).map(str::to_owned)))?;
        let snapshot_every: usize = read.value(SNAPSHOT_EVERY, DEFAULT_SNAPSHOT_EVERY.get())?;
        let snapshot_every = NonZeroUsize::new(snapshot_every).ok_or_else(|| read.invalid(SNAPSHOT_EVERY, "must be positive"))?;
        #[cfg(feature = "postgres")]
        let postgres = PostgresSettings::read(&read)?;
        let lock_limits = command_lock_limits(&read, task_runtime.worker_pool().handler_timeout())?;
        let processor = projection_config(&read)?;
        let (lag_cap, status_timeout) = read_side(&read)?;
        #[cfg(feature = "mongodb")]
        let mongodb = MongoSettings::read(&read)?;
        Ok(Self {
            prefix: prefix.to_owned(),
            application_name: defaults.application_name().to_owned(),
            snapshot_every,
            conflict_retry: AggregateConflictRetryPolicy::default(),
            lock_limits,
            processor,
            node_name: read.raw(NODE_NAME),
            lag_cap,
            status_timeout,
            task_runtime,
            #[cfg(feature = "postgres")]
            postgres,
            #[cfg(feature = "mongodb")]
            mongodb,
            #[cfg(feature = "http")]
            http_addr: read.value(HTTP_ADDR, DEFAULT_HTTP_ADDR)?,
        })
    }

    /// The same settings, every conflict retried by `policy`.
    pub fn with_conflict_retry(mut self, policy: AggregateConflictRetryPolicy) -> Self {
        self.conflict_retry = policy;
        self
    }

    /// The same settings on the node `name`: the owner of the checkpoints of its processors.
    pub fn with_node_name(mut self, name: impl Into<String>) -> Self {
        self.node_name = Some(name.into());
        self
    }

    /// The prefix of the variables the settings were read from (empty for settings set by hand).
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn application_name(&self) -> &str {
        &self.application_name
    }

    /// Every executor: a snapshot of an aggregate every this many events.
    pub fn snapshot_every(&self) -> NonZeroUsize {
        self.snapshot_every
    }

    pub fn conflict_retry(&self) -> AggregateConflictRetryPolicy {
        self.conflict_retry
    }

    pub fn lock_limits(&self) -> LockLimits {
        self.lock_limits
    }

    /// Every projection processor.
    pub fn processor(&self) -> ProcessorConfig {
        self.processor
    }

    /// The owner this node writes on the checkpoints of its processors; `None`: a fresh one per engine.
    pub fn node_name(&self) -> Option<&str> {
        self.node_name.as_deref()
    }

    /// Events counted at most for how far a view is behind its journal.
    pub fn lag_cap(&self) -> u64 {
        self.lag_cap
    }

    /// How long an operator's status waits for the checkpoints.
    pub fn status_timeout(&self) -> Duration {
        self.status_timeout
    }

    /// The task runtime: its workers, its buffer, its poll, every attempt's handler timeout and lease, the storage
    /// budget.
    pub fn task_runtime(&self) -> &TaskRuntimeConfig {
        &self.task_runtime
    }

    /// How long a task's handler may run.
    pub fn handler_timeout(&self) -> Duration {
        self.task_runtime.worker_pool().handler_timeout()
    }

    /// The lease of a claimed task.
    pub fn task_lease(&self) -> Duration {
        self.task_runtime.worker_pool().lease_duration()
    }

    pub fn task_storage(&self) -> &TaskStorageConfig {
        self.task_runtime.storage()
    }

    /// The pools of an engine over PostgreSQL.
    #[cfg(feature = "postgres")]
    pub fn postgres(&self) -> &PostgresSettings {
        &self.postgres
    }

    /// The client of an engine with its views in MongoDB, and their database.
    #[cfg(feature = "mongodb")]
    pub fn mongodb(&self) -> &MongoSettings {
        &self.mongodb
    }

    /// Where `serve` listens.
    #[cfg(feature = "http")]
    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }

    /// The name of the variable of a setting: `{prefix}_{name}`, or the name alone without a prefix.
    pub(crate) fn variable(&self, name: &str) -> String {
        variable(&self.prefix, name)
    }

    /// Every setting as `(key, value)`, sorted by key: a setting read from a variable under the variable's name with
    /// the value the engine uses, read back from the object built from it (`LockLimits`, `ProcessorConfig`,
    /// `TaskRuntimeConfig`); what has no variable of its own (derived values, the plan of the connections) under a
    /// dotted name in lower case. Durations as [`duration_text`] writes them.
    pub fn describe(&self) -> Vec<(String, String)> {
        let name = |name: &str| self.variable(name);
        let (locks, projection) = (&self.lock_limits, &self.processor);
        let delivery = projection.delivery_limits();
        let worker = self.task_runtime.worker_pool();
        let mut lines: Vec<(String, String)> = vec![
            (name(SNAPSHOT_EVERY), self.snapshot_every.to_string()),
            (name(LOCK_MAX_WAIT), duration_text(locks.max_wait())),
            (name(LOCK_MAX_WAITERS), locks.max_waiters().to_string()),
            (name(PROJECTION_POLL), duration_text(projection.poll_interval())),
            (name(PROJECTION_PAGE), projection.page_size().to_string()),
            (name(PROJECTION_LEASE), duration_text(projection.lease())),
            (name(PROJECTION_RETRY_BASE), duration_text(projection.retry_base())),
            (name(PROJECTION_RETRY_MAX), duration_text(projection.retry_max())),
            (name(PROJECTION_CONCURRENCY), projection.concurrency().to_string()),
            (name(PROJECTION_BUFFER), delivery.capacity().to_string()),
            (name(PROJECTION_MAX_BATCH), delivery.max_batch().to_string()),
            (name(PROJECTION_REBUILD_PAGE), projection.rebuild_page().to_string()),
            (name(PROJECTION_SETTLE), duration_text(projection.settle())),
            (
                "projection.pages_in_flight".to_owned(),
                projection.pages_in_flight().to_string(),
            ),
            (name(LAG_CAP), self.lag_cap.to_string()),
            (name(STATUS_TIMEOUT), duration_text(self.status_timeout)),
            (name(NODE_NAME), self.node_name.clone().unwrap_or_else(|| "none".to_owned())),
            ("OUTBOX_WORKER_CONCURRENCY".to_owned(), worker.concurrency().to_string()),
            ("OUTBOX_QUEUE_CAPACITY".to_owned(), worker.buffer_capacity().to_string()),
            (
                "OUTBOX_QUEUE_REFILL_PERCENT".to_owned(),
                format!("{} %", worker.buffer_refill_percent()),
            ),
            ("OUTBOX_POLL_INTERVAL_MS".to_owned(), duration_text(worker.poll_interval())),
            (
                "OUTBOX_HANDLER_TIMEOUT_SECS".to_owned(),
                duration_text(worker.handler_timeout()),
            ),
            ("OUTBOX_LEASE_SECS".to_owned(), duration_text(worker.lease_duration())),
            (
                "OUTBOX_STORAGE_TIMEOUT_MS".to_owned(),
                duration_text(self.task_runtime.storage().storage_timeout()),
            ),
            (
                "outbox.lease_budget".to_owned(),
                duration_text(lease_budget(&self.task_runtime)),
            ),
        ];
        #[cfg(feature = "postgres")]
        lines.extend(self.postgres.describe(&name, &self.application_name));
        #[cfg(feature = "mongodb")]
        lines.extend(self.mongodb.describe(&name, &self.application_name));
        #[cfg(feature = "http")]
        lines.push((name(HTTP_ADDR), self.http_addr.to_string()));
        lines.sort();
        lines
    }
}

/// `{prefix}_{name}`, or the name alone without a prefix.
pub(crate) fn variable(prefix: &str, name: &str) -> String {
    match prefix {
        "" => name.to_owned(),
        prefix => format!("{prefix}_{name}"),
    }
}

/// A duration as a description of the settings writes it: whole seconds as `N s` (zero too), otherwise whole
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

/// The variables of the settings: by their names after the prefix, the application's defaults where they are absent.
pub(crate) struct Read<'a> {
    prefix: &'a str,
    lookup: &'a dyn Fn(&str) -> Option<String>,
    defaults: &'a Defaults,
}

impl<'a> Read<'a> {
    #[cfg(all(test, feature = "postgres"))]
    pub(crate) fn over(prefix: &'a str, lookup: &'a dyn Fn(&str) -> Option<String>, defaults: &'a Defaults) -> Self {
        Self {
            prefix,
            lookup,
            defaults,
        }
    }

    /// The names of settings set by hand: no prefix, no variable.
    pub(crate) fn bare() -> Read<'static> {
        Read {
            prefix: "",
            lookup: &|_| None,
            defaults: &Defaults::LIBRARY,
        }
    }

    pub(crate) fn variable(&self, name: &str) -> String {
        variable(self.prefix, name)
    }

    /// The variable `name` trimmed, absent when blank; else the application's default.
    pub(crate) fn raw(&self, name: &str) -> Option<String> {
        (self.lookup)(&self.variable(name))
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .or_else(|| self.defaults.get(name).map(str::to_owned))
    }

    /// The variable `name` parsed, `default` when it is absent; a value that does not parse names the variable.
    pub(crate) fn value<T: FromStr>(&self, name: &str, default: T) -> Result<T, SettingsError>
    where
        T::Err: Display,
    {
        match self.raw(name) {
            Some(raw) => raw
                .parse()
                .map_err(|error: T::Err| self.invalid(name, format!("cannot parse {raw:?}: {error}"))),
            None => Ok(default),
        }
    }

    /// The variable `name` in milliseconds.
    pub(crate) fn millis(&self, name: &str, default: Duration) -> Result<Duration, SettingsError> {
        match self.raw(name) {
            Some(_) => Ok(Duration::from_millis(self.value(name, 0)?)),
            None => Ok(default),
        }
    }

    pub(crate) fn invalid(&self, name: &str, message: impl Into<String>) -> SettingsError {
        self.invalid_named(&self.variable(name), message)
    }

    fn invalid_named(&self, variable: &str, message: impl Into<String>) -> SettingsError {
        SettingsError::Invalid {
            variable: variable.to_owned(),
            message: message.into(),
        }
    }
}

/// A chain task waits for the lock inside its handler, so the wait must end before the handler times out.
fn below_handler_timeout(read: &Read, limits: LockLimits, handler_timeout: Duration) -> Result<(), SettingsError> {
    if limits.max_wait() >= handler_timeout {
        return Err(read.invalid(LOCK_MAX_WAIT, "must be below OUTBOX_HANDLER_TIMEOUT_SECS"));
    }
    Ok(())
}

/// Commands to one aggregate run one at a time under its lock, in arrival order; these bound the wait for the lock
/// and default to the library's.
fn command_lock_limits(read: &Read, handler_timeout: Duration) -> Result<LockLimits, SettingsError> {
    let max_wait = read.millis(LOCK_MAX_WAIT, DEFAULT_LOCK_MAX_WAIT)?;
    let max_waiters = read.value(LOCK_MAX_WAITERS, DEFAULT_LOCK_MAX_WAITERS.get())?;
    let limits = LockLimits::new(max_wait, max_waiters).map_err(|error| match error {
        LockLimitsError::ZeroMaxWait => read.invalid(LOCK_MAX_WAIT, "must be positive"),
        LockLimitsError::ZeroMaxWaiters => read.invalid(LOCK_MAX_WAITERS, "must be positive"),
    })?;
    below_handler_timeout(read, limits, handler_timeout)?;
    Ok(limits)
}

/// The projection processors: every bound defaults to the library's (the settle to the application's, if it has one);
/// the key buffer is the depth of the window of pages plus one, and holds two at least.
fn projection_config(read: &Read) -> Result<ProcessorConfig, SettingsError> {
    let poll = read.millis(PROJECTION_POLL, DEFAULT_PROCESSOR_POLL_INTERVAL)?;
    let page = read.value(PROJECTION_PAGE, DEFAULT_PROCESSOR_PAGE_SIZE)?;
    let lease = read.millis(PROJECTION_LEASE, DEFAULT_PROCESSOR_LEASE)?;
    let retry_base = read.millis(PROJECTION_RETRY_BASE, DEFAULT_PROCESSOR_RETRY_BASE)?;
    let retry_max = read.millis(PROJECTION_RETRY_MAX, DEFAULT_PROCESSOR_RETRY_MAX)?;
    let concurrency = read.value(PROJECTION_CONCURRENCY, DEFAULT_PROCESSOR_CONCURRENCY)?;
    let rebuild_page = read.value(PROJECTION_REBUILD_PAGE, DEFAULT_PROCESSOR_REBUILD_PAGE)?;
    let settle = read.millis(PROJECTION_SETTLE, DEFAULT_PROCESSOR_SETTLE)?;
    let capacity = read.value(PROJECTION_BUFFER, DEFAULT_DELIVERY_CAPACITY)?;
    let max_batch = read.value(PROJECTION_MAX_BATCH, DEFAULT_DELIVERY_MAX_BATCH)?;
    let delivery_limits = DeliveryLimits::new(capacity, max_batch).map_err(|error| match error {
        DeliveryLimitsError::CapacityTooSmall { min } => read.invalid(PROJECTION_BUFFER, format!("must be at least {min}")),
        DeliveryLimitsError::ZeroMaxBatch => read.invalid(PROJECTION_MAX_BATCH, "must be positive"),
    })?;
    let named = |error: ProcessorConfigError| match error {
        ProcessorConfigError::ZeroPollInterval => read.invalid(PROJECTION_POLL, "must be positive"),
        ProcessorConfigError::ZeroPageSize => read.invalid(PROJECTION_PAGE, "must be positive"),
        ProcessorConfigError::ZeroRebuildPage => read.invalid(PROJECTION_REBUILD_PAGE, "must be positive"),
        ProcessorConfigError::LeaseTooShort { min } => {
            read.invalid(PROJECTION_LEASE, format!("must be at least {} ms", min.as_millis()))
        }
        ProcessorConfigError::InvalidRetry if retry_base.is_zero() => read.invalid(PROJECTION_RETRY_BASE, "must be positive"),
        ProcessorConfigError::InvalidRetry => read.invalid(
            PROJECTION_RETRY_BASE,
            format!("must not exceed {}", read.variable(PROJECTION_RETRY_MAX)),
        ),
        ProcessorConfigError::ZeroConcurrency => read.invalid(PROJECTION_CONCURRENCY, "must be positive"),
        ProcessorConfigError::SettleTooLong { .. } => read.invalid(
            PROJECTION_SETTLE,
            format!("must not exceed {}", read.variable(PROJECTION_POLL)),
        ),
    };
    ProcessorConfig::new(poll, page, lease, delivery_limits)
        .and_then(|config| config.with_retry(retry_base, retry_max))
        .and_then(|config| config.with_rebuild_page(rebuild_page))
        .and_then(|config| config.with_concurrency(concurrency))
        .and_then(|config| config.with_settle(settle))
        .map_err(named)
}

/// The operations over the views: a lag is counted up to a cap, a status waits a while for the checkpoints.
fn read_side(read: &Read) -> Result<(u64, Duration), SettingsError> {
    let lag_cap = read.value(LAG_CAP, DEFAULT_LAG_CAP)?;
    let status_timeout = read.millis(STATUS_TIMEOUT, DEFAULT_STATUS_TIMEOUT)?;
    if lag_cap == 0 {
        return Err(read.invalid(LAG_CAP, "must be positive"));
    }
    if status_timeout.is_zero() {
        return Err(read.invalid(STATUS_TIMEOUT, "must be positive"));
    }
    Ok((lag_cap, status_timeout))
}

/// What the task lease must exceed, the bound the library checks when it reads the task runtime
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::runtime::worker_pool_config::{TaskWorkerPoolConfig, TaskWorkerPoolConfigError};
    use std::collections::HashMap;

    /// What the demo of the repository sets where its variables are absent: the task runtime and the settle the moved
    /// tests of its configuration were written with.
    const DEMO: Defaults = Defaults::new(
        "encore-backend",
        &[
            ("OUTBOX_WORKER_CONCURRENCY", "2"),
            ("OUTBOX_QUEUE_CAPACITY", "2"),
            ("OUTBOX_POLL_INTERVAL_MS", "300"),
            ("OUTBOX_HANDLER_TIMEOUT_SECS", "5"),
            ("OUTBOX_LEASE_SECS", "30"),
            ("OUTBOX_STORAGE_TIMEOUT_MS", "3000"),
            ("PROJECTION_SETTLE_MS", "10"),
        ],
    );

    // The variables by their names under the demo's prefix, as its configuration's tests named them.
    #[cfg(feature = "postgres")]
    const STATEMENT_TIMEOUT: &str = "ENCORE_DATABASE_STATEMENT_TIMEOUT_MS";
    const LOCK_MAX_WAIT: &str = "ENCORE_COMMAND_LOCK_MAX_WAIT_MS";
    const LOCK_MAX_WAITERS: &str = "ENCORE_COMMAND_LOCK_MAX_WAITERS";
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
    const LAG_CAP: &str = "ENCORE_PROJECTION_LAG_CAP";
    const STATUS_TIMEOUT: &str = "ENCORE_PROJECTION_STATUS_TIMEOUT_MS";

    /// The settings of the variables `pairs` (full names) under the prefix `ENCORE`, the demo's defaults elsewhere.
    #[cfg(feature = "postgres")]
    fn from_pairs(pairs: &[(&str, &str)]) -> Result<Settings, SettingsError> {
        let pairs: HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        Settings::from_lookup("ENCORE", |name| pairs.get(name).cloned(), &DEMO)
    }

    /// `lookup` read as the settings of the prefix `ENCORE` read it, the demo's defaults elsewhere.
    fn reading<T>(pairs: &[(&'static str, &str)], read: impl FnOnce(&Read) -> T) -> T {
        let pairs: HashMap<&str, String> = pairs.iter().map(|(k, v)| (*k, v.to_string())).collect();
        let lookup = |name: &str| pairs.get(name).cloned();
        read(&Read {
            prefix: "ENCORE",
            lookup: &lookup,
            defaults: &DEMO,
        })
    }

    fn outbox_default(name: &str) -> Option<String> {
        name.starts_with("OUTBOX_")
            .then(|| DEMO.get(name).map(str::to_owned))
            .flatten()
    }

    #[test]
    fn the_default_settings_are_the_library_constants() {
        let defaults = Settings::default();
        assert_eq!(
            (
                defaults.snapshot_every().get(),
                defaults.conflict_retry(),
                defaults.lock_limits()
            ),
            (
                10,
                AggregateConflictRetryPolicy::default(),
                LockLimits::new(DEFAULT_LOCK_MAX_WAIT, DEFAULT_LOCK_MAX_WAITERS.get()).unwrap()
            )
        );
        assert_eq!(
            (
                defaults.processor(),
                defaults.node_name(),
                defaults.handler_timeout(),
                defaults.task_lease()
            ),
            (
                ProcessorConfig::default(),
                None,
                Duration::from_secs(15),
                Duration::from_secs(60)
            )
        );
        assert_eq!(defaults.task_storage().storage_timeout(), Duration::from_secs(6));
    }

    #[test]
    fn the_task_defaults_are_the_task_runtimes() {
        let runtime = TaskRuntimeConfig::default();
        let defaults = Settings::default();
        assert_eq!(
            (
                defaults.handler_timeout(),
                defaults.task_lease(),
                defaults.task_storage().storage_timeout()
            ),
            (
                runtime.worker_pool().handler_timeout(),
                runtime.worker_pool().lease_duration(),
                runtime.storage().storage_timeout()
            )
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn durations_are_whole_seconds_or_milliseconds() {
        use crate::{engine::postgres::statement_timeout_text, postgres::pool::StatementTimeout};
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

    #[cfg(feature = "postgres")]
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

    fn lock_limits(pairs: &[(&'static str, &str)], handler_timeout_secs: u64) -> Result<LockLimits, SettingsError> {
        reading(pairs, |read| {
            command_lock_limits(read, Duration::from_secs(handler_timeout_secs))
        })
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

    fn projection(pairs: &[(&'static str, &str)]) -> Result<ProcessorConfig, SettingsError> {
        reading(pairs, projection_config)
    }

    /// The demo's pause between a commit's wake-up and the processor's read.
    const DEFAULT_PROJECTION_SETTLE: Duration = Duration::from_millis(10);

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

    fn settings(pairs: &[(&'static str, &str)]) -> Result<(u64, Duration), SettingsError> {
        reading(pairs, read_side)
    }

    #[test]
    fn the_read_side_defaults_to_the_library_cap_and_names_the_variable_it_rejects() {
        assert_eq!(settings(&[]).unwrap(), (DEFAULT_LAG_CAP, DEFAULT_STATUS_TIMEOUT));
        for (pairs, name) in [
            (vec![(LAG_CAP, "0")], LAG_CAP),
            (vec![(LAG_CAP, "-1")], LAG_CAP),
            (vec![(STATUS_TIMEOUT, "0")], STATUS_TIMEOUT),
        ] {
            let error = settings(&pairs).unwrap_err().to_string();
            assert!(error.starts_with(&format!("{name}: ")), "{pairs:?}: {error}");
        }
    }

    #[test]
    fn defaults_apply_only_where_a_variable_is_absent_and_name_variables_of_the_engine() {
        const NODE: Defaults = Defaults::new(
            "shop",
            &[("NODE_NAME", "node-a"), ("SNAPSHOT_EVERY", "25"), ("OUTBOX_LEASE_SECS", "90")],
        );
        let lookup = |name: &str| match name {
            "SHOP_SNAPSHOT_EVERY" => Some(" 5 ".to_owned()),
            "SHOP_NODE_NAME" => Some("  ".to_owned()),
            _ => None,
        };
        let settings = Settings::from_lookup("SHOP", lookup, &NODE).unwrap();
        assert_eq!(
            (
                settings.snapshot_every().get(),
                settings.node_name(),
                settings.task_lease(),
                settings.application_name(),
                settings.prefix()
            ),
            (5, Some("node-a"), Duration::from_secs(90), "shop", "SHOP"),
            "a variable wins, a blank one is absent, the default fills in"
        );
        const UNKNOWN: Defaults = Defaults::new("shop", &[("TICKET_LIMIT", "4")]);
        assert_eq!(
            Settings::from_lookup("SHOP", |_| None, &UNKNOWN).unwrap_err().to_string(),
            "defaults: TICKET_LIMIT is no variable of the engine"
        );
        const BROKEN: Defaults = Defaults::new("shop", &[("SNAPSHOT_EVERY", "0")]);
        assert_eq!(
            Settings::from_lookup("SHOP", |_| None, &BROKEN).unwrap_err().to_string(),
            "SHOP_SNAPSHOT_EVERY: must be positive"
        );
        assert_eq!(Defaults::default(), Defaults::LIBRARY);
        assert_eq!(
            Defaults::new("cqrsx", &[]),
            Defaults::LIBRARY,
            "built at run time as in a constant"
        );
        assert_eq!(
            Settings::from_lookup("", |_| None, &Defaults::LIBRARY)
                .unwrap()
                .application_name(),
            "cqrsx"
        );
        let outbox = Settings::from_lookup("SHOP", |name| (name == "OUTBOX_LEASE_SECS").then(|| "1".to_owned()), &NODE);
        assert!(matches!(outbox, Err(SettingsError::TaskRuntime(_))), "{outbox:?}");
        assert!(outbox.unwrap_err().to_string().starts_with("task runtime configuration: "));
    }

    #[test]
    fn every_variable_of_the_engine_is_read_once_under_its_prefix() {
        let read = std::sync::Mutex::new(Vec::new());
        Settings::from_lookup(
            "SHOP",
            |name| {
                read.lock().unwrap().push(name.to_owned());
                None
            },
            &Defaults::LIBRARY,
        )
        .unwrap();
        let mut read = read.into_inner().unwrap();
        read.sort();
        let mut expected: Vec<String> = variables()
            .into_iter()
            .map(|name| {
                if name.starts_with("OUTBOX_") {
                    name.to_owned()
                } else {
                    format!("SHOP_{name}")
                }
            })
            .collect();
        expected.sort();
        assert_eq!(read, expected);
        let described: Vec<String> = Settings::default()
            .describe()
            .into_iter()
            .map(|(key, _)| key)
            .filter(|key| key.starts_with(|c: char| c.is_ascii_uppercase()))
            .collect();
        let mut names: Vec<&str> = variables();
        names.sort();
        assert_eq!(described, names, "every variable is described under its name");
        let counted = 16 + 7;
        #[cfg(feature = "postgres")]
        let counted = counted + 5;
        #[cfg(feature = "mongodb")]
        let counted = counted + 3;
        #[cfg(feature = "http")]
        let counted = counted + 1;
        assert_eq!(names.len(), counted);
    }

    #[test]
    fn a_checked_new_names_the_value_it_refuses() {
        let (limits, processor, runtime) = (
            LockLimits::default(),
            ProcessorConfig::default(),
            TaskRuntimeConfig::default(),
        );
        assert_eq!(
            Settings::new(0, limits, processor, runtime.clone()).unwrap_err().to_string(),
            "SNAPSHOT_EVERY: must be positive"
        );
        let long_wait = LockLimits::new(Duration::from_secs(15), 1).unwrap();
        assert_eq!(
            Settings::new(10, long_wait, processor, runtime.clone())
                .unwrap_err()
                .to_string(),
            "COMMAND_LOCK_MAX_WAIT_MS: must be below OUTBOX_HANDLER_TIMEOUT_SECS"
        );
        let settings = Settings::new(3, limits, processor, runtime).unwrap().with_node_name("node-b");
        assert_eq!(
            (
                settings.snapshot_every().get(),
                settings.node_name(),
                settings.lag_cap(),
                settings.status_timeout()
            ),
            (3, Some("node-b"), DEFAULT_LAG_CAP, DEFAULT_STATUS_TIMEOUT)
        );
        #[cfg(feature = "postgres")]
        assert_eq!(settings.postgres(), &PostgresSettings::default());
        #[cfg(feature = "mongodb")]
        assert_eq!(settings.mongodb(), &MongoSettings::default());
        #[cfg(feature = "http")]
        assert_eq!(settings.http_addr(), DEFAULT_HTTP_ADDR);
    }
}
