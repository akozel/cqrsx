//! The engine over PostgreSQL (feature `postgres`): [`Postgres`] is the store of the journal, the snapshots, the
//! outbox and the task queue (`Builder::event_store`) and may hold the views and their checkpoints too
//! (`Builder::projections`). Its settings are the three pools of [`PostgresSettings`], opened as their [`PoolPlan`]
//! says when the engine is built: one connection each, an unreachable database fails the build.
use crate::{
    engine::{
        build::BuildError,
        settings::{Read, Settings, SettingsError, duration_text},
    },
    postgres::pool::{
        ConnectionPingPolicy, DEFAULT_IDLE_PING_AFTER, DEFAULT_MAX_CONNECTIONS, DEFAULT_STATEMENT_TIMEOUT, StatementTimeout,
        connect_pool_with,
    },
};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{collections::BTreeMap, fmt, num::NonZeroU32, time::Duration};

/// The journal, the snapshots, the outbox and the task queue in PostgreSQL (`Builder::event_store`), or the views and
/// their checkpoints (`Builder::projections`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Postgres;

/// The three pools of an engine over PostgreSQL, opened by the engine as the settings plan them, or by the application
/// (`Builder::postgres_pools`).
#[derive(Clone, Debug)]
pub struct PostgresPools {
    /// Commands, the task queue, the checkpoints and the views in PostgreSQL, the count of a lag: short statements under
    /// the statement timeout.
    pub main: PgPool,
    /// Whole-history reads: verify, the size of the journal, the history of one aggregate.
    pub history: PgPool,
    /// The processors' reads of the journal, apart from the commands' connections.
    pub projection: PgPool,
}

/// Opens the pools the settings plan, over the database of the settings, in the order main, projection, history: each
/// opens one connection, so an unreachable database fails here.
pub(crate) async fn open(settings: &Settings) -> Result<PostgresPools, BuildError> {
    let postgres = settings.postgres();
    let plan = postgres.pool_plan(settings.application_name());
    let connection = postgres
        .url()
        .parse::<PgConnectOptions>()
        .map_err(|source| BuildError::DatabaseUrl {
            variable: settings.variable(DATABASE_URL),
            source,
        })?
        .application_name(&plan.application_name);
    let main = plan.main.connect(connection.clone()).await.map_err(BuildError::Pool)?;
    let projection = plan
        .projection
        .connect(connection.clone())
        .await
        .map_err(BuildError::ProjectionPool)?;
    let history = plan.history.connect(connection).await.map_err(BuildError::HistoryPool)?;
    Ok(PostgresPools {
        main,
        history,
        projection,
    })
}

/// The database the engine connects to.
pub const DEFAULT_DATABASE_URL: &str = "postgresql://cqrsx:cqrsx@localhost:5433/cqrsx";
/// Connections of the history pool: whole-history reads are rare.
pub const DEFAULT_HISTORY_POOL_SIZE: NonZeroU32 = NonZeroU32::new(1).unwrap();
/// Connections of the projection pool: the processors' pages and rebuilds.
pub const DEFAULT_PROJECTION_POOL_SIZE: NonZeroU32 = NonZeroU32::new(4).unwrap();
/// How long the history pool keeps an idle connection: it keeps none at least, it is for rare reads.
pub const HISTORY_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

const DATABASE_URL: &str = "DATABASE_URL";
const DATABASE_POOL_SIZE: &str = "DATABASE_POOL_SIZE";
const STATEMENT_TIMEOUT: &str = "DATABASE_STATEMENT_TIMEOUT_MS";
const HISTORY_POOL_SIZE: &str = "DATABASE_HISTORY_POOL_SIZE";
const PROJECTION_POOL_SIZE: &str = "DATABASE_PROJECTION_POOL_SIZE";

/// The variables of the pools, by their names after the prefix.
pub(crate) const VARIABLES: [&str; 5] = [
    DATABASE_URL,
    DATABASE_POOL_SIZE,
    STATEMENT_TIMEOUT,
    HISTORY_POOL_SIZE,
    PROJECTION_POOL_SIZE,
];

/// The pools of an engine over PostgreSQL, all over one database: the main pool (commands, the task queue, the
/// checkpoints and the views in PostgreSQL, the count of a lag) under a statement timeout; the projection pool (the
/// processors' reads of the journal) under the same timeout but apart from the commands' connections; the history pool
/// (verify, the size of the journal, the history of one aggregate) without one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresSettings {
    url: String,
    pool_size: NonZeroU32,
    statement_timeout: StatementTimeout,
    history_pool_size: NonZeroU32,
    projection_pool_size: NonZeroU32,
}

impl Default for PostgresSettings {
    fn default() -> Self {
        Self {
            url: DEFAULT_DATABASE_URL.to_owned(),
            pool_size: DEFAULT_MAX_CONNECTIONS,
            statement_timeout: StatementTimeout::Default,
            history_pool_size: DEFAULT_HISTORY_POOL_SIZE,
            projection_pool_size: DEFAULT_PROJECTION_POOL_SIZE,
        }
    }
}

impl PostgresSettings {
    /// The pools of the database `url`; a custom statement timeout is 1 ms to `i32::MAX` ms.
    pub fn try_new(
        url: impl Into<String>,
        pool_size: NonZeroU32,
        statement_timeout: StatementTimeout,
        history_pool_size: NonZeroU32,
        projection_pool_size: NonZeroU32,
    ) -> Result<Self, SettingsError> {
        if let StatementTimeout::Custom(timeout) = statement_timeout
            && !(1..=i32::MAX as u128).contains(&timeout.as_millis())
        {
            return Err(Read::bare().invalid(STATEMENT_TIMEOUT, OUT_OF_RANGE));
        }
        Ok(Self {
            url: url.into(),
            pool_size,
            statement_timeout,
            history_pool_size,
            projection_pool_size,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Connections of the main pool.
    pub fn pool_size(&self) -> NonZeroU32 {
        self.pool_size
    }

    /// The cap of every statement of the main and the projection pool.
    pub fn statement_timeout(&self) -> StatementTimeout {
        self.statement_timeout
    }

    pub fn history_pool_size(&self) -> NonZeroU32 {
        self.history_pool_size
    }

    pub fn projection_pool_size(&self) -> NonZeroU32 {
        self.projection_pool_size
    }

    /// The pools as they are opened, under the connections' name `application_name`: the sizes and the timeout of
    /// these settings, a ping only after 100 ms idle, and a history pool without a cap that keeps no idle connection
    /// longer than 30 s.
    pub fn pool_plan(&self, application_name: &str) -> PoolPlan {
        let ping = ConnectionPingPolicy::Idle(DEFAULT_IDLE_PING_AFTER);
        let capped = |max_connections| PostgresPool {
            max_connections,
            min_connections: None,
            idle_timeout: None,
            ping,
            statement_timeout: self.statement_timeout,
        };
        PoolPlan {
            application_name: application_name.to_owned(),
            main: capped(self.pool_size),
            projection: capped(self.projection_pool_size),
            history: PostgresPool {
                max_connections: self.history_pool_size,
                min_connections: Some(0),
                idle_timeout: Some(HISTORY_IDLE_TIMEOUT),
                ping,
                statement_timeout: StatementTimeout::Unlimited,
            },
        }
    }

    pub(crate) fn read(read: &Read) -> Result<Self, SettingsError> {
        let pool_size = read.value(DATABASE_POOL_SIZE, DEFAULT_MAX_CONNECTIONS)?;
        let history_pool_size = read.value(HISTORY_POOL_SIZE, DEFAULT_HISTORY_POOL_SIZE)?;
        let projection_pool_size = read.value(PROJECTION_POOL_SIZE, DEFAULT_PROJECTION_POOL_SIZE)?;
        let statement_timeout = checked_timeout(read, read.raw(STATEMENT_TIMEOUT))?;
        Ok(Self {
            url: read.raw(DATABASE_URL).unwrap_or_else(|| DEFAULT_DATABASE_URL.to_owned()),
            pool_size,
            statement_timeout,
            history_pool_size,
            projection_pool_size,
        })
    }

    /// The variables of these settings and the plan of the pools, in the order described.
    pub(crate) fn entries(&self, name: &dyn Fn(&str) -> String, application_name: &str) -> Vec<(String, String)> {
        let mut entries = vec![
            (name(DATABASE_URL), self.url.clone()),
            (name(DATABASE_POOL_SIZE), self.pool_size.to_string()),
            (name(STATEMENT_TIMEOUT), self.statement_timeout.to_string()),
            (name(HISTORY_POOL_SIZE), self.history_pool_size.to_string()),
            (name(PROJECTION_POOL_SIZE), self.projection_pool_size.to_string()),
        ];
        entries.extend(self.pool_plan(application_name).entries());
        entries
    }
}

const OUT_OF_RANGE: &str = "must be 0 (no cap) or 1..=2147483647 milliseconds";

/// Unset = `StatementTimeout::Default`, `0` = `Unlimited`, any other number of milliseconds = `Custom`.
fn checked_timeout(read: &Read, value: Option<String>) -> Result<StatementTimeout, SettingsError> {
    match value.map(|value| value.parse::<u32>()) {
        None => Ok(StatementTimeout::Default),
        Some(Ok(0)) => Ok(StatementTimeout::Unlimited),
        Some(Ok(millis)) if millis <= i32::MAX as u32 => Ok(StatementTimeout::Custom(Duration::from_millis(millis.into()))),
        Some(_) => Err(read.invalid(STATEMENT_TIMEOUT, OUT_OF_RANGE)),
    }
}

/// The cap a statement timeout puts on a statement as the description of the settings writes it (`Default` is the
/// library's 4 s), or `unlimited`.
impl fmt::Display for StatementTimeout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => f.write_str(&duration_text(DEFAULT_STATEMENT_TIMEOUT)),
            Self::Custom(timeout) => f.write_str(&duration_text(*timeout)),
            Self::Unlimited => f.write_str("unlimited"),
        }
    }
}

/// How the engine connects one PostgreSQL pool: what it sets; everything else is SQLx's default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PostgresPool {
    pub max_connections: NonZeroU32,
    /// `None` keeps SQLx's minimum (none).
    pub min_connections: Option<u32>,
    /// `None` keeps SQLx's idle timeout (10 minutes).
    pub idle_timeout: Option<Duration>,
    pub ping: ConnectionPingPolicy,
    pub statement_timeout: StatementTimeout,
}

impl PostgresPool {
    /// SQLx's pool options with what this plan sets on top of its defaults.
    pub fn options(&self) -> PgPoolOptions {
        let mut options = PgPoolOptions::new().max_connections(self.max_connections.get());
        if let Some(min_connections) = self.min_connections {
            options = options.min_connections(min_connections);
        }
        if let Some(idle_timeout) = self.idle_timeout {
            options = options.idle_timeout(idle_timeout);
        }
        options
    }

    /// Opens the pool: one connection at once, so an unreachable database fails here.
    pub(crate) async fn connect(&self, connection: PgConnectOptions) -> Result<PgPool, sqlx::Error> {
        connect_pool_with(connection, self.options(), self.ping, self.statement_timeout).await
    }

    /// The pool as it is opened: SQLx's options read back, the ping policy and the statement timeout.
    fn entries(&self, name: &str) -> Vec<(String, String)> {
        let options = self.options();
        let optional = |duration: Option<Duration>| duration.map_or_else(|| "none".to_owned(), duration_text);
        let ping = match self.ping {
            ConnectionPingPolicy::Default => "always".to_owned(),
            ConnectionPingPolicy::Idle(after) => format!("idle {}", duration_text(after)),
        };
        [
            ("acquire_timeout", duration_text(options.get_acquire_timeout())),
            ("idle_timeout", optional(options.get_idle_timeout())),
            ("max_connections", options.get_max_connections().to_string()),
            ("max_lifetime", optional(options.get_max_lifetime())),
            ("min_connections", options.get_min_connections().to_string()),
            ("ping", ping),
            ("statement_timeout", self.statement_timeout.to_string()),
        ]
        .into_iter()
        .map(|(field, value)| (format!("postgres.{name}.{field}"), value))
        .collect()
    }
}

/// The three pools of an engine over PostgreSQL, over the same address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PoolPlan {
    /// PostgreSQL's `application_name` of every connection.
    pub application_name: String,
    /// Commands, the task queue, the checkpoints: short static statements, each under the statement timeout.
    pub main: PostgresPool,
    /// The projection processors' reads of the journal, under the main pool's statement timeout but apart from the
    /// commands' connections.
    pub projection: PostgresPool,
    /// Whole-history reads: no statement timeout, and no connection kept while none of them runs.
    pub history: PostgresPool,
}

impl PoolPlan {
    /// Every pool as `postgres.<pool>.<field>` and the application name, by key.
    pub fn describe(&self) -> BTreeMap<String, String> {
        self.entries().into_iter().collect()
    }

    /// What [`describe`](Self::describe) holds, in the order described.
    pub(crate) fn entries(&self) -> Vec<(String, String)> {
        let mut entries = vec![("postgres.application_name".to_owned(), self.application_name.clone())];
        entries.extend(self.main.entries("main"));
        entries.extend(self.projection.entries("projection"));
        entries.extend(self.history.entries("history"));
        entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::settings::{Defaults, Settings};
    use std::{collections::HashMap, num::NonZeroUsize};

    /// `value` as the variable of the statement timeout under the prefix `ENCORE` (`None`: absent; blank counts as a
    /// value here, as the configuration's parser saw it).
    fn statement_timeout(value: Option<String>) -> Result<StatementTimeout, SettingsError> {
        let lookup = |_: &str| None;
        checked_timeout(&Read::over("ENCORE", &lookup, &Defaults::LIBRARY), value)
    }

    fn settings(pairs: &[(&str, &str)]) -> Result<Settings, SettingsError> {
        let pairs: HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        Settings::from_lookup("ENCORE", |name| pairs.get(name).cloned(), &Defaults::LIBRARY)
    }

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
        assert_eq!(settings(&[]).unwrap().postgres().pool_size().get(), 10);
        let error = settings(&[("ENCORE_DATABASE_POOL_SIZE", "0")]).unwrap_err();
        assert!(error.to_string().contains("ENCORE_DATABASE_POOL_SIZE"), "{error}");
    }

    #[test]
    fn the_pool_plan_takes_the_sizes_and_the_timeout_of_the_configuration() {
        let config = PostgresSettings::try_new(
            "postgresql://localhost/test",
            NonZeroU32::new(7).unwrap(),
            StatementTimeout::Custom(Duration::from_millis(2500)),
            NonZeroU32::new(2).unwrap(),
            NonZeroU32::new(3).unwrap(),
        )
        .unwrap();
        let plan = config.pool_plan("encore-backend");
        let ping = ConnectionPingPolicy::Idle(Duration::from_millis(100));
        let capped = |size| PostgresPool {
            max_connections: NonZeroU32::new(size).unwrap(),
            min_connections: None,
            idle_timeout: None,
            ping,
            statement_timeout: StatementTimeout::Custom(Duration::from_millis(2500)),
        };
        assert_eq!((plan.main, plan.projection), (capped(7), capped(3)));
        assert_eq!(
            plan.history,
            PostgresPool {
                max_connections: NonZeroU32::new(2).unwrap(),
                min_connections: Some(0),
                idle_timeout: Some(Duration::from_secs(30)),
                ping,
                statement_timeout: StatementTimeout::Unlimited,
            }
        );
        assert_eq!(plan.application_name, "encore-backend");
        let (main, history) = (plan.main.options(), plan.history.options());
        assert_eq!(
            (
                main.get_max_connections(),
                main.get_min_connections(),
                main.get_idle_timeout()
            ),
            (7, 0, Some(Duration::from_secs(600))),
            "SQLx's minimum and idle timeout where the plan sets none"
        );
        assert_eq!(
            (
                history.get_max_connections(),
                history.get_min_connections(),
                history.get_idle_timeout()
            ),
            (2, 0, Some(Duration::from_secs(30)))
        );
        let described = plan.describe();
        for (key, value) in [
            ("postgres.main.statement_timeout", "2500 ms"),
            ("postgres.history.statement_timeout", "unlimited"),
            ("postgres.projection.ping", "idle 100 ms"),
        ] {
            assert_eq!(described.get(key).map(String::as_str), Some(value), "{key} in {described:?}");
        }
        let always = PostgresPool {
            ping: ConnectionPingPolicy::Default,
            ..plan.main
        };
        let ping = always
            .entries("main")
            .into_iter()
            .find(|(key, _)| key == "postgres.main.ping");
        assert_eq!(ping.map(|(_, value)| value).as_deref(), Some("always"));
    }

    #[test]
    fn checked_postgres_settings_refuse_a_timeout_postgresql_cannot_hold() {
        let one = NonZeroU32::new(1).unwrap();
        let refused = PostgresSettings::try_new("u", one, StatementTimeout::Custom(Duration::ZERO), one, one).unwrap_err();
        assert_eq!(
            refused.to_string(),
            "DATABASE_STATEMENT_TIMEOUT_MS: must be 0 (no cap) or 1..=2147483647 milliseconds"
        );
        let unlimited = PostgresSettings::try_new("u", one, StatementTimeout::Unlimited, one, one).unwrap();
        assert_eq!(
            (unlimited.url(), unlimited.statement_timeout(), unlimited.history_pool_size()),
            ("u", StatementTimeout::Unlimited, one)
        );
        assert_eq!(
            PostgresSettings::default().projection_pool_size(),
            DEFAULT_PROJECTION_POOL_SIZE
        );
    }

    /// G-01, G-05, G-07: the executor of an aggregate over PostgreSQL is the core of the settings, as in memory.
    /// An executor over PostgreSQL is the command executor over the outbox repository: it prints its attempt, and
    /// a store that does not answer is `Unavailable` and heard as such by its observer.
    #[tokio::test]
    async fn an_executor_over_postgres_prints_its_attempt_and_reports_an_unreachable_store_to_its_observer() {
        use crate::{
            command::{
                CommandGatewayError,
                executor::StoreCommandExecutor,
                locks::LockLimits,
                observer::{CommandObserver, CommandReport, Outcome},
                retry::AggregateConflictRetryPolicy,
            },
            engine::test_support::{Book, Show},
            journal::persisted::PersistedEventStore,
            postgres::outbox::PostgresEventRepositoryWithOutbox,
        };
        use std::sync::{Arc, Mutex};
        /// Notes the outcome of every command.
        #[derive(Default)]
        struct Heard(Mutex<Vec<&'static str>>);
        impl CommandObserver<Show> for Heard {
            fn command(&self, report: &CommandReport<'_, Show>) {
                self.0.lock().unwrap().push(match report.outcome {
                    Outcome::Unavailable => "unavailable",
                    Outcome::Accepted { .. } | Outcome::Rejected(_) | Outcome::Conflict => "ran",
                });
            }
        }
        // Nobody listens on port 1: the first acquire fails at once.
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(100))
            .connect_lazy("postgres://nobody@127.0.0.1:1/none")
            .unwrap();
        let store = PersistedEventStore::new(PostgresEventRepositoryWithOutbox::new(pool)).with_snapshots(NonZeroUsize::MIN);
        let heard = Arc::new(Heard::default());
        let executor =
            StoreCommandExecutor::<Show, _>::new(store, (), AggregateConflictRetryPolicy::default(), LockLimits::default())
                .with_commit_wakeup(crate::journal::wakeup::CommitWakeup::new())
                .with_observer(heard.clone());
        let printed = format!("{executor:?}");
        assert!(
            printed.starts_with(
                r#"StoreCommandExecutor { attempt: CommandAttempt { aggregate_type: "show", task_planner: false, .. }"#
            ),
            "{printed}"
        );
        assert!(
            printed.contains("commit_wakeup: true") && printed.contains("observer: true"),
            "{printed}"
        );
        let book = Book {
            seat: "A1".into(),
            fan: "fox".into(),
            booking_id: None,
        };
        assert_eq!(
            executor.execute(&"show".into(), book).await.map(|report| report.events.len()),
            Err(CommandGatewayError::Unavailable)
        );
        assert_eq!(*heard.0.lock().unwrap(), ["unavailable"]);
    }

    #[tokio::test]
    async fn the_executors_over_postgres_take_the_settings_of_the_engine() {
        use crate::{
            command::{locks::LockLimits, retry::AggregateConflictRetryPolicy},
            engine::{Cqrs, Executor, test_support::*},
            projection::processor::config::ProcessorConfig,
            worker::runtime::config::TaskRuntimeConfig,
        };
        let lazy = || {
            PgPoolOptions::new()
                .acquire_timeout(Duration::from_millis(200))
                .connect_lazy_with("postgresql://shop@127.0.0.1:1/shop".parse().unwrap())
        };
        let limits = LockLimits::try_new(Duration::from_millis(500), 7).unwrap();
        let retry = AggregateConflictRetryPolicy::try_new(5, Duration::from_millis(1), Duration::from_millis(2), 0.0).unwrap();
        let settings = Settings::try_new(
            NonZeroUsize::new(3).unwrap(),
            limits,
            ProcessorConfig::default(),
            TaskRuntimeConfig::default(),
        )
        .unwrap()
        .with_conflict_retry(retry);
        let pools = PostgresPools {
            main: lazy(),
            history: lazy(),
            projection: lazy(),
        };
        let cqrs = Cqrs::with_settings(settings)
            .event_store(Postgres)
            .postgres_pools(pools)
            .module(shows)
            .build()
            .await
            .unwrap();
        let Some(Executor::Postgres(executor)) = cqrs.executor_of::<Show>() else {
            panic!("the executor of an engine over PostgreSQL is PostgreSQL's");
        };
        assert_eq!(
            (
                executor.lock_limits(),
                executor.retry_policy(),
                executor.store().snapshot_interval()
            ),
            (limits, retry, Some(3))
        );
        assert!(
            cqrs.history::<Show>(&"s-1".into()).await.is_err(),
            "nobody listens on the history pool"
        );
        assert!(cqrs.documents::<Hall, Show>().is_some(), "the views in memory");
    }
}
