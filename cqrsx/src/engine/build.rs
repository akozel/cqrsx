//! How an engine is built: its settings are read, its modules set up, their registrations merged and checked
//! (`Registrations`), its stores opened, and the components assembled from the registrations over the stores.
#[cfg(feature = "mongodb")]
use crate::engine::mongodb::MongoDb;
#[cfg(feature = "postgres")]
use crate::engine::postgres::{Postgres, PostgresPools};
use crate::{
    cqrs::projection::runtime::ProjectionRuntime,
    engine::{
        Cqrs, Journal,
        memory::MemoryBackend,
        module::{AggregateEntry, Module, ViewEntry},
        settings::{Defaults, Settings, SettingsError},
        setup::{Lookup, Setup, SetupError},
        views::CheckpointOps,
    },
    task::{
        adapter::boxed::BoxedTaskAdapter,
        context::Executors,
        dependencies::Dependencies,
        dispatcher::{TaskDispatcher, TaskDispatcherError},
        handlers::TaskHandlers,
        policy::{Policies, PolicyError, TaskPolicy},
    },
};
#[cfg(test)]
use crate::{
    cqrs::{
        aggregate::Aggregate,
        command::{CommandInfo, observer::CommandObserver},
        projection::processor::observer::ProjectionObserver,
        rejection::Rejection,
    },
    engine::Commands,
    task::dependencies::MissingDependency,
};
#[cfg(test)]
use std::any::TypeId;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Why an engine could not be built: its settings, the first refusal of its modules, or a store it could not open.
#[derive(Debug, Error)]
pub enum BuildError {
    #[error("{0}")]
    Settings(#[from] SettingsError),
    #[error("{0}")]
    Setup(#[from] SetupError),
    #[error("the aggregate type `{0}` is registered twice: one executor per aggregate type")]
    AggregateTwice(&'static str),
    #[error("the view `{0}` is registered twice: one projection per name")]
    ViewTwice(&'static str),
    #[error("the module `{0}` registers a second observer of the commands or of the processors")]
    ObserverTwice(&'static str),
    #[error("task policies: {0}")]
    Policy(#[from] PolicyError),
    #[error("task handlers: {0}")]
    Tasks(#[from] TaskDispatcherError),
    /// Views in PostgreSQL or MongoDB hold positions of a durable journal in their checkpoints.
    #[error("the views in {0} need the journal in PostgreSQL")]
    ViewsWithoutJournal(&'static str),
    #[cfg(feature = "postgres")]
    #[error("{variable}: {source}")]
    DatabaseUrl { variable: String, source: sqlx::Error },
    #[cfg(feature = "postgres")]
    #[error("cannot connect to PostgreSQL: {0}")]
    Pool(sqlx::Error),
    #[cfg(feature = "postgres")]
    #[error("cannot connect the history pool to PostgreSQL: {0}")]
    HistoryPool(sqlx::Error),
    #[cfg(feature = "postgres")]
    #[error("cannot connect the projection pool to PostgreSQL: {0}")]
    ProjectionPool(sqlx::Error),
    #[cfg(feature = "mongodb")]
    #[error("{variable}: {source}")]
    MongoUri {
        variable: String,
        source: ::mongodb::error::Error,
    },
    #[cfg(feature = "mongodb")]
    #[error("MongoDB client: {0}")]
    MongoClient(::mongodb::error::Error),
}

type ModuleRoot = Box<dyn FnOnce(&Setup) -> Result<Module, SetupError>>;

/// Where the views of an engine live: `Builder::projections` takes one of these, PostgreSQL or MongoDB; without it
/// the views are in memory.
pub trait ProjectionStore: sealed::Sealed {}

mod sealed {
    /// The views' store an engine is built with.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Views {
        Memory,
        #[cfg(feature = "postgres")]
        Postgres,
        #[cfg(feature = "mongodb")]
        MongoDb,
    }

    pub trait Sealed {
        const VIEWS: Views;
    }
}

use sealed::Views;

#[cfg(feature = "postgres")]
impl sealed::Sealed for Postgres {
    const VIEWS: Views = Views::Postgres;
}

#[cfg(feature = "postgres")]
impl ProjectionStore for Postgres {}

#[cfg(feature = "mongodb")]
impl sealed::Sealed for MongoDb {
    const VIEWS: Views = Views::MongoDb;
}

#[cfg(feature = "mongodb")]
impl ProjectionStore for MongoDb {}

/// The stores an engine is built over, and what the application opened for them.
struct Stores {
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    postgres: bool,
    views: Views,
    #[cfg(feature = "postgres")]
    pools: Option<PostgresPools>,
    #[cfg(feature = "mongodb")]
    client: Option<::mongodb::Client>,
}

/// What an engine is built from: its settings (given, or read from the variables when it is built, with the
/// application's defaults), the roots of its modules and the variables they read, its stores, and the dependencies
/// the builder provides instead of theirs (a test replaces a module's by type).
pub struct Builder {
    settings: Option<Settings>,
    prefix: String,
    lookup: Lookup,
    defaults: Defaults,
    stores: Stores,
    modules: Vec<ModuleRoot>,
    dependencies: Dependencies,
}

impl Builder {
    pub(crate) fn new(settings: Option<Settings>, prefix: String, lookup: Lookup) -> Self {
        Self {
            settings,
            prefix,
            lookup,
            defaults: Defaults::LIBRARY,
            stores: Stores {
                postgres: false,
                views: Views::Memory,
                #[cfg(feature = "postgres")]
                pools: None,
                #[cfg(feature = "mongodb")]
                client: None,
            },
            modules: Vec::new(),
            dependencies: Dependencies::default(),
        }
    }

    /// The application's values where a variable is absent, and its name (`Cqrs::from_env`, `Cqrs::from_lookup`).
    pub fn defaults(mut self, defaults: Defaults) -> Self {
        self.defaults = defaults;
        self
    }

    /// The journal, the snapshots, the outbox and the task queue in PostgreSQL: three pools as the settings plan them,
    /// opened when the engine is built (one connection each: an unreachable database fails the build).
    #[cfg(feature = "postgres")]
    pub fn event_store(mut self, _: Postgres) -> Self {
        self.stores.postgres = true;
        self
    }

    /// The views and their checkpoints in PostgreSQL (the main pool) or MongoDB (the database of the settings); the
    /// journal must be in PostgreSQL.
    #[cfg(any(feature = "postgres", feature = "mongodb"))]
    pub fn projections<S: ProjectionStore>(mut self, _: S) -> Self {
        self.stores.views = S::VIEWS;
        self
    }

    /// The pools of the engine over PostgreSQL as the application opened them (e.g. pools that connect lazily, for a
    /// test over a database nobody listens on): the engine opens none.
    #[cfg(feature = "postgres")]
    pub fn postgres_pools(mut self, pools: PostgresPools) -> Self {
        self.stores.pools = Some(pools);
        self
    }

    /// The client of the views in MongoDB as the application made it: the engine makes none.
    #[cfg(feature = "mongodb")]
    pub fn mongodb_client(mut self, client: ::mongodb::Client) -> Self {
        self.stores.client = Some(client);
        self
    }

    /// The module its `root` makes when the engine is built.
    pub fn module(mut self, root: impl FnOnce(&Setup) -> Result<Module, SetupError> + 'static) -> Self {
        self.modules.push(Box::new(root));
        self
    }

    /// `dependency` instead of what a module provides of its type `T`.
    pub fn provide<T: ?Sized + Send + Sync + 'static>(mut self, dependency: Arc<T>) -> Self {
        self.dependencies = self.dependencies.provide(dependency);
        self
    }

    /// The task handlers of the modules with their dependencies (the builder's winning), checked as `build` checks
    /// them, without the engine: for a runtime or a test of one's own, which adds the command executors and builds them.
    pub fn task_handlers(self) -> Result<TaskHandlers, BuildError> {
        let mut registrations = self.registrations()?;
        Ok(registrations.take_task_handlers())
    }

    /// The engine: its settings, its modules set up and checked, its stores opened (or taken from the application),
    /// its components assembled over them; or the first refusal.
    pub async fn build(self) -> Result<Cqrs, BuildError> {
        let Registrations {
            mut cqrs,
            aggregates,
            views,
            adapters,
            stores,
        } = self.registrations()?;
        cqrs.open(stores).await?;
        for entry in aggregates {
            // The rules are gone only when a test took them.
            entry.rules.map_or(Ok(()), |rules| (entry.install)(&mut cqrs, rules))?;
        }
        for entry in views {
            (entry.install)(&mut cqrs);
        }
        // A dispatcher needs a handler: an engine without task types runs none.
        if !adapters.is_empty() {
            let dispatcher = TaskDispatcher::assemble(adapters, cqrs.dependencies.clone(), cqrs.executors.clone())?;
            cqrs.dispatcher = Some(Arc::new(dispatcher));
        }
        Ok(cqrs)
    }

    /// The settings, then what the modules registered, merged and checked as `build` checks them (an aggregate type and
    /// a view name once, a policy declared once and bound to its task types, one observer of each kind), over stores
    /// in memory not yet chosen.
    pub(crate) fn registrations(self) -> Result<Registrations, BuildError> {
        let settings = match self.settings {
            Some(settings) => settings,
            None => Settings::from_lookup(&self.prefix, &*self.lookup, &self.defaults)?,
        };
        let durable_views = match self.stores.views {
            Views::Memory => None,
            #[cfg(feature = "postgres")]
            Views::Postgres => Some("PostgreSQL"),
            #[cfg(feature = "mongodb")]
            Views::MongoDb => Some("MongoDB"),
        };
        if let Some(store) = durable_views
            && !self.stores.postgres
        {
            return Err(BuildError::ViewsWithoutJournal(store));
        }
        let setup = Setup::over(self.prefix, self.lookup, settings);
        let modules = self
            .modules
            .into_iter()
            .map(|root| root(&setup))
            .collect::<Result<Vec<_>, _>>()?;
        let shutdown = CancellationToken::new();
        let settings = setup.engine().clone();
        let memory = MemoryBackend::new();
        let checkpoints: Arc<dyn CheckpointOps> = memory.checkpoints.clone();
        let mut cqrs = Cqrs {
            owner: settings.node_name().map_or_else(|| Uuid::new_v4().to_string(), str::to_owned),
            settings,
            policies: Policies::default(),
            dependencies: Dependencies::default(),
            command_observer: None,
            projection_observers: None,
            commands: Default::default(),
            executors: Executors::default(),
            wakeups: Default::default(),
            dispatcher: None,
            views: Default::default(),
            described: setup.described(),
            memory,
            journal: Journal::Memory,
            #[cfg(feature = "postgres")]
            view_store: crate::engine::ViewStore::Memory,
            checkpoints,
            processors: Mutex::new(ProjectionRuntime::new()),
            #[cfg(feature = "http")]
            routes: axum::Router::new(),
            _stop: shutdown.clone().drop_guard(),
            shutdown,
        };
        for (name, retry, max_attempts) in modules.iter().flat_map(|module| &module.policies) {
            cqrs.policies = cqrs.policies.with_policy(*name, TaskPolicy::new(*retry, *max_attempts)?)?;
        }
        let (mut registered, mut named) = (HashSet::new(), HashSet::new());
        let (mut aggregates, mut views, mut adapters) = (Vec::new(), Vec::new(), Vec::new());
        for module in modules {
            for entry in module.aggregates {
                registered
                    .insert(entry.id)
                    .then_some(())
                    .ok_or(BuildError::AggregateTwice(entry.name))?;
                aggregates.push(entry);
            }
            for entry in module.views {
                named
                    .insert(entry.name)
                    .then_some(())
                    .ok_or(BuildError::ViewTwice(entry.name))?;
                views.push(entry);
            }
            for (adapter, binding) in module.tasks {
                adapters.push(adapter.map_err(TaskDispatcherError::from)?);
                if let Some((name, bind)) = binding {
                    cqrs.policies = bind(cqrs.policies, name)?;
                }
            }
            cqrs.dependencies = cqrs.dependencies.merged(&module.dependencies);
            let twice = BuildError::ObserverTwice(module.name);
            for observer in module.command_observers {
                if cqrs.command_observer.replace(observer).is_some() {
                    return Err(twice);
                }
            }
            for observers in module.projection_observers {
                if cqrs.projection_observers.replace(observers).is_some() {
                    return Err(twice);
                }
            }
            #[cfg(feature = "http")]
            if let Some(routes) = module.routes {
                cqrs.routes = std::mem::take(&mut cqrs.routes).merge(routes);
            }
        }
        cqrs.dependencies = cqrs.dependencies.merged(&self.dependencies);
        Ok(Registrations {
            cqrs,
            aggregates,
            views,
            adapters,
            stores: self.stores,
        })
    }
}

impl Cqrs {
    /// Opens the stores the builder chose, or takes those the application opened: the pools of PostgreSQL in the order
    /// of the plan (main, projection, history), the client of MongoDB (lazy).
    async fn open(&mut self, stores: Stores) -> Result<(), BuildError> {
        #[cfg(feature = "postgres")]
        if stores.postgres {
            let pools = match stores.pools {
                Some(pools) => pools,
                None => crate::engine::postgres::open(&self.settings).await?,
            };
            match stores.views {
                Views::Postgres => {
                    self.checkpoints = Arc::new(crate::postgres::projection_checkpoints::PostgresCheckpointStore::new(
                        pools.main.clone(),
                    ));
                    self.view_store = crate::engine::ViewStore::Postgres;
                }
                #[cfg(feature = "mongodb")]
                Views::MongoDb => {
                    let client = match stores.client {
                        Some(client) => client,
                        None => crate::engine::mongodb::client(&self.settings).await?,
                    };
                    let database = client.database(self.settings.mongodb().read_db());
                    self.checkpoints = Arc::new(crate::mongodb::projection_checkpoints::MongoCheckpointStore::new(&database));
                    self.view_store = crate::engine::ViewStore::MongoDb(database);
                }
                Views::Memory => {}
            }
            self.journal = Journal::Postgres(pools);
        }
        let _ = stores;
        Ok(())
    }
}

/// What the modules of an engine registered, merged and checked ([`Builder::registrations`]), with the engine they are
/// installed into and the stores it is built over.
pub(crate) struct Registrations {
    cqrs: Cqrs,
    aggregates: Vec<AggregateEntry>,
    views: Vec<ViewEntry>,
    adapters: Vec<BoxedTaskAdapter>,
    stores: Stores,
}

impl Registrations {
    #[cfg(test)]
    pub(crate) fn settings(&self) -> &Settings {
        &self.cqrs.settings
    }

    #[cfg(test)]
    /// The policies of the task types: the reactions plan with them, the task runtime pauses by them.
    pub(crate) fn policies(&self) -> &Policies {
        &self.cqrs.policies
    }

    #[cfg(test)]
    /// The aggregate types, in the order of the registrations.
    pub(crate) fn aggregate_types(&self) -> Vec<&'static str> {
        self.aggregates.iter().map(|entry| entry.name).collect()
    }

    #[cfg(test)]
    /// The names of the views, in the order of the registrations.
    pub(crate) fn view_names(&self) -> Vec<&'static str> {
        self.views.iter().map(|entry| entry.name).collect()
    }

    #[cfg(test)]
    /// The rules of the aggregate type `A`, once.
    pub(crate) fn take_rules<A: Aggregate + 'static>(&mut self) -> Option<A::Rules> {
        let entry = self.aggregates.iter_mut().find(|entry| entry.id == TypeId::of::<A>())?;
        entry.rules.take()?.downcast::<A::Rules>().ok().map(|rules| *rules)
    }

    #[cfg(test)]
    /// The observer of every command, as the observer of the commands of `A`.
    pub(crate) fn command_observer<A: Aggregate>(&self) -> Option<Arc<dyn CommandObserver<A>>>
    where
        A::Command: CommandInfo,
        A::Error: Rejection,
    {
        Some(Arc::new(Commands(self.cqrs.command_observer.clone()?)))
    }

    #[cfg(test)]
    /// The observer of the processor of the view named `projection` on this node.
    pub(crate) fn projection_observer(&self, projection: &'static str) -> Option<Arc<dyn ProjectionObserver>> {
        let observers = self.cqrs.projection_observers.as_ref()?;
        Some(observers(projection, &self.cqrs.owner))
    }

    #[cfg(test)]
    pub(crate) fn dependency<T: ?Sized + 'static>(&self) -> Result<Arc<T>, MissingDependency> {
        self.cqrs.dependency::<T>()
    }

    /// The task handlers with their dependencies, once: the assembly adds the command executors and builds them.
    pub(crate) fn take_task_handlers(&mut self) -> TaskHandlers {
        TaskHandlers::assembled(std::mem::take(&mut self.adapters), self.cqrs.dependencies.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cqrs::command::observer::{AnyCommandObserver, CommandReport},
        cqrs::testing::mem_view_repository::MemViewRepository,
        cqrs::{
            command::{
                locks::LockLimits,
                observer::{AnyCommandReport, AnyOutcome},
                retry::AggregateConflictRetryPolicy,
            },
            projection::processor::config::{DeliveryLimits, ProcessorConfig},
        },
        engine::test_support::*,
        task::{
            dependencies::DependencyId,
            dispatcher::TaskDispatcherError,
            format::{TaskFormat, TaskFormatError, TaskPayload},
            handler::TaskHandler,
            policy::Retry,
            result::TaskResult,
            runtime::config::TaskRuntimeConfig,
        },
    };
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::time::Duration;
    use uuid::Uuid;

    fn refusal(builder: Builder) -> BuildError {
        ready(builder.build()).err().unwrap()
    }

    fn module(root: impl FnOnce(Module) -> Module + 'static) -> impl FnOnce(&Setup) -> Result<Module, SetupError> {
        move |_| Ok(root(Module::new("test")))
    }

    #[test]
    fn a_second_registration_of_an_aggregate_type_is_refused() {
        let twice = refusal(Cqrs::in_memory().module(shows).module(wallets).module(shows));
        assert!(matches!(twice, BuildError::AggregateTwice("show")), "{twice:?}");
        assert_eq!(
            twice.to_string(),
            "the aggregate type `show` is registered twice: one executor per aggregate type"
        );
        let in_one_module = module(|module| module.aggregate::<Show>(()).aggregate::<Show>(()));
        assert!(matches!(
            Cqrs::in_memory().module(in_one_module).registrations().err(),
            Some(BuildError::AggregateTwice("show"))
        ));
    }

    #[test]
    fn a_view_name_registered_twice_is_refused() {
        let twice = module(|module| module.view::<Hall, Show>().view::<Hall, Show>());
        let refused = refusal(Cqrs::in_memory().module(twice));
        assert!(matches!(refused, BuildError::ViewTwice("halls")), "{refused:?}");
        assert_eq!(
            refused.to_string(),
            "the view `halls` is registered twice: one projection per name"
        );
    }

    #[derive(Serialize, Deserialize)]
    struct Unversioned {}

    impl TaskPayload for Unversioned {
        const TASK_TYPE: &'static str = "unversioned";
        const TASK_VERSION: i32 = 0;
    }

    struct UnversionedHandler;

    impl TaskHandler<Unversioned> for UnversionedHandler {
        async fn handle(&self, _: Unversioned, _: &crate::task::context::TaskContext) -> TaskResult {
            TaskResult::Done
        }
    }

    #[test]
    fn a_policy_must_be_declared_once_with_a_budget_and_bound_once() {
        let unknown = module(|module| module.task(RecordInWallet).policy(STEP));
        let twice = module(|module| {
            let one_second = Retry::fixed(Duration::from_secs(1));
            module.policy(STEP, one_second, 3).policy(STEP, one_second, 3)
        });
        let no_budget = module(|module| module.policy(STEP, Retry::fixed(Duration::from_secs(1)), 0));
        let bound_twice = module(|module| {
            let module = module.policy(STEP, Retry::fixed(Duration::from_secs(1)), 3);
            module.task(RecordInWallet).policy(STEP).task(RecordInWallet).policy(STEP)
        });
        let refused = [
            refusal(Cqrs::in_memory().module(unknown)),
            refusal(Cqrs::in_memory().module(twice)),
            refusal(Cqrs::in_memory().module(no_budget)),
            refusal(Cqrs::in_memory().module(bound_twice)),
        ];
        assert!(
            refused.iter().all(|refused| matches!(refused, BuildError::Policy(_))),
            "{refused:?}"
        );
        let expected = [
            PolicyError::Unknown(STEP),
            PolicyError::DefinedTwice(STEP),
            PolicyError::MaxAttempts(0),
            PolicyError::BoundTwice(TaskFormat::of::<RecordTicket>().unwrap()),
        ];
        assert_eq!(
            refused.map(|refused| refused.to_string()),
            expected.map(|error| format!("task policies: {error}"))
        );
    }

    #[tokio::test]
    async fn a_handler_without_its_dependency_and_a_task_format_twice_or_invalid_are_refused() {
        let missing = module(|module| module.task(SendMail).without_policy());
        let twice = module(|module| {
            let module = module.provide(Arc::new(Mailbox::default()));
            module.task(SendMail).without_policy().task(SendMail).without_policy()
        });
        let invalid = module(|module| module.task(UnversionedHandler).without_policy());
        let refused = [
            refusal(Cqrs::in_memory().module(missing)),
            refusal(Cqrs::in_memory().module(twice)),
        ];
        assert!(
            refused.iter().all(|refused| matches!(refused, BuildError::Tasks(_))),
            "{refused:?}"
        );
        let refused = refused.map(|refused| refused.to_string().replacen("task handlers: ", "", 1));
        let mail = TaskFormat::of::<MailTicket>().unwrap();
        assert_eq!(
            refused,
            [
                TaskDispatcherError::MissingDependency {
                    format: mail.clone(),
                    dependency: DependencyId::of::<Mailbox>().name()
                }
                .to_string(),
                TaskDispatcherError::DuplicateFormat(mail).to_string()
            ]
        );
        assert!(matches!(
            refusal(Cqrs::in_memory().module(invalid)),
            BuildError::Tasks(TaskDispatcherError::InvalidFormat(TaskFormatError::InvalidVersion))
        ));
        let ctx = crate::task::context::TaskContext::new(
            crate::task::attempt::TaskAttempt::new(Uuid::nil(), TaskFormat::new("other", 1).unwrap(), json!({}), json!({}), 1)
                .unwrap(),
            Duration::ZERO,
        );
        let answered = UnversionedHandler.handle(Unversioned {}, &ctx).await;
        assert_eq!(
            answered,
            TaskResult::Done,
            "a handler of its own, refused only for its format"
        );
    }

    #[test]
    fn a_module_whose_setup_fails_fails_the_build() {
        let limit = |setup: &Setup| setup.env::<u32>("LIMIT", 1).map(|_| Module::new("limited"));
        let builder = Cqrs::from_lookup("APP", |name| (name == "APP_LIMIT").then(|| "x".to_owned()));
        let refused = refusal(builder.module(limit));
        assert_eq!(
            refused.to_string(),
            r#"APP_LIMIT: cannot parse "x": invalid digit found in string"#
        );
        assert!(matches!(refused, BuildError::Setup(SetupError::Invalid { .. })));
    }

    #[test]
    fn one_observer_of_each_kind_per_engine() {
        let commands = || module(|module| module.observe_commands(Arc::new(Log::default())));
        let refused = refusal(Cqrs::in_memory().module(commands()).module(commands()));
        assert!(matches!(refused, BuildError::ObserverTwice("test")), "{refused:?}");
        let projections = module(|module| {
            let observer = |_: &'static str, _: &str| -> Arc<dyn ProjectionObserver> { Arc::new(Silent) };
            module.observe_projections(observer).observe_projections(observer)
        });
        assert_eq!(
            refusal(Cqrs::in_memory().module(projections)).to_string(),
            "the module `test` registers a second observer of the commands or of the processors"
        );
    }

    struct Silent;

    impl ProjectionObserver for Silent {}

    #[tokio::test]
    async fn the_last_provide_wins_so_a_test_replaces_a_modules_dependency() {
        let (cqrs, mailbox) = engine().await;
        assert!(Arc::ptr_eq(&cqrs.dependency::<Mailbox>().unwrap(), &mailbox));
        assert_eq!(
            cqrs.dependency::<String>().unwrap_err().to_string(),
            "the dependency `alloc::string::String` is not provided"
        );
        cqrs.from("ui")
            .execute::<Show>(&"s-1".into(), book("A1", "fox"))
            .await
            .unwrap();
        cqrs.run_tasks().await;
        assert_eq!(mailbox.sent(), ["fox:A1"], "the letter went to the mailbox of the test");
    }

    #[tokio::test]
    async fn the_engine_builds_every_executor_and_processor_with_its_settings() {
        let processor = ProcessorConfig::new(
            Duration::from_millis(300),
            50,
            Duration::from_secs(30),
            DeliveryLimits::default(),
        )
        .unwrap();
        let limits = LockLimits::new(Duration::from_millis(500), 7).unwrap();
        let retry = AggregateConflictRetryPolicy::new(5, Duration::from_millis(1), Duration::from_millis(2), 0.0).unwrap();
        let settings = Settings::new(3, limits, processor, TaskRuntimeConfig::default())
            .unwrap()
            .with_conflict_retry(retry)
            .with_node_name("node-a");
        let defaults = Cqrs::in_memory().module(shows).registrations().unwrap();
        let set = Cqrs::with_settings(settings).module(shows).registrations().unwrap();
        let mut built = Vec::new();
        for registrations in [defaults, set] {
            let mut cqrs = registrations.cqrs;
            let executor = cqrs.executor::<Show, _>(cqrs.store::<Show>(), ());
            let checkpoints = Arc::new(crate::cqrs::testing::mem_checkpoint_store::MemCheckpointStore::default());
            let documents = Arc::new(MemViewRepository::<Hall, Show>::default());
            let journal = Arc::new(cqrs.memory.journal.clone());
            let processor = cqrs.processor::<Hall, Show, _, _, _>(journal, checkpoints, documents);
            let (owner, config) = processor.settings();
            built.push((
                executor.lock_limits(),
                executor.retry_policy(),
                executor.framework().store().snapshot_interval(),
                *config,
                owner == cqrs.owner,
            ));
        }
        assert_eq!(
            built,
            [
                (
                    LockLimits::default(),
                    AggregateConflictRetryPolicy::default(),
                    Some(10),
                    ProcessorConfig::default(),
                    true
                ),
                (limits, retry, Some(3), processor, true),
            ],
            "the library's defaults, then the settings"
        );
        let named = Cqrs::with_settings(Settings::default().with_node_name("node-a"));
        assert_eq!(named.registrations().unwrap().cqrs.owner, "node-a");
        // The reactions plan with the budget of each task type's policy.
        let (cqrs, _) = engine().await;
        let booked = cqrs
            .from("ui")
            .execute::<Show>(&"s-1".into(), book("A1", "fox"))
            .await
            .unwrap();
        assert_eq!(booked.tasks[0].max_attempts(), 3, "STEP's budget");
    }

    /// Writes the aggregate type, the command and the outcome of every command.
    #[derive(Default)]
    struct Log(std::sync::Mutex<Vec<String>>);

    impl AnyCommandObserver for Log {
        fn command(&self, report: &AnyCommandReport<'_>) {
            let outcome = match &report.outcome {
                AnyOutcome::Rejected(rejection) => format!("{} {}", rejection.code(), rejection.params()),
                outcome => (if matches!(outcome, AnyOutcome::Accepted { .. }) {
                    "accepted"
                } else {
                    "other"
                })
                .to_owned(),
            };
            let (name, labels) = (report.command.name(), report.command.labels());
            self.0
                .lock()
                .unwrap()
                .push(format!("{} {name} {labels:?} {outcome}", report.aggregate_type));
        }
    }

    /// The projections it was asked to observe, with the node.
    #[derive(Default)]
    struct Observed(std::sync::Mutex<Vec<String>>);

    #[tokio::test]
    async fn the_observers_of_a_module_hear_every_command_and_every_processor() {
        let (log, observed) = (Arc::new(Log::default()), Arc::new(Observed::default()));
        let (feed, seen) = (log.clone(), observed.clone());
        let observers = move |_: &Setup| -> Result<Module, SetupError> {
            Ok(Module::new("feed").observe_commands(feed).observe_projections(
                move |projection, owner| -> Arc<dyn ProjectionObserver> {
                    seen.0.lock().unwrap().push(format!("{projection}@{owner}"));
                    Arc::new(Silent)
                },
            ))
        };
        let node = Settings::default().with_node_name("node-a");
        let cqrs = Cqrs::with_settings(node)
            .module(shows)
            .module(wallets)
            .module(observers)
            .build()
            .await
            .unwrap();
        assert_eq!(cqrs.settings().node_name(), Some("node-a"));
        let ui = cqrs.from("ui");
        ui.execute::<Show>(&"s-1".into(), book("A1", "fox")).await.unwrap();
        ui.execute::<Show>(&"s-1".into(), book("A1", "panda")).await.unwrap_err();
        cqrs.run_tasks().await;
        let again = ui.execute::<Wallet>(&"fox".into(), Record { seat: "A1".into() }).await;
        assert_eq!(
            again.unwrap_err(),
            crate::cqrs::command::CommandGatewayError::Domain(AlreadyRecorded)
        );
        assert_eq!(
            *log.0.lock().unwrap(),
            [
                r#"show Book [("fan", "fox")] accepted"#,
                r#"show Book [("fan", "panda")] seat_taken {}"#,
                "wallet Record [] accepted",
                "wallet Record [] already_recorded {}"
            ]
        );
        assert_eq!(*observed.0.lock().unwrap(), ["halls@node-a", "tickets@node-a"]);
    }

    #[tokio::test]
    async fn registrations_hand_out_what_the_modules_registered() {
        let log = Arc::new(Log::default());
        let feed = log.clone();
        let observers = move |_: &Setup| -> Result<Module, SetupError> {
            Ok(Module::new("feed")
                .observe_commands(feed)
                .observe_projections(|_, _| Arc::new(Silent)))
        };
        let builder = Cqrs::in_memory().module(shows).module(wallets).module(observers);
        let mut registrations = builder.provide(Arc::new(String::from("extra"))).registrations().unwrap();
        assert_eq!(registrations.aggregate_types(), ["show", "wallet"]);
        assert_eq!(registrations.view_names(), ["halls", "tickets"]);
        assert_eq!(registrations.settings().snapshot_every().get(), 10);
        assert_eq!(registrations.take_rules::<Show>(), Some(()));
        assert_eq!(registrations.take_rules::<Show>(), None, "once");
        assert_eq!(
            registrations.take_rules::<crate::cqrs::aggregate::tests::Ledger>(),
            None,
            "not an aggregate type of the modules"
        );
        assert_eq!(
            registrations
                .policies()
                .max_attempts(&TaskFormat::of::<MailTicket>().unwrap()),
            6
        );
        assert_eq!(*registrations.dependency::<String>().unwrap(), "extra");
        assert!(registrations.dependency::<u8>().is_err());
        assert!(registrations.projection_observer("halls").is_some());
        let observer = registrations.command_observer::<Show>().unwrap();
        let metadata = std::collections::HashMap::new();
        observer.command(&CommandReport {
            aggregate_id: &"s-1".to_owned(),
            command: &book("A1", "fox"),
            metadata: &metadata,
            outcome: crate::cqrs::command::observer::Outcome::Accepted { tasks: &[] },
        });
        observer.command(&CommandReport {
            aggregate_id: &"s-1".to_owned(),
            command: &book("A2", "fox"),
            metadata: &metadata,
            outcome: crate::cqrs::command::observer::Outcome::Unavailable,
        });
        assert_eq!(
            *log.0.lock().unwrap(),
            [
                r#"show Book [("fan", "fox")] accepted"#,
                r#"show Book [("fan", "fox")] other"#
            ]
        );
        let dispatcher = registrations.take_task_handlers().build().unwrap();
        assert_eq!(
            dispatcher.formats(),
            [
                TaskFormat::of::<RecordTicket>().unwrap(),
                TaskFormat::of::<MailTicket>().unwrap()
            ]
        );
        let mut bare = Cqrs::in_memory().module(shows).registrations().unwrap();
        assert!(bare.command_observer::<Show>().is_none() && bare.projection_observer("halls").is_none());
        assert!(bare.take_task_handlers().build().is_err(), "no task type");
    }

    #[test]
    fn views_outside_memory_need_the_journal_in_postgres() {
        #[cfg(feature = "postgres")]
        assert_eq!(
            refusal(Cqrs::in_memory().projections(crate::engine::postgres::Postgres).module(shows)).to_string(),
            "the views in PostgreSQL need the journal in PostgreSQL"
        );
        #[cfg(feature = "mongodb")]
        assert!(matches!(
            refusal(Cqrs::in_memory().projections(crate::engine::mongodb::MongoDb).module(shows)),
            BuildError::ViewsWithoutJournal("MongoDB")
        ));
        let settings = refusal(Cqrs::from_lookup("APP", |name| {
            (name == "APP_SNAPSHOT_EVERY").then(|| "x".to_owned())
        }));
        assert!(matches!(settings, BuildError::Settings(_)), "{settings:?}");
    }

    #[tokio::test]
    async fn task_handlers_are_the_modules_with_their_dependencies() {
        let mailbox = Arc::new(Mailbox::default());
        let handlers = Cqrs::in_memory()
            .module(shows)
            .module(wallets)
            .provide(mailbox.clone())
            .task_handlers()
            .unwrap();
        let dispatcher = handlers.build().unwrap();
        assert_eq!(
            dispatcher.formats(),
            [
                TaskFormat::of::<RecordTicket>().unwrap(),
                TaskFormat::of::<MailTicket>().unwrap()
            ]
        );
        let attempt = crate::task::attempt::TaskAttempt::first(
            &MailTicket {
                to: "fox".into(),
                seat: "A1".into(),
            },
            json!({}),
        )
        .unwrap();
        assert_eq!(dispatcher.dispatch(attempt, Duration::ZERO).await, TaskResult::Done);
        assert_eq!(mailbox.sent(), ["fox:A1"], "the builder's mailbox, not the module's");
        let refused = Cqrs::in_memory().module(shows).module(shows).task_handlers().err().unwrap();
        assert!(matches!(refused, BuildError::AggregateTwice("show")));
    }
}
