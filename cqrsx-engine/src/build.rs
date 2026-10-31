//! How an engine is built: its settings are read, its modules set up, their registrations merged and checked
//! (`Registrations`), its stores opened, and the components assembled from the registrations over the stores.
#[cfg(feature = "mongodb")]
use crate::mongodb::MongoDb;
use crate::{
    Cqrs, Journal,
    executor::{Commands, Executor},
    memory::MemoryBackend,
    module::{AggregateEntry, Module, ObservedProcessor, ViewEntry},
    settings::{Defaults, Settings, SettingsError},
    setup::{Lookup, Setup, SetupError},
    views::{CheckpointOps, Counts},
};
#[cfg(feature = "postgres")]
use crate::{
    ViewStore,
    postgres::{Postgres, PostgresPools},
};
#[cfg(feature = "postgres")]
use cqrsx::postgres::{
    checkpoint_store::PostgresCheckpointStore, event_repository::PostgresEventRepository,
    outbox::PostgresEventRepositoryWithOutbox, view_repository::PostgresViewRepository,
};
use cqrsx::{
    command::{executor::StoreCommandExecutor, observer::CommandObserver},
    domain::{aggregate::Aggregate, command::Command, rejection::Rejection},
    journal::{EventStore, persisted::PersistedEventStore, repository::PersistedEventRepository, wakeup::CommitWakeup},
    memory::event_repository::MemEventRepository,
    mutex::get_mut,
    projection::{
        RebuildableProjection, checkpoint::CheckpointStore, processor::ProjectionProcessor, runtime::ProjectionRuntime,
    },
    reactions::{Reactions, ReactionsPlanner},
    task::policy::{Policies, PolicyError, TaskPolicy},
    view::{View, projection::ViewProjection, repository::ViewRepository},
    worker::{
        adapter::boxed::BoxedTaskAdapter,
        context::Executors,
        dependencies::Dependencies,
        dispatcher::{TaskDispatcher, TaskDispatcherError},
        handlers::TaskHandlers,
    },
};
#[cfg(test)]
use cqrsx::{projection::processor::observer::ProjectionObserver, worker::dependencies::MissingDependency};
use std::{
    any::{Any, TypeId},
    collections::{BTreeMap, HashSet, btree_map::Entry},
    fmt,
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
    /// One key of the engine's description ([`Cqrs::describe`]) with two values: from the engine's settings, a module
    /// (`Setup::describe`, `Setup::env`) or two modules. The same value twice is one entry.
    #[error("the key `{key}` is described twice, as `{first}` and as `{second}`")]
    DescribedTwice { key: String, first: String, second: String },
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
#[must_use = "an engine is made by `build`"]
pub struct Builder {
    settings: Option<Settings>,
    prefix: String,
    lookup: Lookup,
    defaults: Defaults,
    stores: Stores,
    modules: Vec<ModuleRoot>,
    dependencies: Dependencies,
}

// By hand: the settings hold the database URLs with their passwords, the stores hold pools and clients, the variables
// and the module roots are closures; none of them is printed.
impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder")
            .field("prefix", &self.prefix)
            .field("defaults", &self.defaults)
            .field("views", &self.stores.views)
            .field("modules", &self.modules.len())
            .field("dependencies", &self.dependencies)
            .finish_non_exhaustive()
    }
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
        let described = merge_described(settings.entries().into_iter().chain(setup.described()))?;
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
            described,
            memory,
            journal: Journal::Memory,
            #[cfg(feature = "postgres")]
            view_store: crate::ViewStore::Memory,
            checkpoints,
            processors: Mutex::new(ProjectionRuntime::new()),
            #[cfg(feature = "http")]
            routes: axum::Router::new(),
            _stop: shutdown.clone().drop_guard(),
            shutdown,
        };
        for policy in modules.iter().flat_map(|module| &module.policies) {
            cqrs.policies = cqrs
                .policies
                .with_policy(policy.name, TaskPolicy::new(policy.retry, policy.max_attempts))?;
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
            for task in module.tasks {
                adapters.push(task.adapter);
                if let Some(binding) = task.binding {
                    cqrs.policies = (binding.bind)(cqrs.policies, binding.name)?;
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
                None => crate::postgres::open(&self.settings).await?,
            };
            match stores.views {
                Views::Postgres => {
                    self.checkpoints = Arc::new(cqrsx::postgres::checkpoint_store::PostgresCheckpointStore::new(
                        pools.main.clone(),
                    ));
                    self.view_store = crate::ViewStore::Postgres;
                }
                #[cfg(feature = "mongodb")]
                Views::MongoDb => {
                    let client = match stores.client {
                        Some(client) => client,
                        None => crate::mongodb::client(&self.settings).await?,
                    };
                    let database = client.database(self.settings.mongodb().read_db());
                    self.checkpoints = Arc::new(cqrsx::mongodb::checkpoint_store::MongoCheckpointStore::new(&database));
                    self.view_store = crate::ViewStore::MongoDb(database);
                }
                Views::Memory => {}
            }
            self.journal = Journal::Postgres(pools);
        }
        let _ = stores;
        Ok(())
    }
}

/// The engine's description by key from `entries` in the order described: a key given again with its value is one
/// entry, with another value it is refused, naming the value given first and the other one.
fn merge_described(entries: impl IntoIterator<Item = (String, String)>) -> Result<BTreeMap<String, String>, BuildError> {
    let mut described = BTreeMap::new();
    for (key, value) in entries {
        match described.entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
            Entry::Occupied(entry) if *entry.get() == value => {}
            Entry::Occupied(entry) => {
                let (key, first) = entry.remove_entry();
                return Err(BuildError::DescribedTwice {
                    key,
                    first,
                    second: value,
                });
            }
        }
    }
    Ok(described)
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
        A::Command: Command,
        A::Error: Rejection,
    {
        Some(Arc::new(Commands(self.cqrs.command_observer.clone()?)))
    }

    #[cfg(test)]
    /// The observer of the processor of the view named `projection` on this node.
    pub(crate) fn projection_observer(&self, projection: &'static str) -> Option<Arc<dyn ProjectionObserver>> {
        let observers = self.cqrs.projection_observers.as_ref()?;
        Some(observers(ObservedProcessor {
            projection,
            node: &self.cqrs.owner,
        }))
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

impl Cqrs {
    /// Installs the executor of `A` over the engine's journal, with the rules its module registered: one per aggregate
    /// type, shared by the entry points (`from(..).execute`) and the task handlers (`ctx.execute`).
    pub(crate) fn install_aggregate<A>(&mut self, rules: Box<dyn Any + Send>) -> Result<(), BuildError>
    where
        A: Reactions + Clone + 'static,
        A::Command: Command + Clone,
        A::Error: Rejection + Send + 'static,
    {
        // The entry of `A` holds the rules of `A`.
        let Ok(rules) = rules.downcast::<A::Rules>() else {
            return Ok(());
        };
        let executor = match &self.journal {
            Journal::Memory => Executor::Memory(self.executor::<A, _>(self.store::<A>(), *rules)),
            #[cfg(feature = "postgres")]
            Journal::Postgres(pools) => Executor::Postgres(self.executor::<A, _>(self.postgres_store::<A>(pools), *rules)),
        };
        let executor = Arc::new(executor);
        let executors = self.executors.clone().with::<A, _>(executor.clone());
        self.executors = executors.map_err(|twice| BuildError::AggregateTwice(twice.0))?;
        self.commands.insert(TypeId::of::<A>(), executor);
        Ok(())
    }

    /// The store of the commands of `A` in PostgreSQL: the journal with its outbox, a snapshot every `snapshot_every`
    /// events.
    #[cfg(feature = "postgres")]
    pub(crate) fn postgres_store<A: Aggregate>(
        &self,
        pools: &PostgresPools,
    ) -> PersistedEventStore<PostgresEventRepositoryWithOutbox, A> {
        PersistedEventStore::new(PostgresEventRepositoryWithOutbox::new(pools.main.clone()))
            .with_snapshots(self.settings.snapshot_every())
    }

    /// The store of the commands of `A` in memory: the journal, a snapshot every `snapshot_every` events.
    fn store<A: Aggregate>(&self) -> PersistedEventStore<MemEventRepository, A> {
        PersistedEventStore::new(self.memory.journal.clone()).with_snapshots(self.settings.snapshot_every())
    }

    /// The executor of `A` over `store` as every backend builds it: the settings' conflict retry and lock limits, the
    /// reactions with the policies, the wake-up of the processors of `A`, the observer of every command.
    fn executor<A, S>(&mut self, store: S, rules: A::Rules) -> StoreCommandExecutor<A, S>
    where
        A: Reactions + 'static,
        A::Command: Command + Clone,
        A::Error: Rejection,
        S: EventStore<A>,
    {
        let settings = &self.settings;
        let executor = StoreCommandExecutor::new(store, rules, settings.conflict_retry(), settings.lock_limits())
            .with_task_planner(ReactionsPlanner::<A>::new(self.policies.clone()))
            .with_commit_wakeup(self.wakeup::<A>());
        match self.commands_observer::<A>() {
            Some(observer) => executor.with_observer(observer),
            None => executor,
        }
    }

    /// The observer of every command, as the observer of the commands of `A`.
    fn commands_observer<A: Aggregate>(&self) -> Option<Arc<dyn CommandObserver<A>>>
    where
        A::Command: Command,
        A::Error: Rejection,
    {
        let observer = self.command_observer.clone()?;
        Some(Arc::new(Commands(observer)))
    }

    /// Installs the processor of `V` over the engine's journal and checkpoints, and the repository of its documents,
    /// which the queries read.
    pub(crate) fn install_view<V: View<A> + PartialEq + 'static, A: Aggregate + 'static>(&mut self) {
        match &self.journal {
            Journal::Memory => {
                let documents = self.memory.documents::<V, A>();
                let journal = self.memory.journal.clone();
                let checkpoints = self.memory.checkpoints.clone();
                self.register_view::<V, A, _, _, _, _, _>(
                    Arc::new(journal.clone()),
                    checkpoints,
                    documents,
                    journal.clone(),
                    journal,
                );
            }
            #[cfg(feature = "postgres")]
            Journal::Postgres(pools) => {
                let feed = Arc::new(PostgresEventRepository::new(pools.projection.clone()));
                let history = PostgresEventRepository::new(pools.history.clone());
                let counter = PostgresEventRepository::new(pools.main.clone());
                let main = pools.main.clone();
                match &self.view_store {
                    ViewStore::Memory => {
                        let (documents, checkpoints) = (self.memory.documents::<V, A>(), self.memory.checkpoints.clone());
                        self.register_view::<V, A, _, _, _, _, _>(feed, checkpoints, documents, history, counter);
                    }
                    ViewStore::Postgres => {
                        let documents = Arc::new(PostgresViewRepository::<V, A>::new(main.clone()));
                        let checkpoints = Arc::new(PostgresCheckpointStore::new(main));
                        self.register_view::<V, A, _, _, _, _, _>(feed, checkpoints, documents, history, counter);
                    }
                    #[cfg(feature = "mongodb")]
                    ViewStore::MongoDb(database) => {
                        let documents = Arc::new(cqrsx::mongodb::view_repository::MongoViewRepository::<V, A>::new(database));
                        let checkpoints = Arc::new(cqrsx::mongodb::checkpoint_store::MongoCheckpointStore::new(database));
                        self.register_view::<V, A, _, _, _, _, _>(feed, checkpoints, documents, history, counter);
                    }
                }
            }
        }
    }

    /// The processor of `V` over `feed` and `checkpoints` into `documents`, and the view's queries and operations:
    /// verify against `history`, the lag counted on `counter`.
    fn register_view<V, A, R, C, D, H, K>(&mut self, feed: Arc<R>, checkpoints: Arc<C>, documents: Arc<D>, history: H, counter: K)
    where
        V: View<A> + PartialEq + 'static,
        A: Aggregate + 'static,
        R: PersistedEventRepository + 'static,
        C: CheckpointStore + 'static,
        D: ViewRepository<V, A> + 'static,
        H: PersistedEventRepository + Counts + 'static,
        K: Counts + 'static,
    {
        let processor = self.processor::<V, A, _, _, _>(feed, checkpoints, documents.clone());
        let processors = get_mut(&mut self.processors, "engine processors");
        *processors = std::mem::take(processors).with_processor(processor);
        self.views.register::<V, A, _, _, _>(documents, history, counter);
    }

    /// The processor of `V` as every backend builds it: the settings' configuration, this node as the owner, the
    /// wake-up of `A`, the observer of every processor.
    fn processor<V, A, R, C, D>(
        &mut self,
        journal: Arc<R>,
        checkpoints: Arc<C>,
        documents: Arc<D>,
    ) -> ProjectionProcessor<A, R, C>
    where
        V: View<A> + 'static,
        A: Aggregate + 'static,
        R: PersistedEventRepository + 'static,
        C: CheckpointStore + 'static,
        D: ViewRepository<V, A> + 'static,
    {
        let projection: Arc<dyn RebuildableProjection<A>> = Arc::new(ViewProjection::new(documents));
        let processor = ProjectionProcessor::new(V::NAME, journal, checkpoints, projection, self.settings.processor())
            .with_wakeup(self.wakeup::<A>())
            .with_owner(self.owner.clone());
        match &self.projection_observers {
            Some(observers) => processor.with_observer(observers(ObservedProcessor {
                projection: V::NAME,
                node: &self.owner,
            })),
            None => processor,
        }
    }

    /// The wake-up of the processors of `A`, shared by its executor.
    fn wakeup<A: 'static>(&mut self) -> CommitWakeup {
        self.wakeups.entry(TypeId::of::<A>()).or_default().clone()
    }

    /// The executor of `A`, if a module registered the aggregate type.
    pub(crate) fn executor_of<A: Aggregate + 'static>(&self) -> Option<&Executor<A>> {
        self.commands.get(&TypeId::of::<A>())?.downcast_ref::<Executor<A>>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use cqrsx::{
        command::observer::{AnyCommandObserver, CommandReport},
        command::{
            locks::LockLimits,
            observer::{AnyCommandReport, AnyOutcome},
            retry::AggregateConflictRetryPolicy,
        },
        memory::view_repository::MemViewRepository,
        projection::processor::config::{DeliveryLimits, ProcessorConfig},
        task::{format::TaskFormat, policy::Retry},
        view::View,
        worker::{TaskResult, dependencies::DependencyId, dispatcher::TaskDispatcherError, runtime::config::TaskRuntimeConfig},
    };
    use serde_json::json;
    use std::{
        num::{NonZeroU16, NonZeroUsize},
        time::Duration,
    };

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

    #[test]
    fn a_policy_must_be_declared_once_and_bound_once() {
        let three = NonZeroU16::new(3).unwrap();
        let unknown = module(|module| module.task(RecordInWallet).policy(STEP));
        let twice = module(move |module| {
            let one_second = Retry::fixed(Duration::from_secs(1));
            module.policy(STEP, one_second, three).policy(STEP, one_second, three)
        });
        let bound_twice = module(move |module| {
            let module = module.policy(STEP, Retry::fixed(Duration::from_secs(1)), three);
            module.task(RecordInWallet).policy(STEP).task(RecordInWallet).policy(STEP)
        });
        let refused = [
            refusal(Cqrs::in_memory().module(unknown)),
            refusal(Cqrs::in_memory().module(twice)),
            refusal(Cqrs::in_memory().module(bound_twice)),
        ];
        assert!(
            refused.iter().all(|refused| matches!(refused, BuildError::Policy(_))),
            "{refused:?}"
        );
        let expected = [
            PolicyError::Unknown(STEP),
            PolicyError::DefinedTwice(STEP),
            PolicyError::BoundTwice(TaskFormat::of::<RecordTicket>()),
        ];
        assert_eq!(
            refused.map(|refused| refused.to_string()),
            expected.map(|error| format!("task policies: {error}"))
        );
    }

    #[test]
    fn a_handler_without_its_dependency_and_a_task_format_twice_are_refused() {
        let missing = module(|module| module.task(SendMail).without_policy());
        let twice = module(|module| {
            let module = module.provide(Arc::new(Mailbox::default()));
            module.task(SendMail).without_policy().task(SendMail).without_policy()
        });
        let refused = [
            refusal(Cqrs::in_memory().module(missing)),
            refusal(Cqrs::in_memory().module(twice)),
        ];
        assert!(
            refused.iter().all(|refused| matches!(refused, BuildError::Tasks(_))),
            "{refused:?}"
        );
        let refused = refused.map(|refused| refused.to_string().replacen("task handlers: ", "", 1));
        let mail = TaskFormat::of::<MailTicket>();
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
    }

    /// What `refused` says of a key described twice: the key, the value given first and the other one.
    fn described_twice(refused: &BuildError) -> Option<(&str, &str, &str)> {
        match refused {
            BuildError::DescribedTwice { key, first, second } => Some((key, first, second)),
            _ => None,
        }
    }

    #[test]
    fn a_key_described_with_two_values_fails_the_build() {
        let lookup = || Cqrs::from_lookup("SHOP", |_| None);
        let snapshots = |setup: &Setup| -> Result<Module, SetupError> {
            setup.describe("SHOP_SNAPSHOT_EVERY", "11");
            Ok(Module::new("snapshots"))
        };
        let refused = refusal(lookup().module(snapshots));
        assert_eq!(
            described_twice(&refused),
            Some(("SHOP_SNAPSHOT_EVERY", "10", "11")),
            "a module against the engine's settings"
        );
        assert_eq!(
            refused.to_string(),
            "the key `SHOP_SNAPSHOT_EVERY` is described twice, as `10` and as `11`"
        );
        let three = |setup: &Setup| setup.env("RETRIES", 3).map(|_| Module::new("three"));
        let four = |setup: &Setup| setup.env("RETRIES", 4).map(|_| Module::new("four"));
        assert_eq!(
            described_twice(&refusal(lookup().module(three).module(four))),
            Some(("SHOP_RETRIES", "3", "4")),
            "two modules reading one variable with two defaults"
        );
        let again = |setup: &Setup| -> Result<Module, SetupError> {
            setup.describe("shop.timeout", "1 s");
            setup.describe("shop.timeout", "2 s");
            Ok(Module::new("again"))
        };
        assert_eq!(
            described_twice(&refusal(lookup().module(again))),
            Some(("shop.timeout", "1 s", "2 s")),
            "one module twice"
        );
    }

    #[test]
    fn a_key_described_again_with_its_value_is_one_entry() {
        let reader = |setup: &Setup| -> Result<Module, SetupError> {
            setup.env("LIMIT", 4)?;
            setup.describe("SHOP_SNAPSHOT_EVERY", "10");
            Ok(Module::new("reader"))
        };
        let other = |setup: &Setup| setup.env("LIMIT", 4).map(|_| Module::new("other"));
        let builder = Cqrs::from_lookup("SHOP", |name| (name == "SHOP_LIMIT").then(|| "5".to_owned()));
        let cqrs = ready(builder.module(reader).module(other).build()).unwrap();
        let described = cqrs.describe();
        assert_eq!(
            (described.get("SHOP_LIMIT"), described.get("SHOP_SNAPSHOT_EVERY")),
            (Some(&"5".to_owned()), Some(&"10".to_owned())),
            "two modules sharing a variable, a module repeating the engine's value"
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
            let observer = |_: ObservedProcessor<'_>| -> Arc<dyn ProjectionObserver> { Arc::new(Silent) };
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

    #[test]
    fn a_builder_a_setup_and_a_module_print_what_they_name_but_never_a_url() {
        let secret = |_: &str| Some("postgres://shop:s3cret@db/shop".to_owned());
        let builder = Cqrs::from_lookup("SHOP", secret).module(shows).module(wallets);
        assert_eq!(
            format!("{builder:?}"),
            format!(
                "Builder {{ prefix: \"SHOP\", defaults: {:?}, views: Memory, modules: 2, dependencies: Dependencies {{ count: 0 }}, .. }}",
                Defaults::LIBRARY
            )
        );
        let setup = Setup::new("SHOP", secret, Settings::default());
        setup.describe("shop.url", "postgres://shop:s3cret@db/shop");
        assert_eq!(format!("{setup:?}"), r#"Setup { prefix: "SHOP", .. }"#);
        assert_eq!(
            format!("{:?}", wallets(&setup).unwrap()),
            r#"Module { name: "wallets", aggregates: ["wallet"], views: ["tickets"], policies: [PolicyName("step"), PolicyName("mail")], tasks: 2, dependencies: Dependencies { count: 1 }, .. }"#
        );
        let entry = Module::new("mail").task(SendMail);
        let printed = format!("{entry:?}");
        assert!(
            printed.starts_with(r#"TaskEntry { module: Module { name: "mail""#),
            "{printed}"
        );
    }

    #[tokio::test]
    async fn a_processor_and_the_read_side_print_their_names() {
        let mut cqrs = Cqrs::in_memory().module(shows).registrations().unwrap().cqrs;
        let checkpoints = Arc::new(cqrsx::memory::checkpoint_store::MemCheckpointStore::default());
        let documents = Arc::new(MemViewRepository::<Hall, Show>::default());
        let journal = Arc::new(cqrs.memory.journal.clone());
        let processor = cqrs.processor::<Hall, Show, _, _, _>(journal, checkpoints, documents);
        assert_eq!(
            format!("{processor:?}"),
            format!(
                "ProjectionProcessor {{ name: {:?}, aggregate_type: {:?}, owner: {:?}, upcasters: 0, config: {:?}, .. }}",
                Hall::NAME,
                Show::TYPE,
                cqrs.owner,
                ProcessorConfig::default()
            )
        );
        let read_side = ProjectionRuntime::new().with_processor(processor);
        assert_eq!(
            format!("{read_side:?}"),
            format!("ProjectionRuntime {{ processors: [{:?}], .. }}", Hall::NAME)
        );
    }

    #[tokio::test]
    async fn the_engine_builds_every_executor_and_processor_with_its_settings() {
        let processor = ProcessorConfig::try_new(
            Duration::from_millis(300),
            50,
            Duration::from_secs(30),
            DeliveryLimits::default(),
        )
        .unwrap();
        let limits = LockLimits::try_new(Duration::from_millis(500), 7).unwrap();
        let retry = AggregateConflictRetryPolicy::try_new(5, Duration::from_millis(1), Duration::from_millis(2), 0.0).unwrap();
        let settings = Settings::try_new(NonZeroUsize::new(3).unwrap(), limits, processor, TaskRuntimeConfig::default())
            .unwrap()
            .with_conflict_retry(retry)
            .with_node_name("node-a");
        let defaults = Cqrs::in_memory().module(shows).registrations().unwrap();
        let set = Cqrs::with_settings(settings).module(shows).registrations().unwrap();
        let mut built = Vec::new();
        for registrations in [defaults, set] {
            let mut cqrs = registrations.cqrs;
            let store = cqrs.store::<Show>();
            let executor = cqrs.executor::<Show, _>(store, ());
            let checkpoints = Arc::new(cqrsx::memory::checkpoint_store::MemCheckpointStore::default());
            let documents = Arc::new(MemViewRepository::<Hall, Show>::default());
            let journal = Arc::new(cqrs.memory.journal.clone());
            let processor = cqrs.processor::<Hall, Show, _, _, _>(journal, checkpoints, documents);
            built.push((
                format!("{executor:?}"),
                format!("{:?}", cqrs.store::<Show>()),
                format!("{processor:?}"),
                cqrs.owner.clone(),
            ));
        }
        let executor = |retry: AggregateConflictRetryPolicy, limits: LockLimits| {
            format!("retry_policy: {retry:?}, lock_limits: {limits:?}, commit_wakeup: true, observer: false")
        };
        let printed = |owner: &str, config: ProcessorConfig| format!("owner: {owner:?}, upcasters: 0, config: {config:?}, ..");
        let [
            (defaults_executor, defaults_store, defaults_processor, defaults_owner),
            (set_executor, set_store, set_processor, set_owner),
        ] = &built[..]
        else {
            panic!("two engines were built");
        };
        assert!(
            defaults_executor.contains(&executor(AggregateConflictRetryPolicy::default(), LockLimits::default())),
            "the library's defaults: {defaults_executor}"
        );
        assert!(defaults_store.contains("snapshot_policy: Every(10)"), "{defaults_store}");
        assert!(
            defaults_processor.contains(&printed(defaults_owner, ProcessorConfig::default())),
            "{defaults_processor}"
        );
        assert!(
            set_executor.contains(&executor(retry, limits)),
            "the settings: {set_executor}"
        );
        assert!(set_store.contains("snapshot_policy: Every(3)"), "{set_store}");
        assert!(set_processor.contains(&printed(set_owner, processor)), "{set_processor}");
        assert_eq!(set_owner, "node-a");
        let named = Cqrs::with_settings(Settings::default().with_node_name("node-a"));
        assert_eq!(named.registrations().unwrap().cqrs.owner, "node-a");
        // The reactions plan with the budget of each task type's policy.
        let (cqrs, _) = engine().await;
        let booked = cqrs
            .from("ui")
            .execute::<Show>(&"s-1".into(), book("A1", "fox"))
            .await
            .unwrap();
        assert_eq!(booked.tasks[0].max_attempts().get(), 3, "STEP's budget");
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
                move |processor: ObservedProcessor<'_>| -> Arc<dyn ProjectionObserver> {
                    seen.0
                        .lock()
                        .unwrap()
                        .push(format!("{}@{}", processor.projection, processor.node));
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
            cqrsx::command::CommandGatewayError::Domain(AlreadyRecorded)
        );
        assert_eq!(
            *log.0.lock().unwrap(),
            [
                r#"show Book [Label { name: "fan", value: "fox" }] accepted"#,
                r#"show Book [Label { name: "fan", value: "panda" }] seat_taken {}"#,
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
                .observe_projections(|_| Arc::new(Silent)))
        };
        let builder = Cqrs::in_memory().module(shows).module(wallets).module(observers);
        let mut registrations = builder.provide(Arc::new(String::from("extra"))).registrations().unwrap();
        assert_eq!(registrations.aggregate_types(), ["show", "wallet"]);
        assert_eq!(registrations.view_names(), ["halls", "tickets"]);
        assert_eq!(registrations.settings().snapshot_every().get(), 10);
        assert_eq!(registrations.take_rules::<Show>(), Some(()));
        assert_eq!(registrations.take_rules::<Show>(), None, "once");
        assert_eq!(
            registrations.take_rules::<Stranger>(),
            None,
            "not an aggregate type of the modules"
        );
        assert_eq!(
            registrations.policies().max_attempts(&TaskFormat::of::<MailTicket>()).get(),
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
            outcome: cqrsx::command::observer::Outcome::Accepted { tasks: &[] },
        });
        observer.command(&CommandReport {
            aggregate_id: &"s-1".to_owned(),
            command: &book("A2", "fox"),
            metadata: &metadata,
            outcome: cqrsx::command::observer::Outcome::Unavailable,
        });
        assert_eq!(
            *log.0.lock().unwrap(),
            [
                r#"show Book [Label { name: "fan", value: "fox" }] accepted"#,
                r#"show Book [Label { name: "fan", value: "fox" }] other"#
            ]
        );
        let dispatcher = registrations.take_task_handlers().build().unwrap();
        assert_eq!(
            dispatcher.formats(),
            [TaskFormat::of::<RecordTicket>(), TaskFormat::of::<MailTicket>()]
        );
        let mut bare = Cqrs::in_memory().module(shows).registrations().unwrap();
        assert!(bare.command_observer::<Show>().is_none() && bare.projection_observer("halls").is_none());
        assert!(bare.take_task_handlers().build().is_err(), "no task type");
    }

    #[test]
    fn views_outside_memory_need_the_journal_in_postgres() {
        #[cfg(feature = "postgres")]
        assert_eq!(
            refusal(Cqrs::in_memory().projections(crate::postgres::Postgres).module(shows)).to_string(),
            "the views in PostgreSQL need the journal in PostgreSQL"
        );
        #[cfg(feature = "mongodb")]
        assert!(matches!(
            refusal(Cqrs::in_memory().projections(crate::mongodb::MongoDb).module(shows)),
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
            [TaskFormat::of::<RecordTicket>(), TaskFormat::of::<MailTicket>()]
        );
        let attempt = cqrsx::task::attempt::TaskAttempt::try_first(
            &MailTicket {
                to: "fox".into(),
                seat: "A1".into(),
            },
            json!({}),
        )
        .unwrap();
        assert_eq!(dispatcher.dispatch(Arc::new(attempt), Duration::ZERO).await, TaskResult::Done);
        assert_eq!(mailbox.sent(), ["fox:A1"], "the builder's mailbox, not the module's");
        let refused = Cqrs::in_memory().module(shows).module(shows).task_handlers().err().unwrap();
        assert!(matches!(refused, BuildError::AggregateTwice("show")));
    }
}
