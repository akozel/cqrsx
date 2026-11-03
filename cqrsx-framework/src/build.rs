//! How a framework is built: its settings are read, its modules set up, their registrations merged and checked into an
//! `Assembly`, its stores opened, and the components installed from the assembly over the stores.
#[cfg(feature = "projections-mongodb")]
use crate::mongodb::MongoDb;
#[cfg(feature = "event-store-postgres")]
use crate::postgres::Postgres;
#[cfg(feature = "event-store-postgres")]
use crate::postgres::PostgresPools;
use crate::{
    Cqrs,
    module::{AggregateEntry, Module, ObservedProcessor, ProjectionObserverFactory, ViewEntry},
    settings::{Defaults, Settings, SettingsError},
    setup::{Lookup, Setup, SetupError},
    stores::{EventStoreKind, Executor, Journal, ProjectionStoreKind, Stores},
    views::{CheckpointOperations, Counts, Registry},
};
use cqrsx::{
    command::{
        executor::StoreCommandExecutor,
        locks::LockLimits,
        observer::{AnyCommandObserver, AnyCommandReport, CommandObserver, CommandReport},
        retry::AggregateConflictRetryPolicy,
    },
    domain::{aggregate::Aggregate, command::Command, rejection::Rejection},
    journal::{AggregateStore, repository::EventRepository, wakeup::CommitWakeup},
    mutex::get_mut,
    projection::{
        RebuildableProjection, checkpoint::CheckpointStore, processor::ProjectionProcessor, runtime::ProjectionRuntime,
    },
    reactions::{Reactions, ReactionsPlanner},
    task::policy::{Policies, PolicyError},
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
    collections::{BTreeMap, HashMap, HashSet, btree_map::Entry},
    fmt,
    sync::{Arc, Mutex},
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Why a framework could not be built: its settings, the first refusal of its modules, or a store it could not open.
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
    /// One key of the framework's description ([`Cqrs::description`]) with two values: from the framework's settings, a module
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
    /// No `Builder::event_store(..)`: a framework has no default journal.
    #[error("the framework has no journal: give the builder `event_store(..)`")]
    NoJournal,
    /// Modules registered views and there is no `Builder::projections(..)`: a framework has no default store for them.
    #[error("the views have no store: give the builder `projections(..)`")]
    ViewsWithoutStore,
    #[cfg(feature = "event-store-postgres")]
    #[error("{variable}: {source}")]
    DatabaseUrl { variable: String, source: sqlx::Error },
    #[cfg(feature = "event-store-postgres")]
    #[error("cannot connect to PostgreSQL: {0}")]
    MainPool(sqlx::Error),
    #[cfg(feature = "event-store-postgres")]
    #[error("cannot connect the history pool to PostgreSQL: {0}")]
    HistoryPool(sqlx::Error),
    #[cfg(feature = "event-store-postgres")]
    #[error("cannot connect the projection pool to PostgreSQL: {0}")]
    ProjectionPool(sqlx::Error),
    #[cfg(feature = "projections-mongodb")]
    #[error("{variable}: {source}")]
    MongoUri {
        variable: String,
        source: ::mongodb::error::Error,
    },
    #[cfg(feature = "projections-mongodb")]
    #[error("MongoDB client: {0}")]
    MongoClient(::mongodb::error::Error),
}

type ModuleRoot = Box<dyn FnOnce(&Setup) -> Result<Module, SetupError>>;

/// Where the event store of a framework lives (its journal, snapshots, outbox and task queue): `Builder::event_store`
/// takes one of these, `Postgres` (feature `event-store-postgres`) or `InMemory` (feature `test-utils`). There is no default.
pub trait EventStore: sealed::SealedEventStore {}

/// Where the views of a framework live: `Builder::projections` takes one of these, `Postgres` (feature
/// `projections-postgres`), `MongoDb` (feature `projections-mongodb`) or `InMemory` (feature `test-utils`). There is no
/// default.
pub trait ProjectionStore: sealed::SealedProjectionStore {}

mod sealed {
    use crate::stores::{EventStoreKind, ProjectionStoreKind};

    pub trait SealedEventStore {
        const KIND: EventStoreKind;
    }

    pub trait SealedProjectionStore {
        const KIND: ProjectionStoreKind;
    }
}

#[cfg(feature = "test-utils")]
impl sealed::SealedEventStore for crate::memory::InMemory {
    const KIND: EventStoreKind = EventStoreKind::Memory;
}

#[cfg(feature = "test-utils")]
impl EventStore for crate::memory::InMemory {}

#[cfg(feature = "test-utils")]
impl sealed::SealedProjectionStore for crate::memory::InMemory {
    const KIND: ProjectionStoreKind = ProjectionStoreKind::Memory;
}

#[cfg(feature = "test-utils")]
impl ProjectionStore for crate::memory::InMemory {}

#[cfg(feature = "event-store-postgres")]
impl sealed::SealedEventStore for Postgres {
    const KIND: EventStoreKind = EventStoreKind::Postgres;
}

#[cfg(feature = "event-store-postgres")]
impl EventStore for Postgres {}

#[cfg(feature = "projections-postgres")]
impl sealed::SealedProjectionStore for Postgres {
    const KIND: ProjectionStoreKind = ProjectionStoreKind::Postgres;
}

#[cfg(feature = "projections-postgres")]
impl ProjectionStore for Postgres {}

#[cfg(feature = "projections-mongodb")]
impl sealed::SealedProjectionStore for MongoDb {
    const KIND: ProjectionStoreKind = ProjectionStoreKind::MongoDb;
}

#[cfg(feature = "projections-mongodb")]
impl ProjectionStore for MongoDb {}

/// What a framework is built from: its settings (given, or read from the variables when it is built, with the
/// application's defaults), the roots of its modules and the variables they read, its stores, and the dependencies
/// the builder provides instead of theirs (a test replaces a module's by type).
#[must_use = "a framework is made by `build`"]
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
            stores: Stores::default(),
            modules: Vec::new(),
            dependencies: Dependencies::default(),
        }
    }

    /// The application's values where a variable is absent, and its name (`Cqrs::from_env`, `Cqrs::from_lookup`).
    pub fn defaults(mut self, defaults: Defaults) -> Self {
        self.defaults = defaults;
        self
    }

    /// The journal, the snapshots, the outbox and the task queue. With `Postgres` they are in PostgreSQL: three
    /// pools as the settings plan them, opened when the framework is built (one connection each: an unreachable database
    /// fails the build). With `InMemory` (a test) they are in memory. A framework has no
    /// default: without this call the build is refused ([`BuildError::NoJournal`]).
    pub fn event_store<S: EventStore>(mut self, _: S) -> Self {
        self.stores.event_store = Some(S::KIND);
        self
    }

    /// The views and their checkpoints in PostgreSQL (the main pool), MongoDB (the database of the settings) or, in a
    /// test, `InMemory`; the journal of PostgreSQL and MongoDB views must be in PostgreSQL.
    /// A framework has no default: modules with views and no call are refused ([`BuildError::ViewsWithoutStore`]).
    pub fn projections<S: ProjectionStore>(mut self, _: S) -> Self {
        self.stores.views = Some(S::KIND);
        self
    }

    /// The pools of the framework over PostgreSQL as the application opened them (e.g. pools that connect lazily, for a
    /// test over a database nobody listens on): the framework opens none.
    #[cfg(feature = "event-store-postgres")]
    pub fn postgres_pools(mut self, pools: PostgresPools) -> Self {
        self.stores.pools = Some(pools);
        self
    }

    /// The client of the views in MongoDB as the application made it: the framework makes none.
    #[cfg(feature = "projections-mongodb")]
    pub fn mongodb_client(mut self, client: ::mongodb::Client) -> Self {
        self.stores.client = Some(client);
        self
    }

    /// The module its `root` makes when the framework is built.
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
    /// them, without the framework: for a runtime or a test of one's own, which adds the command executors and builds them.
    pub fn task_handlers(self) -> Result<TaskHandlers, BuildError> {
        let mut assembly = self.assemble()?;
        Ok(assembly.take_task_handlers())
    }

    /// The framework: its settings, its modules set up and checked, its stores opened (or taken from the application),
    /// its components assembled over them; or the first refusal.
    pub async fn build(self) -> Result<Cqrs, BuildError> {
        self.assemble()?.open().await
    }

    /// The settings, then what the modules registered, merged and checked as `build` checks them (an aggregate type and
    /// a view name once, a task type once, with its policy, one observer of each kind), before the stores are opened.
    pub(crate) fn assemble(self) -> Result<Assembly, BuildError> {
        let settings = match self.settings {
            Some(settings) => settings,
            None => Settings::from_lookup(&self.prefix, &*self.lookup, &self.defaults)?,
        };
        if let Some(store) = self.stores.views.and_then(ProjectionStoreKind::durable_name)
            && !self.stores.event_store.is_some_and(EventStoreKind::is_durable)
        {
            return Err(BuildError::ViewsWithoutJournal(store));
        }
        let setup = Setup::from_parts(self.prefix, self.lookup, settings);
        let modules = self
            .modules
            .into_iter()
            .map(|root| root(&setup))
            .collect::<Result<Vec<_>, _>>()?;
        let settings = setup.settings().clone();
        let described = merge_described(settings.entries().into_iter().chain(setup.described()))?;
        let (mut dependencies, mut command_observer, mut projection_observer) = (Dependencies::default(), None, None);
        #[cfg(feature = "axum")]
        let mut routes = axum::Router::new();
        let mut policies = Policies::builder();
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
                policies = (task.register)(policies, task.policy);
            }
            dependencies = dependencies.merged(&module.dependencies);
            let twice = BuildError::ObserverTwice(module.name);
            for observer in module.command_observers {
                if command_observer.replace(observer).is_some() {
                    return Err(twice);
                }
            }
            for observers in module.projection_observers {
                if projection_observer.replace(observers).is_some() {
                    return Err(twice);
                }
            }
            #[cfg(feature = "axum")]
            if let Some(module_routes) = module.routes {
                routes = routes.merge(module_routes);
            }
        }
        Ok(Assembly {
            owner: settings.node_name().map_or_else(|| Uuid::new_v4().to_string(), str::to_owned),
            settings,
            policies: policies.build()?,
            dependencies: dependencies.merged(&self.dependencies),
            command_observer,
            projection_observer,
            described,
            #[cfg(feature = "axum")]
            routes,
            aggregates,
            views,
            adapters,
            stores: self.stores,
        })
    }
}

impl Assembly {
    /// The framework: its stores opened (or taken from the application), its components installed over them from the
    /// assembly; or the first refusal.
    pub(crate) async fn open(mut self) -> Result<Cqrs, BuildError> {
        let event_store = self.stores.ensure_chosen(!self.views.is_empty())?;
        let aggregates = std::mem::take(&mut self.aggregates);
        let views = std::mem::take(&mut self.views);
        let adapters = std::mem::take(&mut self.adapters);
        let stores = std::mem::take(&mut self.stores);
        let journal = stores.open(event_store, &self.settings).await?;
        let mut cqrs = self.over(journal);
        for entry in aggregates {
            // The rules are gone only when a test took them.
            entry.rules.map_or(Ok(()), |rules| (entry.install)(&mut cqrs, rules))?;
        }
        for entry in views {
            (entry.install)(&mut cqrs)?;
        }
        // A dispatcher needs a handler: a framework without task types runs none.
        if !adapters.is_empty() {
            let dispatcher = TaskDispatcher::assemble(adapters, cqrs.dependencies.clone(), cqrs.executors.clone())?;
            cqrs.dispatcher = Some(Arc::new(dispatcher));
        }
        Ok(cqrs)
    }

    /// The framework before its components: the assembly over the opened stores, the one place that makes a [`Cqrs`].
    fn over(self, journal: Journal) -> Cqrs {
        let shutdown = CancellationToken::new();
        Cqrs {
            settings: self.settings,
            owner: self.owner,
            policies: self.policies,
            dependencies: self.dependencies,
            command_observer: self.command_observer,
            projection_observer: self.projection_observer,
            command_executors: Default::default(),
            executors: Executors::default(),
            wakeups: Default::default(),
            dispatcher: None,
            views: Default::default(),
            described: self.described,
            journal,
            processors: Mutex::new(ProjectionRuntime::new()),
            #[cfg(feature = "axum")]
            routes: self.routes,
            _stop: shutdown.clone().drop_guard(),
            shutdown,
        }
    }
}

/// The framework's description by key from `entries` in the order described: a key given again with its value is one
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

/// What the settings and the modules of a framework gave it, merged and checked ([`Builder::assemble`]), and the stores
/// it is built over: [`open`](Self::open) makes the framework of them.
pub(crate) struct Assembly {
    settings: Settings,
    /// This node: the owner of the processors' checkpoints and of the claimed tasks.
    owner: String,
    policies: Policies,
    dependencies: Dependencies,
    command_observer: Option<Arc<dyn AnyCommandObserver>>,
    projection_observer: Option<ProjectionObserverFactory>,
    /// The settings and what the modules read and described when they were set up, by key.
    described: BTreeMap<String, String>,
    #[cfg(feature = "axum")]
    routes: axum::Router<Arc<Cqrs>>,
    aggregates: Vec<AggregateEntry>,
    views: Vec<ViewEntry>,
    adapters: Vec<BoxedTaskAdapter>,
    stores: Stores,
}

impl Assembly {
    #[cfg(test)]
    pub(crate) fn settings(&self) -> &Settings {
        &self.settings
    }

    #[cfg(test)]
    /// The policies of the task types: the reactions plan with them, the task runtime pauses by them.
    pub(crate) fn policies(&self) -> &Policies {
        &self.policies
    }

    #[cfg(test)]
    /// The aggregate types, in the order of the assembly.
    pub(crate) fn aggregate_types(&self) -> Vec<&'static str> {
        self.aggregates.iter().map(|entry| entry.name).collect()
    }

    #[cfg(test)]
    /// The names of the views, in the order of the assembly.
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
        A::Rejection: Rejection,
    {
        Some(Arc::new(AnyObserverAdapter(self.command_observer.clone()?)))
    }

    #[cfg(test)]
    /// The observer of the processor of the view named `projection` on this node.
    pub(crate) fn projection_observer(&self, projection: &'static str) -> Option<Arc<dyn ProjectionObserver>> {
        let observers = self.projection_observer.as_ref()?;
        Some(observers(ObservedProcessor {
            projection,
            node: &self.owner,
        }))
    }

    #[cfg(test)]
    pub(crate) fn dependency<T: ?Sized + 'static>(&self) -> Result<Arc<T>, MissingDependency> {
        self.dependencies
            .shared::<T>()
            .ok_or(MissingDependency(std::any::type_name::<T>()))
    }

    /// The task handlers with their dependencies, once: the assembly adds the command executors and builds them.
    pub(crate) fn take_task_handlers(&mut self) -> TaskHandlers {
        TaskHandlers::from_adapters(std::mem::take(&mut self.adapters), self.dependencies.clone())
    }
}

impl Cqrs {
    /// Installs the executor of `A` over the framework's journal, with the rules its module registered: one per aggregate
    /// type, shared by the entry points (`source(..).execute`) and the task handlers (`ctx.execute`).
    pub(crate) fn install_aggregate<A>(&mut self, rules: Box<dyn Any + Send>) -> Result<(), BuildError>
    where
        A: Reactions + Clone + 'static,
        A::Command: Command + Clone,
        A::Rejection: Rejection + Send + 'static,
    {
        // The entry of `A` holds the rules of `A`.
        let Ok(rules) = rules.downcast::<A::Rules>() else {
            return Ok(());
        };
        let settings = &self.settings;
        let parts = ExecutorParts::<A> {
            rules: *rules,
            conflict_retry: settings.conflict_retry(),
            lock_limits: settings.lock_limits(),
            planner: ReactionsPlanner::<A>::new(self.policies.clone()),
            wakeup: wakeup::<A>(&mut self.wakeups),
            observer: self.command_observer::<A>(),
        };
        let executor = self.journal.executor::<A>(settings.snapshot_every(), parts);
        let executor = Arc::new(executor);
        let executors = self.executors.clone().register::<A, _>(executor.clone());
        self.executors = executors.map_err(|twice| BuildError::AggregateTwice(twice.0))?;
        self.command_executors.insert(TypeId::of::<A>(), executor);
        Ok(())
    }

    /// The observer of every command, as the observer of the commands of `A`.
    fn command_observer<A: Aggregate>(&self) -> Option<Arc<dyn CommandObserver<A>>>
    where
        A::Command: Command,
        A::Rejection: Rejection,
    {
        let observer = self.command_observer.clone()?;
        Some(Arc::new(AnyObserverAdapter(observer)))
    }

    /// Installs the processor of `V` over the framework's journal and checkpoints, and the repository of its documents,
    /// which the queries read.
    pub(crate) fn install_view<V: View<A> + PartialEq + 'static, A: Aggregate + 'static>(&mut self) -> Result<(), BuildError> {
        let parts = ViewParts {
            settings: &self.settings,
            owner: &self.owner,
            projection_observer: &self.projection_observer,
            wakeups: &mut self.wakeups,
            processors: &mut self.processors,
            views: &mut self.views,
        };
        self.journal.install_view::<V, A>(parts)
    }

    /// The executor of `A`, if a module registered the aggregate type.
    pub(crate) fn executor_of<A: Aggregate + 'static>(&self) -> Option<&Executor<A>> {
        self.command_executors.get(&TypeId::of::<A>())?.downcast_ref::<Executor<A>>()
    }
}

/// The wake-up of the processors of `A` in `wakeups`, shared by its executor.
fn wakeup<A: 'static>(wakeups: &mut HashMap<TypeId, CommitWakeup>) -> CommitWakeup {
    wakeups.entry(TypeId::of::<A>()).or_default().clone()
}

/// The observer of every command as the observer of the commands of one aggregate type.
struct AnyObserverAdapter(Arc<dyn AnyCommandObserver>);

impl<A: Aggregate> CommandObserver<A> for AnyObserverAdapter
where
    A::Command: Command,
    A::Rejection: Rejection,
{
    fn observe(&self, report: &CommandReport<'_, A>) {
        self.0.observe(&AnyCommandReport::of(report));
    }
}

/// What the executor of one aggregate type is made of apart from its store: the journal builds the store of the variant
/// it keeps and hands it to [`over`](Self::over).
pub(crate) struct ExecutorParts<A: Reactions> {
    pub(crate) rules: A::Rules,
    pub(crate) conflict_retry: AggregateConflictRetryPolicy,
    pub(crate) lock_limits: LockLimits,
    pub(crate) planner: ReactionsPlanner<A>,
    pub(crate) wakeup: CommitWakeup,
    pub(crate) observer: Option<Arc<dyn CommandObserver<A>>>,
}

impl<A> ExecutorParts<A>
where
    A: Reactions + 'static,
    A::Command: Command + Clone,
    A::Rejection: Rejection,
{
    /// The executor of `A` over `store` as every store builds it: the settings' conflict retry and lock limits, the
    /// reactions with the policies, the wake-up of the processors of `A`, the observer of every command.
    pub(crate) fn over<S: AggregateStore<A>>(self, store: S) -> StoreCommandExecutor<A, S> {
        let executor = StoreCommandExecutor::new(store, self.rules, self.conflict_retry, self.lock_limits)
            .with_task_planner(self.planner)
            .with_commit_wakeup(self.wakeup);
        match self.observer {
            Some(observer) => executor.with_observer(observer),
            None => executor,
        }
    }
}

/// What the registration of a view takes from the framework: borrows of its fields, so that the journal, which the
/// installation borrows too, stays apart from them.
pub(crate) struct ViewParts<'a> {
    pub(crate) settings: &'a Settings,
    /// This node: the owner of the processors' checkpoints.
    pub(crate) owner: &'a str,
    pub(crate) projection_observer: &'a Option<ProjectionObserverFactory>,
    pub(crate) wakeups: &'a mut HashMap<TypeId, CommitWakeup>,
    pub(crate) processors: &'a mut Mutex<ProjectionRuntime>,
    pub(crate) views: &'a mut Registry,
}

impl ViewParts<'_> {
    /// The processor of `V` over `feed` and `checkpoints` into `documents`, and the view's queries and operations: its
    /// checkpoint rows are read and requested through `checkpoints`, its documents verified against `history`, its lag
    /// counted on `counter`.
    pub(crate) fn register<V, A, R, C, D, H, K>(
        mut self,
        feed: Arc<R>,
        checkpoints: Arc<C>,
        documents: Arc<D>,
        history: H,
        counter: K,
    ) where
        V: View<A> + PartialEq + 'static,
        A: Aggregate + 'static,
        R: EventRepository + 'static,
        C: CheckpointStore + 'static,
        D: ViewRepository<V, A> + 'static,
        H: EventRepository + Counts + 'static,
        K: Counts + 'static,
    {
        let operations: Arc<dyn CheckpointOperations> = checkpoints.clone();
        let processor = self.processor::<V, A, _, _, _>(feed, checkpoints, documents.clone());
        let processors = get_mut(self.processors, "framework processors");
        *processors = std::mem::take(processors).with_processor(processor);
        self.views.register::<V, A, _, _, _>(documents, operations, history, counter);
    }

    /// The processor of `V` as every store builds it: the settings' configuration, this node as the owner, the wake-up of
    /// `A`, the observer of every processor.
    fn processor<V, A, R, C, D>(
        &mut self,
        journal: Arc<R>,
        checkpoints: Arc<C>,
        documents: Arc<D>,
    ) -> ProjectionProcessor<A, R, C>
    where
        V: View<A> + 'static,
        A: Aggregate + 'static,
        R: EventRepository + 'static,
        C: CheckpointStore + 'static,
        D: ViewRepository<V, A> + 'static,
    {
        let projection: Arc<dyn RebuildableProjection<A>> = Arc::new(ViewProjection::new(documents));
        let processor = ProjectionProcessor::new(V::NAME, journal, checkpoints, projection, self.settings.processor())
            .with_commit_wakeup(wakeup::<A>(self.wakeups))
            .with_owner(self.owner.to_owned());
        match self.projection_observer {
            Some(observers) => processor.with_observer(observers(ObservedProcessor {
                projection: V::NAME,
                node: self.owner,
            })),
            None => processor,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{memory::InMemory, test_support::*};
    use cqrsx::{
        command::observer::{AnyCommandObserver, CommandReport},
        command::{
            locks::LockLimits,
            observer::{AnyCommandReport, AnyOutcome},
            retry::AggregateConflictRetryPolicy,
        },
        memory::view_repository::MemoryViewRepository,
        projection::processor::config::{DeliveryLimits, ProcessorConfig},
        task::{
            format::TaskFormat,
            policy::{RetryPause, TaskPolicy},
        },
        view::View,
        worker::{TaskResult, dependencies::DependencyId, dispatcher::TaskDispatcherError, runtime::config::TaskRuntimeConfig},
    };
    use serde_json::json;
    use std::{
        num::{NonZeroU16, NonZeroUsize},
        time::Duration,
    };

    fn view_parts(cqrs: &mut Cqrs) -> ViewParts<'_> {
        ViewParts {
            settings: &cqrs.settings,
            owner: &cqrs.owner,
            projection_observer: &cqrs.projection_observer,
            wakeups: &mut cqrs.wakeups,
            processors: &mut cqrs.processors,
            views: &mut cqrs.views,
        }
    }

    fn refusal(builder: Builder) -> BuildError {
        ready(builder.build()).err().unwrap()
    }

    fn module(root: impl FnOnce(Module) -> Module + 'static) -> impl FnOnce(&Setup) -> Result<Module, SetupError> {
        move |_| Ok(root(Module::new("test")))
    }

    #[test]
    fn a_second_registration_of_an_aggregate_type_is_refused() {
        let twice = refusal(memory_builder().module(shows).module(wallets).module(shows));
        assert!(matches!(twice, BuildError::AggregateTwice("show")), "{twice:?}");
        assert_eq!(
            twice.to_string(),
            "the aggregate type `show` is registered twice: one executor per aggregate type"
        );
        let in_one_module = module(|module| module.aggregate::<Show>(()).aggregate::<Show>(()));
        assert!(matches!(
            Cqrs::with_settings(Settings::default())
                .module(in_one_module)
                .assemble()
                .err(),
            Some(BuildError::AggregateTwice("show"))
        ));
    }

    #[test]
    fn a_view_name_registered_twice_is_refused() {
        let twice = module(|module| module.view::<Hall, Show>().view::<Hall, Show>());
        let refused = refusal(memory_builder().module(twice));
        assert!(matches!(refused, BuildError::ViewTwice("halls")), "{refused:?}");
        assert_eq!(
            refused.to_string(),
            "the view `halls` is registered twice: one projection per name"
        );
    }

    #[test]
    fn a_task_type_registered_twice_is_refused() {
        let policy = TaskPolicy::default();
        let in_one_module = module(move |module| module.task(RecordInWallet, policy).task(RecordInWallet, policy));
        let in_two_modules = |_: &Setup| Ok(Module::new("again").task(RecordInWallet, TaskPolicy::default()));
        let refused = [
            refusal(memory_builder().module(in_one_module)),
            refusal(memory_builder().module(wallets).module(in_two_modules)),
        ];
        assert!(
            refused.iter().all(|refused| matches!(refused, BuildError::Policy(_))),
            "{refused:?}"
        );
        let expected = PolicyError::TaskTwice(TaskFormat::of::<RecordTicket>());
        assert_eq!(
            refused.map(|refused| refused.to_string()),
            [format!("task policies: {expected}"), format!("task policies: {expected}")]
        );
        assert_eq!(expected.to_string(), "the task type `record_ticket v1` is registered twice");
    }

    #[test]
    fn a_task_type_has_the_policy_it_was_registered_with() {
        let slow = TaskPolicy::new(RetryPause::fixed(Duration::from_secs(9)), NonZeroU16::new(2).unwrap());
        let roots = module(move |module| module.provide(Arc::new(Mailbox::default())).task(RecordInWallet, slow));
        let assembly = Cqrs::with_settings(Settings::default()).module(roots).assemble().unwrap();
        let policies = assembly.policies();
        assert_eq!(policies.policy_for(&TaskFormat::of::<RecordTicket>()), slow);
        let defaults = Cqrs::with_settings(Settings::default()).module(wallets).assemble().unwrap();
        assert_eq!(
            defaults.policies().policy_for(&TaskFormat::of::<RecordTicket>()),
            TaskPolicy::new(RetryPause::fixed(Duration::from_secs(1)), NonZeroU16::new(3).unwrap())
        );
        assert_eq!(
            defaults.policies().policy_for(&TaskFormat::of::<MailTicket>()),
            TaskPolicy::new(
                RetryPause::backoff(Duration::from_secs(1), Duration::from_secs(8)),
                NonZeroU16::new(6).unwrap()
            )
        );
        let kept = module(|module| {
            module
                .provide(Arc::new(Mailbox::default()))
                .task(SendMail, TaskPolicy::default())
        });
        let kept = Cqrs::with_settings(Settings::default()).module(kept).assemble().unwrap();
        assert_eq!(
            kept.policies().policy_for(&TaskFormat::of::<MailTicket>()),
            TaskPolicy::default()
        );
    }

    #[test]
    fn a_handler_without_its_dependency_is_refused() {
        let missing = module(|module| module.task(SendMail, TaskPolicy::default()));
        let refused = refusal(memory_builder().module(missing));
        assert!(matches!(refused, BuildError::Tasks(_)), "{refused:?}");
        assert_eq!(
            refused.to_string().replacen("task handlers: ", "", 1),
            TaskDispatcherError::MissingDependency {
                format: TaskFormat::of::<MailTicket>(),
                dependency: DependencyId::of::<Mailbox>().name()
            }
            .to_string()
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
            "a module against the framework's settings"
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
        let builder = Cqrs::from_lookup("SHOP", |name| (name == "SHOP_LIMIT").then(|| "5".to_owned()))
            .event_store(InMemory)
            .projections(InMemory);
        let cqrs = ready(builder.module(reader).module(other).build()).unwrap();
        let described = cqrs.description();
        assert_eq!(
            (described.get("SHOP_LIMIT"), described.get("SHOP_SNAPSHOT_EVERY")),
            (Some(&"5".to_owned()), Some(&"10".to_owned())),
            "two modules sharing a variable, a module repeating the framework's value"
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
    fn one_observer_of_each_kind_per_framework() {
        let commands = || module(|module| module.observe_commands(Arc::new(Log::default())));
        let refused = refusal(memory_builder().module(commands()).module(commands()));
        assert!(matches!(refused, BuildError::ObserverTwice("test")), "{refused:?}");
        let projections = module(|module| {
            let observer = |_: ObservedProcessor<'_>| -> Arc<dyn ProjectionObserver> { Arc::new(Silent) };
            module.observe_projections(observer).observe_projections(observer)
        });
        assert_eq!(
            refusal(memory_builder().module(projections)).to_string(),
            "the module `test` registers a second observer of the commands or of the processors"
        );
    }

    struct Silent;

    impl ProjectionObserver for Silent {}

    #[tokio::test]
    async fn the_last_provide_wins_so_a_test_replaces_a_modules_dependency() {
        let (cqrs, mailbox) = framework().await;
        assert!(Arc::ptr_eq(&cqrs.dependency::<Mailbox>().unwrap(), &mailbox));
        assert_eq!(
            cqrs.dependency::<String>().unwrap_err().to_string(),
            "the dependency `alloc::string::String` is not provided"
        );
        cqrs.source("ui")
            .execute::<Show>(&"s-1".into(), book("A1", "fox"))
            .await
            .unwrap();
        cqrs.run_tasks().await;
        assert_eq!(mailbox.sent(), ["fox:A1"], "the letter went to the mailbox of the test");
    }

    #[test]
    fn a_builder_a_setup_and_a_module_print_what_they_name_but_never_a_url() {
        let secret = |_: &str| Some("postgres://shop:s3cret@db/shop".to_owned());
        let builder = Cqrs::from_lookup("SHOP", secret)
            .event_store(InMemory)
            .projections(InMemory)
            .module(shows)
            .module(wallets);
        assert_eq!(
            format!("{builder:?}"),
            format!(
                "Builder {{ prefix: \"SHOP\", defaults: {:?}, views: Some(Memory), modules: 2, dependencies: Dependencies {{ count: 0 }}, .. }}",
                Defaults::LIBRARY
            )
        );
        let setup = Setup::new("SHOP", secret, Settings::default());
        setup.describe("shop.url", "postgres://shop:s3cret@db/shop");
        assert_eq!(format!("{setup:?}"), r#"Setup { prefix: "SHOP", .. }"#);
        assert_eq!(
            format!("{:?}", wallets(&setup).unwrap()),
            r#"Module { name: "wallets", aggregates: ["wallet"], views: ["tickets"], tasks: 2, dependencies: Dependencies { count: 1 }, .. }"#
        );
        let entry = Module::new("mail").task(SendMail, TaskPolicy::default());
        assert_eq!(
            format!("{entry:?}"),
            r#"Module { name: "mail", aggregates: [], views: [], tasks: 1, dependencies: Dependencies { count: 0 }, .. }"#
        );
    }

    #[tokio::test]
    async fn a_processor_and_the_read_side_print_their_names() {
        let mut cqrs = memory_builder().module(shows).build().await.unwrap();
        let checkpoints = Arc::new(cqrsx::memory::checkpoint_store::MemoryCheckpointStore::default());
        let documents = Arc::new(MemoryViewRepository::<Hall, Show>::default());
        let journal = Arc::new(cqrs.journal.memory().unwrap().journal.clone());
        let processor = view_parts(&mut cqrs).processor::<Hall, Show, _, _, _>(journal, checkpoints, documents);
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
    async fn the_framework_builds_every_executor_and_processor_with_its_settings() {
        let processor = ProcessorConfig::builder()
            .poll_interval(Duration::from_millis(300))
            .page_size(50)
            .lease(Duration::from_secs(30))
            .delivery_limits(DeliveryLimits::default())
            .build()
            .unwrap();
        let limits = LockLimits::try_new(Duration::from_millis(500), 7).unwrap();
        let retry = AggregateConflictRetryPolicy::try_new(5, Duration::from_millis(1), Duration::from_millis(2), 0.0).unwrap();
        let settings = Settings::try_new(NonZeroUsize::new(3).unwrap(), limits, processor, TaskRuntimeConfig::default())
            .unwrap()
            .with_conflict_retry(retry)
            .with_node_name("node-a");
        let defaults = memory_builder().module(shows).build().await.unwrap();
        let set = Cqrs::with_settings(settings)
            .event_store(InMemory)
            .projections(InMemory)
            .module(shows)
            .build()
            .await
            .unwrap();
        let mut built = Vec::new();
        for mut cqrs in [defaults, set] {
            let Some(Executor::Memory(executor)) = cqrs.executor_of::<Show>() else {
                panic!("the executor of a framework in memory is the one in memory");
            };
            let (executor, store) = (format!("{executor:?}"), format!("{:?}", executor.store()));
            let checkpoints = Arc::new(cqrsx::memory::checkpoint_store::MemoryCheckpointStore::default());
            let documents = Arc::new(MemoryViewRepository::<Hall, Show>::default());
            let journal = Arc::new(cqrs.journal.memory().unwrap().journal.clone());
            let processor = view_parts(&mut cqrs).processor::<Hall, Show, _, _, _>(journal, checkpoints, documents);
            built.push((executor, store, format!("{processor:?}"), cqrs.owner.clone()));
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
            panic!("two frameworks were built");
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
        assert_eq!(named.assemble().unwrap().owner, "node-a");
        // The reactions plan with the budget of each task type's policy.
        let (cqrs, _) = framework().await;
        let booked = cqrs
            .source("ui")
            .execute::<Show>(&"s-1".into(), book("A1", "fox"))
            .await
            .unwrap();
        assert_eq!(booked.tasks[0].max_attempts().get(), 3, "the step's budget");
    }

    /// Writes the aggregate type, the command and the outcome of every command.
    #[derive(Default)]
    struct Log(std::sync::Mutex<Vec<String>>);

    impl AnyCommandObserver for Log {
        fn observe(&self, report: &AnyCommandReport<'_>) {
            let outcome = match &report.outcome {
                AnyOutcome::Rejected(rejection) => format!("{} {}", rejection.code(), rejection.parameters()),
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
            .event_store(InMemory)
            .projections(InMemory)
            .module(shows)
            .module(wallets)
            .module(observers)
            .build()
            .await
            .unwrap();
        assert_eq!(cqrs.settings().node_name(), Some("node-a"));
        let ui = cqrs.source("ui");
        ui.execute::<Show>(&"s-1".into(), book("A1", "fox")).await.unwrap();
        ui.execute::<Show>(&"s-1".into(), book("A1", "panda")).await.unwrap_err();
        cqrs.run_tasks().await;
        let again = ui.execute::<Wallet>(&"fox".into(), Record { seat: "A1".into() }).await;
        assert_eq!(again.unwrap_err(), cqrsx::command::CommandError::Rejected(AlreadyRecorded));
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
    async fn an_assembly_hands_out_what_the_modules_registered() {
        let log = Arc::new(Log::default());
        let feed = log.clone();
        let observers = move |_: &Setup| -> Result<Module, SetupError> {
            Ok(Module::new("feed")
                .observe_commands(feed)
                .observe_projections(|_| Arc::new(Silent)))
        };
        let builder = Cqrs::with_settings(Settings::default())
            .module(shows)
            .module(wallets)
            .module(observers);
        let mut assembly = builder.provide(Arc::new(String::from("extra"))).assemble().unwrap();
        assert_eq!(assembly.aggregate_types(), ["show", "wallet"]);
        assert_eq!(assembly.view_names(), ["halls", "tickets"]);
        assert_eq!(assembly.settings().snapshot_every().get(), 10);
        assert_eq!(assembly.take_rules::<Show>(), Some(()));
        assert_eq!(assembly.take_rules::<Show>(), None, "once");
        assert_eq!(
            assembly.take_rules::<Stranger>(),
            None,
            "not an aggregate type of the modules"
        );
        assert_eq!(assembly.policies().max_attempts(&TaskFormat::of::<MailTicket>()).get(), 6);
        assert_eq!(*assembly.dependency::<String>().unwrap(), "extra");
        assert!(assembly.dependency::<u8>().is_err());
        assert!(assembly.projection_observer("halls").is_some());
        let observer = assembly.command_observer::<Show>().unwrap();
        let metadata = std::collections::HashMap::new();
        observer.observe(&CommandReport {
            aggregate_id: &"s-1".to_owned(),
            command: &book("A1", "fox"),
            metadata: &metadata,
            outcome: cqrsx::command::observer::Outcome::Accepted { tasks: &[] },
        });
        observer.observe(&CommandReport {
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
        let dispatcher = assembly.take_task_handlers().build().unwrap();
        assert_eq!(
            dispatcher.formats(),
            [TaskFormat::of::<RecordTicket>(), TaskFormat::of::<MailTicket>()]
        );
        let mut bare = Cqrs::with_settings(Settings::default()).module(shows).assemble().unwrap();
        assert!(bare.command_observer::<Show>().is_none() && bare.projection_observer("halls").is_none());
        assert!(bare.take_task_handlers().build().is_err(), "no task type");
    }

    /// A builder with the journal in memory and nothing else chosen.
    fn memory_event_store() -> Builder {
        Cqrs::with_settings(Settings::default()).event_store(InMemory)
    }

    #[test]
    fn a_framework_has_no_default_journal() {
        let none = refusal(Cqrs::with_settings(Settings::default()).module(shows));
        assert!(matches!(none, BuildError::NoJournal), "{none:?}");
        assert_eq!(
            none.to_string(),
            "the framework has no journal: give the builder `event_store(..)`"
        );
        // Views in memory are no journal, and a framework without modules still needs one.
        let views_only = refusal(Cqrs::with_settings(Settings::default()).projections(InMemory).module(shows));
        assert!(matches!(views_only, BuildError::NoJournal), "{views_only:?}");
        let empty = refusal(Cqrs::with_settings(Settings::default()));
        assert!(matches!(empty, BuildError::NoJournal), "{empty:?}");
        // Its own refusals come first: the modules are checked before the stores.
        let twice = refusal(Cqrs::with_settings(Settings::default()).module(shows).module(shows));
        assert!(matches!(twice, BuildError::AggregateTwice("show")), "{twice:?}");
    }

    #[test]
    fn views_have_no_default_store() {
        let none = refusal(memory_event_store().module(shows));
        assert!(matches!(none, BuildError::ViewsWithoutStore), "{none:?}");
        assert_eq!(
            none.to_string(),
            "the views have no store: give the builder `projections(..)`"
        );
        let chosen = ready(memory_event_store().projections(InMemory).module(shows).build());
        assert!(chosen.is_ok());
        // A framework without views needs no store for them.
        let commands_only = |_: &Setup| Ok(Module::new("commands").aggregate::<Show>(()));
        let built = ready(memory_event_store().module(commands_only).build());
        assert!(built.is_ok(), "{:?}", built.err());
        // The task handlers need no store at all.
        assert!(Cqrs::with_settings(Settings::default()).module(shows).task_handlers().is_ok());
    }

    #[test]
    fn views_outside_memory_need_the_journal_in_postgres() {
        #[cfg(feature = "projections-postgres")]
        assert_eq!(
            refusal(memory_event_store().projections(crate::postgres::Postgres).module(shows)).to_string(),
            "the views in PostgreSQL need the journal in PostgreSQL"
        );
        #[cfg(feature = "projections-mongodb")]
        assert!(matches!(
            refusal(memory_event_store().projections(crate::mongodb::MongoDb).module(shows)),
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
        let handlers = Cqrs::with_settings(Settings::default())
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
        let refused = Cqrs::with_settings(Settings::default())
            .module(shows)
            .module(shows)
            .task_handlers()
            .err()
            .unwrap();
        assert!(matches!(refused, BuildError::AggregateTwice("show")));
    }
}
