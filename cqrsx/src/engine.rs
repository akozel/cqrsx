//! The engine: an application of modules ([`module::Module`]) assembled from the components of cqrsx, which keep
//! their logic: an executor per aggregate type with its reactions and policies, a projection processor per view woken
//! by the commits of its type, one dispatcher of the task handlers, the observers of every command and processor.
//!
//! A [`build::Builder`] starts from the library's settings ([`Cqrs::in_memory`]), settings of one's own
//! ([`Cqrs::with_settings`]) or the variables of the environment under the application's prefix ([`Cqrs::from_env`],
//! [`Cqrs::from_lookup`]); `build` sets up the modules ([`setup::Setup`]), checks what they register and assembles
//! the engine over its stores: in memory unless the journal is in PostgreSQL (`event_store`) and the views in
//! PostgreSQL or MongoDB (`projections`). [`Cqrs::from`] is an entry point of the commands ([`outcome`]),
//! [`Cqrs::view`] and [`Cqrs::views`] query and operate the views ([`views`]); [`Cqrs::run_tasks`] runs the due tasks
//! and [`Cqrs::catch_up`] lets the views catch up with the journal. With the feature `http` the modules route HTTP
//! requests ([`http`]) and `serve` runs the whole application until a signal ([`serve`]).
pub mod build;
#[cfg(feature = "http")]
pub mod http;
mod memory;
pub mod module;
#[cfg(feature = "mongodb")]
pub mod mongodb;
pub mod outcome;
#[cfg(feature = "postgres")]
pub mod postgres;
#[cfg(feature = "http")]
pub mod serve;
pub mod settings;
pub mod setup;
#[cfg(test)]
mod test_support;
pub mod views;

#[cfg(feature = "postgres")]
use crate::postgres::{
    command::{PostgresCommandConfig, PostgresCommandExecutor},
    event_repository::PostgresEventRepository,
    projection_checkpoints::PostgresCheckpointStore,
    view_repository::PostgresViewRepository,
};
use crate::{
    cqrs::{
        AggregateError, CommitReport, CqrsFramework,
        aggregate::Aggregate,
        command::{
            CommandExecutor, CommandGatewayError, CommandInfo,
            executor::CommandExecutorCore,
            observer::{AnyCommandObserver, AnyCommandReport, CommandObserver, CommandReport},
            wakeup::CommitWakeup,
        },
        event::EventEnvelope,
        projection::{
            RebuildableProjection, View, ViewProjection, checkpoint::CheckpointStore, processor::ProjectionProcessor,
            repository::ViewRepository, runtime::ProjectionRuntime,
        },
        reactions::{Reactions, ReactionsPlanner},
        rejection::Rejection,
        store::{EventStore, persisted::PersistedEventStore, repository::PersistedEventRepository},
        testing::mem_event_repository::MemEventRepository,
    },
    engine::{
        build::BuildError,
        module::ProjectionObservers,
        settings::Settings,
        views::{CheckpointOps, Counts, Registry},
    },
    task::{
        adapter::boxed::BoxedTaskAdapter,
        context::Executors,
        dependencies::{Dependencies, MissingDependency},
        dispatcher::TaskDispatcher,
        policy::Policies,
    },
};
use std::{
    any::{Any, TypeId, type_name},
    collections::HashMap,
    sync::{Arc, Mutex, PoisonError},
};
use tokio_util::sync::{CancellationToken, DropGuard};

/// An engine: its executors, views and task handlers over its stores, assembled from its modules. Dropping it stops
/// its projection processors.
pub struct Cqrs {
    settings: Settings,
    /// This node: the owner of the processors' checkpoints and of the claimed tasks.
    owner: String,
    policies: Policies,
    dependencies: Dependencies,
    command_observer: Option<Arc<dyn AnyCommandObserver>>,
    projection_observers: Option<ProjectionObservers>,
    /// The executor of every aggregate type, by its type: an `Arc<Executor<A>>`.
    commands: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    executors: Executors,
    wakeups: HashMap<TypeId, CommitWakeup>,
    dispatcher: Option<Arc<TaskDispatcher<BoxedTaskAdapter>>>,
    views: Registry,
    /// What the modules read and described when they were set up.
    described: Vec<(String, String)>,
    memory: memory::MemoryBackend,
    journal: Journal,
    #[cfg(feature = "postgres")]
    view_store: ViewStore,
    /// The checkpoints of every view, its store erased: what the operations over the views read and request.
    checkpoints: Arc<dyn CheckpointOps>,
    processors: Mutex<ProjectionRuntime>,
    #[cfg(feature = "http")]
    routes: axum::Router<Arc<Cqrs>>,
    shutdown: CancellationToken,
    /// Cancels `shutdown` when the engine is dropped.
    _stop: DropGuard,
}

/// Where the engine keeps the journal, the snapshots, the outbox and the task queue.
pub(crate) enum Journal {
    Memory,
    #[cfg(feature = "postgres")]
    Postgres(postgres::PostgresPools),
}

/// Where an engine over PostgreSQL keeps the views and their checkpoints.
#[cfg(feature = "postgres")]
pub(crate) enum ViewStore {
    Memory,
    Postgres,
    #[cfg(feature = "mongodb")]
    MongoDb(::mongodb::Database),
}

impl Cqrs {
    /// An engine with the library's settings and no variables: every module reads its defaults. Its stores are in
    /// memory unless the builder chooses others.
    pub fn in_memory() -> build::Builder {
        Self::with_settings(Settings::default())
    }

    /// An engine with `settings` (e.g. read with [`Settings::from_lookup`] and changed by a test); its modules read no
    /// variable.
    pub fn with_settings(settings: Settings) -> build::Builder {
        build::Builder::new(Some(settings), String::new(), Arc::new(|_| None))
    }

    /// An engine whose settings ([`Settings::from_lookup`]) and modules' variables (`Setup::env`) come from `lookup`
    /// under `prefix`, read when the engine is built; `Builder::defaults` gives the application's values.
    pub fn from_lookup(
        prefix: impl Into<String>,
        lookup: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
    ) -> build::Builder {
        build::Builder::new(None, prefix.into(), Arc::new(lookup))
    }

    /// [`from_lookup`](Self::from_lookup) over the environment of the process.
    pub fn from_env(prefix: impl Into<String>) -> build::Builder {
        Self::from_lookup(prefix, |name| std::env::var(name).ok())
    }

    /// The dependency of type `T` a module or the builder provided.
    pub fn dependency<T: ?Sized + 'static>(&self) -> Result<Arc<T>, MissingDependency> {
        self.dependencies.shared::<T>().ok_or(MissingDependency(type_name::<T>()))
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// The policies of the task types: the reactions plan with them, the task runtime pauses by them.
    pub fn policies(&self) -> &Policies {
        &self.policies
    }

    /// The task handlers of every module in one dispatcher, with their dependencies and the executors of every
    /// aggregate type; `None` without a task type.
    pub fn dispatcher(&self) -> Option<&TaskDispatcher<BoxedTaskAdapter>> {
        self.dispatcher.as_deref()
    }

    /// The settings of the engine ([`Settings::describe`]) and what its modules read and described, as `(key, value)`
    /// sorted by key.
    pub fn describe(&self) -> Vec<(String, String)> {
        let mut lines = self.settings.describe();
        lines.extend(self.described.iter().cloned());
        lines.sort();
        lines
    }

    /// A token cancelled once the engine stops (it is served no more, or dropped); cancelling it stops nothing. A
    /// stream of the HTTP API ends with it.
    pub fn stopping(&self) -> CancellationToken {
        self.shutdown.child_token()
    }

    /// The committed events of the aggregate `id` of type `A`, from its journal alone, without a snapshot (on
    /// PostgreSQL through the pool of whole-history reads, without a statement timeout).
    pub async fn history<A: Aggregate>(&self, id: &A::Id) -> Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>> {
        let id = id.to_string();
        match &self.journal {
            Journal::Memory => {
                PersistedEventStore::<_, A>::new_event_store(self.memory.journal.clone())
                    .load_events(&id)
                    .await
            }
            #[cfg(feature = "postgres")]
            Journal::Postgres(pools) => {
                PersistedEventStore::<_, A>::new_event_store(PostgresEventRepository::new(pools.history.clone()))
                    .load_events(&id)
                    .await
            }
        }
    }

    /// Installs the executor of `A` over the engine's journal, with the rules its module registered: one per aggregate
    /// type, shared by the entry points (`from(..).execute`) and the task handlers (`ctx.execute`).
    fn install_aggregate<A>(&mut self, rules: Box<dyn Any + Send>) -> Result<(), BuildError>
    where
        A: Reactions + Clone + 'static,
        A::Command: CommandInfo + Clone,
        A::Error: Rejection + Send + 'static,
    {
        // The entry of `A` holds the rules of `A`.
        let Ok(rules) = rules.downcast::<A::Rules>() else {
            return Ok(());
        };
        let executor = match &self.journal {
            Journal::Memory => Executor::Memory(self.executor::<A, _>(self.store::<A>(), *rules)),
            #[cfg(feature = "postgres")]
            Journal::Postgres(pools) => {
                let pool = pools.main.clone();
                Executor::Postgres(self.postgres_executor::<A>(pool, *rules))
            }
        };
        let executor = Arc::new(executor);
        let executors = self.executors.clone().with::<A, _>(executor.clone());
        self.executors = executors.map_err(|twice| BuildError::AggregateTwice(twice.0))?;
        self.commands.insert(TypeId::of::<A>(), executor);
        Ok(())
    }

    /// The store of the commands of `A` in memory: the journal, a snapshot every `snapshot_every` events.
    fn store<A: Aggregate>(&self) -> PersistedEventStore<MemEventRepository, A> {
        PersistedEventStore::new_snapshot_store(self.memory.journal.clone(), self.settings.snapshot_every())
    }

    /// The executor of `A` over `store` as every backend builds it: the settings' conflict retry and lock limits, the
    /// reactions with the policies, the wake-up of the processors of `A`, the observer of every command.
    fn executor<A, S>(&mut self, store: S, rules: A::Rules) -> CommandExecutorCore<A, S>
    where
        A: Reactions + 'static,
        A::Command: CommandInfo + Clone,
        A::Error: Rejection,
        S: EventStore<A>,
    {
        let framework = CqrsFramework::new(store, rules);
        let settings = &self.settings;
        let executor = CommandExecutorCore::new(framework, settings.conflict_retry(), settings.lock_limits())
            .with_task_planner(ReactionsPlanner::<A>::new(self.policies.clone()))
            .with_commit_wakeup(self.wakeup::<A>());
        match self.commands_observer::<A>() {
            Some(observer) => executor.with_observer(observer),
            None => executor,
        }
    }

    /// The executor of `A` over PostgreSQL, built as the core of [`executor`](Self::executor): the snapshot store on
    /// the outbox repository of the main pool.
    #[cfg(feature = "postgres")]
    fn postgres_executor<A>(&mut self, pool: sqlx::PgPool, rules: A::Rules) -> PostgresCommandExecutor<A>
    where
        A: Reactions + 'static,
        A::Command: CommandInfo + Clone,
        A::Error: Rejection,
    {
        let settings = &self.settings;
        let config = PostgresCommandConfig::new(settings.snapshot_every(), settings.conflict_retry(), settings.lock_limits());
        let executor = PostgresCommandExecutor::new(pool, config, rules)
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
        A::Command: CommandInfo,
        A::Error: Rejection,
    {
        let observer = self.command_observer.clone()?;
        Some(Arc::new(Commands(observer)))
    }

    /// Installs the processor of `V` over the engine's journal and checkpoints, and the repository of its documents,
    /// which the queries read.
    fn install_view<V: View<A> + PartialEq + 'static, A: Aggregate + 'static>(&mut self) {
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
                        let documents = Arc::new(crate::mongodb::view_repository::MongoViewRepository::<V, A>::new(database));
                        let checkpoints = Arc::new(crate::mongodb::projection_checkpoints::MongoCheckpointStore::new(database));
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
        let processors = self.processors.get_mut().unwrap_or_else(PoisonError::into_inner);
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
            Some(observers) => processor.with_observer(observers(V::NAME, &self.owner)),
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

/// The executor of one aggregate type over the engine's journal: the variants are the stores, so a command from an
/// entry point waits for the store's own future, without an allocation.
pub(crate) enum Executor<A: Aggregate> {
    Memory(CommandExecutorCore<A, PersistedEventStore<MemEventRepository, A>>),
    #[cfg(feature = "postgres")]
    Postgres(PostgresCommandExecutor<A>),
}

impl<A> CommandExecutor<A> for Executor<A>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone + 'static,
    A::Error: Send + 'static,
{
    async fn execute_with_metadata(
        &self,
        id: &A::Id,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, CommandGatewayError<A::Error>> {
        match self {
            Self::Memory(core) => core.execute_with_metadata(id, command, metadata).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(executor) => executor.execute_with_metadata(id, command, metadata).await,
        }
    }
}

/// The observer of every command as the observer of the commands of one aggregate type.
struct Commands(Arc<dyn AnyCommandObserver>);

impl<A: Aggregate> CommandObserver<A> for Commands
where
    A::Command: CommandInfo,
    A::Error: Rejection,
{
    fn command(&self, report: &CommandReport<'_, A>) {
        self.0.command(&AnyCommandReport::of(report));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::test_support::*;

    #[tokio::test]
    async fn an_engine_describes_its_settings_and_what_its_modules_read() {
        let read = |setup: &setup::Setup| -> Result<module::Module, setup::SetupError> {
            setup.env("LIMIT", 4)?;
            setup.describe("shop.limit_twice", "8");
            Ok(module::Module::new("reader"))
        };
        let cqrs = Cqrs::from_lookup("SHOP", |name| (name == "SHOP_LIMIT").then(|| "5".to_owned()))
            .module(read)
            .build()
            .await
            .unwrap();
        let lines = cqrs.describe();
        assert!(lines.is_sorted());
        let value = |key: &str| lines.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());
        assert_eq!(
            (value("SHOP_LIMIT"), value("shop.limit_twice"), value("SHOP_SNAPSHOT_EVERY")),
            (Some("5"), Some("8"), Some("10"))
        );
        assert_eq!(cqrs.settings().prefix(), "SHOP");
        let stopping = cqrs.stopping();
        stopping.cancel();
        assert!(!cqrs.shutdown.is_cancelled(), "cancelling the child stops nothing");
        let stopping = cqrs.stopping();
        drop(cqrs);
        assert!(stopping.is_cancelled(), "a dropped engine has stopped");
    }

    #[tokio::test]
    async fn the_history_of_an_aggregate_is_its_journal_without_snapshots() {
        let (cqrs, _) = engine().await;
        for seat in ["A1", "A2"] {
            cqrs.from("ui")
                .execute::<Show>(&"s-1".into(), book(seat, "fox"))
                .await
                .unwrap();
        }
        let history = cqrs.history::<Show>(&"s-1".into()).await.unwrap();
        assert_eq!(history.iter().map(|event| event.sequence).collect::<Vec<_>>(), [1, 2]);
        assert!(cqrs.history::<Show>(&"s-9".into()).await.unwrap().is_empty());
        assert_eq!(
            cqrs.policies()
                .max_attempts(&crate::task::format::TaskFormat::of::<MailTicket>().unwrap()),
            6
        );
        assert!(cqrs.dispatcher().is_some());
    }

    #[test]
    fn from_env_reads_the_process_environment_in_an_isolated_process() {
        use crate::env::test_support;
        if let Some(scenario) = test_support::scenario() {
            assert_eq!(scenario, "variables");
            let read = |setup: &setup::Setup| setup.env("LIMIT", 4).map(|_| module::Module::new("reader"));
            let cqrs = ready(Cqrs::from_env("CQRSX_ENGINE_TEST").module(read).build()).unwrap();
            let lines = cqrs.describe();
            let value = |key: &str| lines.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone());
            assert_eq!(
                (cqrs.settings().snapshot_every().get(), value("CQRSX_ENGINE_TEST_LIMIT")),
                (7, Some("3".to_owned()))
            );
            return;
        }
        test_support::isolated(
            concat!(
                module_path!(),
                "::from_env_reads_the_process_environment_in_an_isolated_process"
            ),
            "variables",
            |child| {
                child
                    .env("CQRSX_ENGINE_TEST_SNAPSHOT_EVERY", "7")
                    .env("CQRSX_ENGINE_TEST_LIMIT", "3");
            },
        );
    }
}
