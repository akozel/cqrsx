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
//!
#![doc = include_str!("../README.md")]

pub mod build;
mod diagnostics;
mod executor;
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

use crate::{
    module::ProjectionObservers,
    settings::Settings,
    views::{CheckpointOps, Registry},
};
use cqrsx::{
    command::observer::AnyCommandObserver,
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::{EventStore, PersistenceError, persisted::PersistedEventStore, wakeup::CommitWakeup},
    projection::runtime::ProjectionRuntime,
    task::policy::Policies,
    worker::{
        adapter::boxed::BoxedTaskAdapter,
        context::Executors,
        dependencies::{Dependencies, MissingDependency},
        dispatcher::TaskDispatcher,
    },
};
#[cfg(feature = "postgres")]
use cqrsx_postgres::event_repository::PostgresEventRepository;
use std::{
    any::{Any, TypeId, type_name},
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{Arc, Mutex},
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
    /// The settings and what the modules read and described when they were set up, by key: one value per key, checked
    /// when the engine was built.
    described: BTreeMap<String, String>,
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

// By hand: the settings and what the modules described hold the database URLs with their passwords; the stores, the
// executors, the observers and the processors are not printed either.
impl fmt::Debug for Cqrs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cqrs")
            .field("owner", &self.owner)
            .field("aggregates", &self.commands.len())
            .field("views", &self.views().keys())
            .field("policies", &self.policies)
            .field("dependencies", &self.dependencies)
            .field("dispatcher", &self.dispatcher)
            .finish_non_exhaustive()
    }
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

    /// The settings of the engine ([`Settings::describe`]) and what its modules read and described, by key. A key has
    /// one value: the build refuses one described twice with two ([`BuildError::DescribedTwice`](build::BuildError)).
    pub fn describe(&self) -> BTreeMap<String, String> {
        self.described.clone()
    }

    /// A token cancelled once the engine stops (it is served no more, or dropped); cancelling it stops nothing. A
    /// stream of the HTTP API ends with it.
    pub fn stopping(&self) -> CancellationToken {
        self.shutdown.child_token()
    }

    /// The committed events of the aggregate `id` of type `A`, from its journal alone, without a snapshot (on
    /// PostgreSQL through the pool of whole-history reads, without a statement timeout).
    pub async fn history<A: Aggregate>(&self, id: &A::Id) -> Result<Vec<EventEnvelope<A>>, PersistenceError> {
        let id = id.to_string();
        match &self.journal {
            Journal::Memory => {
                PersistedEventStore::<_, A>::new(self.memory.journal.clone())
                    .load_events(&id)
                    .await
            }
            #[cfg(feature = "postgres")]
            Journal::Postgres(pools) => {
                PersistedEventStore::<_, A>::new(PostgresEventRepository::new(pools.history.clone()))
                    .load_events(&id)
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

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
        let described = cqrs.describe();
        let value = |key: &str| described.get(key).map(String::as_str);
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
    async fn an_engine_and_its_entry_points_print_what_they_hold_but_never_a_url() {
        let (cqrs, _) = engine().await;
        let printed = format!("{cqrs:?}");
        let head = format!(
            "Cqrs {{ owner: {:?}, aggregates: 2, views: {:?}, ",
            cqrs.owner,
            cqrs.views().keys()
        );
        assert!(printed.starts_with(&head), "{printed}");
        assert!(printed.contains("dispatcher: Some(TaskDispatcher {"), "{printed}");
        assert!(!printed.contains("://"), "no URL of a store: {printed}");
        assert_eq!(format!("{:?}", cqrs.from("ui")), r#"Source { source: "ui", .. }"#);
        assert_eq!(format!("{:?}", cqrs.view::<Hall>()), "ViewQuery { registered: true, .. }");
        assert_eq!(format!("{:?}", cqrs.view::<String>()), "ViewQuery { registered: false, .. }");
        assert_eq!(
            format!("{:?}", cqrs.views()),
            format!("Views {{ keys: {:?}, .. }}", cqrs.views().keys())
        );
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
                .max_attempts(&cqrsx::task::format::TaskFormat::of::<MailTicket>())
                .get(),
            6
        );
        assert!(cqrs.dispatcher().is_some());
    }

    #[test]
    fn from_env_reads_the_process_environment_in_an_isolated_process() {
        use crate::test_support;
        if let Some(scenario) = test_support::scenario() {
            assert_eq!(scenario, "variables");
            let read = |setup: &setup::Setup| setup.env("LIMIT", 4).map(|_| module::Module::new("reader"));
            let cqrs = ready(Cqrs::from_env("CQRSX_ENGINE_TEST").module(read).build()).unwrap();
            let described = cqrs.describe();
            let value = |key: &str| described.get(key).cloned();
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
