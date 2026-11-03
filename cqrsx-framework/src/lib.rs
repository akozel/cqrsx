#![doc = include_str!("../README.md")]
// The framework is the assembly over the stores: without a store feature nothing can be chosen for `event_store(..)`.
#[cfg(not(any(feature = "event-store-postgres", feature = "test-utils")))]
compile_error!("cqrsx-framework needs a store: the feature `event-store-postgres`, or `test-utils` for a framework in memory");

#[cfg(feature = "axum")]
pub mod axum;
pub mod build;
mod diagnostics;
// The stores in memory and their marker `InMemory`: the feature `test-utils` only.
#[cfg(feature = "test-utils")]
pub mod memory;
pub mod module;
#[cfg(feature = "projections-mongodb")]
pub mod mongodb;
#[cfg(feature = "operation-utils")]
mod operation_utils;
#[cfg(feature = "event-store-postgres")]
pub mod postgres;
pub mod process;
pub mod settings;
pub mod setup;
pub mod source;
mod stores;
#[cfg(test)]
mod test_support;
#[cfg(feature = "test-utils")]
mod test_utils;
pub mod views;

use crate::{module::ProjectionObserverFactory, settings::Settings, views::Registry};
use cqrsx::{
    command::observer::AnyCommandObserver,
    journal::wakeup::CommitWakeup,
    projection::runtime::ProjectionRuntime,
    task::policy::Policies,
    worker::{
        adapter::boxed::BoxedTaskAdapter,
        context::Executors,
        dependencies::{Dependencies, MissingDependency},
        dispatcher::TaskDispatcher,
    },
};
use std::{
    any::{Any, TypeId, type_name},
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{Arc, Mutex},
};
use tokio_util::sync::{CancellationToken, DropGuard};

/// A framework: its executors, views and task handlers over its stores, assembled from its modules. Dropping it stops
/// its projection processors.
pub struct Cqrs {
    settings: Settings,
    /// This node: the owner of the processors' checkpoints and of the claimed tasks.
    owner: String,
    policies: Policies,
    dependencies: Dependencies,
    command_observer: Option<Arc<dyn AnyCommandObserver>>,
    projection_observer: Option<ProjectionObserverFactory>,
    /// The executor of every aggregate type, by its type: an `Arc<stores::Executor<A>>`.
    command_executors: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    executors: Executors,
    wakeups: HashMap<TypeId, CommitWakeup>,
    dispatcher: Option<Arc<TaskDispatcher<BoxedTaskAdapter>>>,
    views: Registry,
    /// The settings and what the modules read and described when they were set up, by key: one value per key, checked
    /// when the framework was built.
    described: BTreeMap<String, String>,
    journal: stores::Journal,
    processors: Mutex<ProjectionRuntime>,
    /// The endpoints of every module in the order the modules were registered.
    endpoints: Vec<Box<dyn Any + Send + Sync>>,
    shutdown: CancellationToken,
    /// Cancels `shutdown` when the framework is dropped.
    _stop: DropGuard,
}

// By hand: the settings and what the modules described hold the database URLs with their passwords; the stores, the
// executors, the observers and the processors are not printed either.
impl fmt::Debug for Cqrs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cqrs")
            .field("owner", &self.owner)
            .field("command_executors", &self.command_executors.len())
            .field("views", &self.views().keys())
            .field("policies", &self.policies)
            .field("dependencies", &self.dependencies)
            .field("dispatcher", &self.dispatcher)
            .finish_non_exhaustive()
    }
}

impl Cqrs {
    /// A framework with `settings` (e.g. [`Settings::default`], or read with [`Settings::from_lookup`] and changed by a
    /// test); its modules read no variable.
    pub fn with_settings(settings: Settings) -> build::Builder {
        build::Builder::new(Some(settings), String::new(), Arc::new(|_| None))
    }

    /// A framework whose settings ([`Settings::from_lookup`]) and modules' variables (`Setup::env`) come from `lookup`
    /// under `prefix`, read when the framework is built; `Builder::defaults` gives the application's values.
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

    /// The endpoints the modules registered ([`Module::endpoints`](module::Module::endpoints)) in the type `T` of the
    /// server that serves them, in the order the modules were registered.
    pub fn endpoints<T: Any>(&self) -> impl Iterator<Item = &T> {
        self.endpoints.iter().filter_map(|endpoints| endpoints.downcast_ref::<T>())
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// The policies of the task types: the reactions plan with them, the task runtime pauses by them.
    pub fn policies(&self) -> &Policies {
        &self.policies
    }

    /// The settings of the framework ([`Settings::description`]) and what its modules read and described, by key. A key has
    /// one value: the build refuses one described twice with two ([`BuildError::DescribedTwice`](build::BuildError)).
    pub fn description(&self) -> BTreeMap<String, String> {
        self.described.clone()
    }

    /// A token cancelled once the framework stops (it is served no more, or dropped); cancelling it stops nothing. A
    /// server and its streams end with it.
    pub fn stopping(&self) -> CancellationToken {
        self.shutdown.child_token()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{memory::InMemory, test_support::*};

    #[tokio::test]
    async fn a_framework_describes_its_settings_and_what_its_modules_read() {
        let read = |setup: &setup::Setup| -> Result<module::Module, setup::SetupError> {
            setup.env("LIMIT", 4)?;
            setup.describe("shop.limit_twice", "8");
            Ok(module::Module::new("reader"))
        };
        let cqrs = Cqrs::from_lookup("SHOP", |name| (name == "SHOP_LIMIT").then(|| "5".to_owned()))
            .event_store(InMemory)
            .module(read)
            .build()
            .await
            .unwrap();
        let described = cqrs.description();
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
        assert!(stopping.is_cancelled(), "a dropped framework has stopped");
    }

    #[tokio::test]
    async fn a_framework_and_its_entry_points_print_what_they_hold_but_never_a_url() {
        let (cqrs, _) = framework().await;
        let printed = format!("{cqrs:?}");
        let head = format!(
            "Cqrs {{ owner: {:?}, command_executors: 2, views: {:?}, ",
            cqrs.owner,
            cqrs.views().keys()
        );
        assert!(printed.starts_with(&head), "{printed}");
        assert!(printed.contains("dispatcher: Some(TaskDispatcher {"), "{printed}");
        assert!(!printed.contains("://"), "no URL of a store: {printed}");
        assert_eq!(format!("{:?}", cqrs.source("ui")), r#"Source { source: "ui", .. }"#);
        assert_eq!(format!("{:?}", cqrs.view::<Hall>()), "ViewQuery { registered: true, .. }");
        assert_eq!(format!("{:?}", cqrs.view::<String>()), "ViewQuery { registered: false, .. }");
        assert_eq!(
            format!("{:?}", cqrs.views()),
            format!("Views {{ keys: {:?}, .. }}", cqrs.views().keys())
        );
    }

    #[tokio::test]
    async fn the_history_of_an_aggregate_is_its_journal_without_snapshots() {
        let (cqrs, _) = framework().await;
        for seat in ["A1", "A2"] {
            cqrs.source("ui")
                .execute::<Show>(&"s-1".into(), book(seat, "fox"))
                .await
                .unwrap();
        }
        #[cfg(feature = "operation-utils")]
        {
            let history = cqrs.history::<Show>(&"s-1".into()).await.unwrap();
            assert_eq!(history.iter().map(|event| event.sequence).collect::<Vec<_>>(), [1, 2]);
            assert!(cqrs.history::<Show>(&"s-9".into()).await.unwrap().is_empty());
        }
        assert_eq!(
            cqrs.policies()
                .max_attempts(&cqrsx::task::format::TaskFormat::of::<MailTicket>())
                .get(),
            6
        );
        #[cfg(feature = "operation-utils")]
        assert!(cqrs.dispatcher().is_some());
    }

    #[test]
    fn from_env_reads_the_process_environment_in_an_isolated_process() {
        use crate::test_support;
        if let Some(scenario) = test_support::scenario() {
            assert_eq!(scenario, "variables");
            let read = |setup: &setup::Setup| setup.env("LIMIT", 4).map(|_| module::Module::new("reader"));
            let cqrs = ready(
                Cqrs::from_env("CQRSX_FRAMEWORK_TEST")
                    .event_store(InMemory)
                    .module(read)
                    .build(),
            )
            .unwrap();
            let described = cqrs.description();
            let value = |key: &str| described.get(key).cloned();
            assert_eq!(
                (cqrs.settings().snapshot_every().get(), value("CQRSX_FRAMEWORK_TEST_LIMIT")),
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
                    .env("CQRSX_FRAMEWORK_TEST_SNAPSHOT_EVERY", "7")
                    .env("CQRSX_FRAMEWORK_TEST_LIMIT", "3");
            },
        );
    }
}
