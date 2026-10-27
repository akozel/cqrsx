//! How an engine is built: its modules are set up, their registrations merged and checked ([`Registrations`]), and the
//! components assembled from them over the engine's stores.
use crate::{
    cqrs::{
        aggregate::Aggregate,
        command::{CommandInfo, observer::CommandObserver},
        projection::{processor::observer::ProjectionObserver, runtime::ProjectionRuntime},
        rejection::Rejection,
    },
    engine::{
        Commands, Cqrs,
        memory::MemoryBackend,
        module::{AggregateEntry, Module, ViewEntry},
        setup::{Lookup, Settings, Setup, SetupError},
    },
    task::{
        adapter::boxed::BoxedTaskAdapter,
        context::Executors,
        dependencies::{Dependencies, MissingDependency},
        dispatcher::{TaskDispatcher, TaskDispatcherError},
        handlers::TaskHandlers,
        policy::{Policies, PolicyError, TaskPolicy},
    },
};
use std::{
    any::TypeId,
    collections::HashSet,
    sync::{Arc, Mutex},
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Why an engine could not be built: the first refusal of its modules.
#[derive(Debug, Error)]
pub enum BuildError {
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
}

type ModuleRoot = Box<dyn FnOnce(&Setup) -> Result<Module, SetupError>>;

/// What an engine is built from: the roots of its modules, its settings, the variables the modules read and the
/// dependencies the builder provides instead of theirs (a test replaces a module's by type).
pub struct Builder {
    settings: Settings,
    prefix: String,
    lookup: Lookup,
    modules: Vec<ModuleRoot>,
    dependencies: Dependencies,
}

impl Default for Builder {
    /// The library's settings and no variable: every module reads its defaults.
    fn default() -> Self {
        Self {
            settings: Settings::default(),
            prefix: String::new(),
            lookup: Arc::new(|_| None),
            modules: Vec::new(),
            dependencies: Dependencies::default(),
        }
    }
}

impl Builder {
    pub fn settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }

    /// The modules read their variables from `lookup` under `prefix` (`Setup::env`).
    pub fn env(mut self, prefix: impl Into<String>, lookup: impl Fn(&str) -> Option<String> + Send + Sync + 'static) -> Self {
        (self.prefix, self.lookup) = (prefix.into(), Arc::new(lookup));
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

    /// The engine in memory, or the first refusal of its modules.
    pub fn build(self) -> Result<Cqrs, BuildError> {
        let Registrations {
            mut cqrs,
            aggregates,
            views,
            adapters,
        } = self.registrations()?;
        for entry in aggregates {
            // The rules are gone only when an assembly of one's own took them.
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

    /// What the modules registered, merged and checked as `build` checks them (an aggregate type and a view name
    /// once, a policy declared once and bound to its task types, one observer of each kind), for an assembly over
    /// stores of one's own: a backend the engine does not have yet.
    pub fn registrations(self) -> Result<Registrations, BuildError> {
        let setup = Setup::over(self.prefix, self.lookup, self.settings);
        let modules = self
            .modules
            .into_iter()
            .map(|root| root(&setup))
            .collect::<Result<Vec<_>, _>>()?;
        let shutdown = CancellationToken::new();
        let mut cqrs = Cqrs {
            owner: setup.engine().node_name.clone().unwrap_or_else(|| Uuid::new_v4().to_string()),
            settings: setup.engine().clone(),
            policies: Policies::default(),
            dependencies: Dependencies::default(),
            command_observer: None,
            projection_observers: None,
            executors: Executors::default(),
            wakeups: Default::default(),
            dispatcher: None,
            views: Default::default(),
            memory: MemoryBackend::new(),
            processors: Mutex::new(ProjectionRuntime::new()),
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
        }
        cqrs.dependencies = cqrs.dependencies.merged(&self.dependencies);
        Ok(Registrations {
            cqrs,
            aggregates,
            views,
            adapters,
        })
    }
}

/// What the modules of an engine registered, merged and checked ([`Builder::registrations`]). An application whose
/// stores the engine does not have yet takes from it the rules of every aggregate type, the policies, the task handlers
/// with their dependencies and the observers, and builds the executors and processors of its stores with them.
pub struct Registrations {
    cqrs: Cqrs,
    aggregates: Vec<AggregateEntry>,
    views: Vec<ViewEntry>,
    adapters: Vec<BoxedTaskAdapter>,
}

impl Registrations {
    pub fn settings(&self) -> &Settings {
        &self.cqrs.settings
    }

    /// The policies of the task types: the reactions plan with them, the task runtime pauses by them.
    pub fn policies(&self) -> &Policies {
        &self.cqrs.policies
    }

    /// The aggregate types, in the order of the registrations.
    pub fn aggregate_types(&self) -> Vec<&'static str> {
        self.aggregates.iter().map(|entry| entry.name).collect()
    }

    /// The names of the views, in the order of the registrations.
    pub fn view_names(&self) -> Vec<&'static str> {
        self.views.iter().map(|entry| entry.name).collect()
    }

    /// The rules of the aggregate type `A`, once.
    pub fn take_rules<A: Aggregate + 'static>(&mut self) -> Option<A::Rules> {
        let entry = self.aggregates.iter_mut().find(|entry| entry.id == TypeId::of::<A>())?;
        entry.rules.take()?.downcast::<A::Rules>().ok().map(|rules| *rules)
    }

    /// The observer of every command, as the observer of the commands of `A`.
    pub fn command_observer<A: Aggregate>(&self) -> Option<Arc<dyn CommandObserver<A>>>
    where
        A::Command: CommandInfo,
        A::Error: Rejection,
    {
        Some(Arc::new(Commands(self.cqrs.command_observer.clone()?)))
    }

    /// The observer of the processor of the view named `projection` on this node.
    pub fn projection_observer(&self, projection: &'static str) -> Option<Arc<dyn ProjectionObserver>> {
        let observers = self.cqrs.projection_observers.as_ref()?;
        Some(observers(projection, &self.cqrs.owner))
    }

    pub fn dependency<T: ?Sized + 'static>(&self) -> Result<Arc<T>, MissingDependency> {
        self.cqrs.dependency::<T>()
    }

    /// The task handlers with their dependencies, once: the assembly adds the command executors and builds them.
    pub fn take_task_handlers(&mut self) -> TaskHandlers {
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
        },
    };
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::{num::NonZeroUsize, time::Duration};
    use uuid::Uuid;

    fn refusal(builder: Builder) -> BuildError {
        builder.build().err().unwrap()
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
        let builder = Cqrs::in_memory().env("APP", |name| (name == "APP_LIMIT").then(|| "x".to_owned()));
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
        let (cqrs, mailbox) = engine();
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
        let settings = Settings {
            snapshot_every: NonZeroUsize::new(3).unwrap(),
            conflict_retry: retry,
            lock_limits: limits,
            processor,
            node_name: Some("node-a".into()),
            ..Settings::default()
        };
        let defaults = Cqrs::in_memory().module(shows).registrations().unwrap();
        let set = Cqrs::in_memory().settings(settings).module(shows).registrations().unwrap();
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
        let named = Cqrs::in_memory().settings(Settings {
            node_name: Some("node-a".into()),
            ..Settings::default()
        });
        assert_eq!(named.registrations().unwrap().cqrs.owner, "node-a");
        // The reactions plan with the budget of each task type's policy.
        let (cqrs, _) = engine();
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
        let node = Settings {
            node_name: Some("node-a".into()),
            ..Settings::default()
        };
        let cqrs = Cqrs::in_memory()
            .settings(node)
            .module(shows)
            .module(wallets)
            .module(observers)
            .build()
            .unwrap();
        assert_eq!(cqrs.settings().node_name.as_deref(), Some("node-a"));
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
        assert_eq!(registrations.settings().snapshot_every.get(), 10);
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
}
