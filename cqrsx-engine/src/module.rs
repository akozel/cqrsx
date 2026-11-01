//! A module: what one part of an application plugs into the engine, registered in its root.
use crate::{Cqrs, build::BuildError};
use cqrsx::{
    command::observer::AnyCommandObserver,
    domain::{aggregate::Aggregate, command::Command, rejection::Rejection},
    projection::processor::observer::ProjectionObserver,
    reactions::Reactions,
    task::{
        Task,
        policy::{Policies, PolicyError, PolicyName, Retry},
    },
    view::View,
    worker::{TaskHandler, adapter::boxed::BoxedTaskAdapter, dependencies::Dependencies},
};
use serde::de::DeserializeOwned;
use std::{
    any::{Any, TypeId},
    fmt,
    num::NonZeroU16,
    sync::Arc,
};

/// The processor an observer of [`Module::observe_projections`] is made for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservedProcessor<'a> {
    /// The name of the processor's view, [`View::NAME`].
    pub projection: &'static str,
    /// The node the processor runs on: the owner of its checkpoint while it holds the lease.
    pub node: &'a str,
}

/// The observer of the processor of a view, made from the view's name and the node's (the owner of its checkpoint).
pub(crate) type ProjectionObserverFactory = Arc<dyn Fn(ObservedProcessor<'_>) -> Arc<dyn ProjectionObserver> + Send + Sync>;

type Bind = fn(Policies, PolicyName) -> Result<Policies, PolicyError>;

/// A policy of [`Module::policy`].
pub(crate) struct PolicyEntry {
    pub(crate) name: PolicyName,
    pub(crate) retry: Retry,
    pub(crate) max_attempts: NonZeroU16,
}

/// A task type: its handler and the policy it is bound to.
pub(crate) struct TaskRegistration {
    pub(crate) adapter: BoxedTaskAdapter,
    pub(crate) binding: Option<PolicyBinding>,
}

/// The policy a task type is bound to, and how its payload type binds itself.
pub(crate) struct PolicyBinding {
    pub(crate) name: PolicyName,
    pub(crate) bind: Bind,
}

/// What one part of an application plugs into the engine; the engine checks it when it is built:
/// - an aggregate type with its rules, whose reactions plan the tasks of its decisions ([`aggregate`](Self::aggregate));
/// - the views of an aggregate, each fed by a projection processor ([`view`](Self::view));
/// - task types with their handlers, bound to policies ([`task`](Self::task), [`policy`](Self::policy));
/// - what the handlers depend on, by type ([`provide`](Self::provide));
/// - the observers of every command and every processor of the engine ([`observe_commands`](Self::observe_commands),
///   [`observe_projections`](Self::observe_projections)), e.g. for an activity feed;
/// - with the feature `http`, its HTTP routes, served with the routes of every module (`routes`).
#[must_use = "a module takes effect once its root returns it to the engine's builder"]
pub struct Module {
    pub(crate) name: &'static str,
    pub(crate) aggregates: Vec<AggregateEntry>,
    pub(crate) views: Vec<ViewEntry>,
    pub(crate) policies: Vec<PolicyEntry>,
    pub(crate) tasks: Vec<TaskRegistration>,
    pub(crate) dependencies: Dependencies,
    pub(crate) command_observers: Vec<Arc<dyn AnyCommandObserver>>,
    pub(crate) projection_observers: Vec<ProjectionObserverFactory>,
    #[cfg(feature = "http")]
    pub(crate) routes: Option<axum::Router<Arc<Cqrs>>>,
}

// By hand: the registrations hold closures and trait objects; they print as what they name.
impl fmt::Debug for Module {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let aggregates: Vec<&str> = self.aggregates.iter().map(|aggregate| aggregate.name).collect();
        let views: Vec<&str> = self.views.iter().map(|view| view.name).collect();
        let policies: Vec<PolicyName> = self.policies.iter().map(|policy| policy.name).collect();
        f.debug_struct("Module")
            .field("name", &self.name)
            .field("aggregates", &aggregates)
            .field("views", &views)
            .field("policies", &policies)
            .field("tasks", &self.tasks.len())
            .field("dependencies", &self.dependencies)
            .finish_non_exhaustive()
    }
}

impl Module {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            aggregates: Vec::new(),
            views: Vec::new(),
            policies: Vec::new(),
            tasks: Vec::new(),
            dependencies: Dependencies::default(),
            command_observers: Vec::new(),
            projection_observers: Vec::new(),
            #[cfg(feature = "http")]
            routes: None,
        }
    }

    /// The aggregate type `A` with its `rules`: one executor of its commands, with its reactions. What is known of a
    /// command ([`Command`]) and the code of a rejection ([`Rejection`]) are what an observer of every command names.
    pub fn aggregate<A>(mut self, rules: A::Rules) -> Self
    where
        A: Reactions + Clone + 'static,
        A::Command: Command + Clone,
        A::Error: Rejection + Send + 'static,
    {
        self.aggregates.push(AggregateEntry {
            id: TypeId::of::<A>(),
            name: A::TYPE,
            rules: Some(Box::new(rules)),
            install: Cqrs::install_aggregate::<A>,
        });
        self
    }

    /// The view `V` of the aggregate `A`, fed from the journal of `A` under its name.
    pub fn view<V: View<A> + PartialEq + 'static, A: Aggregate + 'static>(mut self) -> Self {
        self.views.push(ViewEntry {
            name: V::NAME,
            install: Cqrs::install_view::<V, A>,
        });
        self
    }

    /// The task type of `handler`; [`TaskEntry::policy`] binds it to a policy.
    pub fn task<T, H>(self, handler: H) -> TaskEntry
    where
        T: Task + DeserializeOwned + Send + 'static,
        H: TaskHandler<T> + 'static,
    {
        TaskEntry {
            module: self,
            adapter: BoxedTaskAdapter::json::<T, H>(handler),
            bind: Policies::with_task::<T>,
        }
    }

    /// The policy `name`: the pause before a retry and the attempt budget of its task types.
    pub fn policy(mut self, name: PolicyName, retry: Retry, max_attempts: NonZeroU16) -> Self {
        self.policies.push(PolicyEntry {
            name,
            retry,
            max_attempts,
        });
        self
    }

    /// `dependency` for every handler that names its type `T`, a trait object too.
    pub fn provide<T: ?Sized + Send + Sync + 'static>(mut self, dependency: Arc<T>) -> Self {
        self.dependencies = self.dependencies.provide(dependency);
        self
    }

    /// `observer` hears every command of the engine; one per engine.
    pub fn observe_commands(mut self, observer: Arc<dyn AnyCommandObserver>) -> Self {
        self.command_observers.push(observer);
        self
    }

    /// `observers` makes the observer of every processor of the engine from its view's name and the node's; one per
    /// engine.
    pub fn observe_projections(
        mut self,
        observers: impl Fn(ObservedProcessor<'_>) -> Arc<dyn ProjectionObserver> + Send + Sync + 'static,
    ) -> Self {
        self.projection_observers.push(Arc::new(observers));
        self
    }
}

/// A task type of [`Module::task`], bound to a policy or to none.
#[derive(Debug)]
#[must_use = "a task type is registered by `policy` or `without_policy`"]
pub struct TaskEntry {
    module: Module,
    adapter: BoxedTaskAdapter,
    bind: Bind,
}

impl TaskEntry {
    /// The task type waits the pauses and gets the attempts of the policy `name`.
    pub fn policy(mut self, name: PolicyName) -> Module {
        self.module.tasks.push(TaskRegistration {
            adapter: self.adapter,
            binding: Some(PolicyBinding { name, bind: self.bind }),
        });
        self.module
    }

    /// The task type keeps the engine's defaults: the default budget, the queue's redelivery backoff.
    pub fn without_policy(mut self) -> Module {
        self.module.tasks.push(TaskRegistration {
            adapter: self.adapter,
            binding: None,
        });
        self.module
    }
}

/// A registered aggregate type: its rules until its executor takes them, and how the engine installs it.
pub(crate) struct AggregateEntry {
    pub(crate) id: TypeId,
    pub(crate) name: &'static str,
    pub(crate) rules: Option<Box<dyn Any + Send>>,
    pub(crate) install: fn(&mut Cqrs, Box<dyn Any + Send>) -> Result<(), BuildError>,
}

/// A registered view: its name and how the engine installs its processor.
pub(crate) struct ViewEntry {
    pub(crate) name: &'static str,
    pub(crate) install: fn(&mut Cqrs),
}
