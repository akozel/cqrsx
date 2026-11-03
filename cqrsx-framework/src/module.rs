//! A module: what one part of an application plugs into the framework, registered in its root.
use crate::{Cqrs, build::BuildError};
use cqrsx::{
    command::observer::AnyCommandObserver,
    domain::{aggregate::Aggregate, command::Command, rejection::Rejection},
    projection::processor::observer::ProjectionObserver,
    reactions::Reactions,
    task::{
        Task,
        policy::{PoliciesBuilder, TaskPolicy},
    },
    view::View,
    worker::{TaskHandler, adapter::boxed::BoxedTaskAdapter, dependencies::Dependencies},
};
use serde::de::DeserializeOwned;
use std::{
    any::{Any, TypeId},
    fmt,
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

/// How a task type registers its policy with the table: `PoliciesBuilder::task::<T>`.
type RegisterPolicy = fn(PoliciesBuilder, TaskPolicy) -> PoliciesBuilder;

/// A task type: its handler, its policy and how its payload type registers the policy.
pub(crate) struct TaskEntry {
    pub(crate) adapter: BoxedTaskAdapter,
    pub(crate) policy: TaskPolicy,
    pub(crate) register: RegisterPolicy,
}

/// What one part of an application plugs into the framework; the framework checks it when it is built:
/// - an aggregate type with its rules, whose reactions plan the tasks of its decisions ([`aggregate`](Self::aggregate));
/// - the views of an aggregate, each fed by a projection processor ([`view`](Self::view));
/// - task types with their handlers and the policy of each ([`task`](Self::task));
/// - what the handlers depend on, by type ([`provide`](Self::provide));
/// - the observers of every command and every processor of the framework ([`observe_commands`](Self::observe_commands),
///   [`observe_projections`](Self::observe_projections)), e.g. for an activity feed;
/// - its endpoints, in the type of the server that serves them ([`endpoints`](Self::endpoints); with the feature `axum`
///   `routes`).
#[must_use = "a module takes effect once its root returns it to the framework's builder"]
pub struct Module {
    pub(crate) name: &'static str,
    pub(crate) aggregates: Vec<AggregateEntry>,
    pub(crate) views: Vec<ViewEntry>,
    pub(crate) tasks: Vec<TaskEntry>,
    pub(crate) dependencies: Dependencies,
    pub(crate) command_observers: Vec<Arc<dyn AnyCommandObserver>>,
    pub(crate) projection_observers: Vec<ProjectionObserverFactory>,
    pub(crate) endpoints: Vec<Box<dyn Any + Send + Sync>>,
}

// By hand: the registrations hold closures and trait objects; they print as what they name.
impl fmt::Debug for Module {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let aggregates: Vec<&str> = self.aggregates.iter().map(|aggregate| aggregate.name).collect();
        let views: Vec<&str> = self.views.iter().map(|view| view.name).collect();
        f.debug_struct("Module")
            .field("name", &self.name)
            .field("aggregates", &aggregates)
            .field("views", &views)
            .field("tasks", &self.tasks.len())
            .field("dependencies", &self.dependencies)
            .field("endpoints", &self.endpoints.len())
            .finish_non_exhaustive()
    }
}

impl Module {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            aggregates: Vec::new(),
            views: Vec::new(),
            tasks: Vec::new(),
            dependencies: Dependencies::default(),
            command_observers: Vec::new(),
            projection_observers: Vec::new(),
            endpoints: Vec::new(),
        }
    }

    /// The aggregate type `A` with its `rules`: one executor of its commands, with its reactions. What is known of a
    /// command ([`Command`]) and the code of a rejection ([`Rejection`]) are what an observer of every command names.
    pub fn aggregate<A>(mut self, rules: A::Rules) -> Self
    where
        A: Reactions + Clone + 'static,
        A::Command: Command + Clone,
        A::Rejection: Rejection + Send + 'static,
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

    /// The task type of `handler` and the policy its tasks follow: the pause before a retry and the attempt budget
    /// (`TaskPolicy::default()` keeps the library's defaults). A policy is a `Copy` value: tasks of one module or of
    /// several share one by passing the same value. A task type is registered once.
    pub fn task<T, H>(mut self, handler: H, policy: TaskPolicy) -> Self
    where
        T: Task + DeserializeOwned + Send + 'static,
        H: TaskHandler<T> + 'static,
    {
        self.tasks.push(TaskEntry {
            adapter: BoxedTaskAdapter::json::<T, H>(handler),
            policy,
            register: PoliciesBuilder::task::<T>,
        });
        self
    }

    /// `dependency` for every handler that names its type `T`, a trait object too.
    pub fn provide<T: ?Sized + Send + Sync + 'static>(mut self, dependency: Arc<T>) -> Self {
        self.dependencies = self.dependencies.provide(dependency);
        self
    }

    /// `observer` hears every command of the framework; one per framework.
    pub fn observe_commands(mut self, observer: Arc<dyn AnyCommandObserver>) -> Self {
        self.command_observers.push(observer);
        self
    }

    /// `observers` makes the observer of every processor of the framework from its view's name and the node's; one per
    /// framework.
    pub fn observe_projections(
        mut self,
        observers: impl Fn(ObservedProcessor<'_>) -> Arc<dyn ProjectionObserver> + Send + Sync + 'static,
    ) -> Self {
        self.projection_observers.push(Arc::new(observers));
        self
    }

    /// The endpoints of the module in the type of the server that serves them: the routes of axum
    /// ([`Module::routes`], feature `axum`), or those of a server of another protocol the application brings; the
    /// framework keeps them in the order of registration for [`Cqrs::endpoints`].
    pub fn endpoints<T: Any + Send + Sync>(mut self, endpoints: T) -> Self {
        self.endpoints.push(Box::new(endpoints));
        self
    }
}

/// A registered aggregate type: its rules until its executor takes them, and how the framework installs it.
pub(crate) struct AggregateEntry {
    pub(crate) id: TypeId,
    pub(crate) name: &'static str,
    pub(crate) rules: Option<Box<dyn Any + Send>>,
    pub(crate) install: fn(&mut Cqrs, Box<dyn Any + Send>) -> Result<(), BuildError>,
}

/// A registered view: its name and how the framework installs its processor.
pub(crate) struct ViewEntry {
    pub(crate) name: &'static str,
    pub(crate) install: fn(&mut Cqrs) -> Result<(), BuildError>,
}
