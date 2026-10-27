//! The engine: an application of modules ([`module::Module`]) assembled from the components of cqrsx, which keep
//! their logic: an executor per aggregate type with its reactions and policies, a projection processor per view woken
//! by the commits of its type, one dispatcher of the task handlers, the observers of every command and processor.
//!
//! [`Cqrs::in_memory`] starts a [`build::Builder`]: `build` sets up the modules ([`setup::Setup`]), checks what they
//! register and assembles the engine over stores in memory. [`Cqrs::from`] is an entry point of the commands
//! ([`outcome`]), [`Cqrs::view`] and [`Cqrs::views`] query and operate the views ([`views`]); in memory
//! [`Cqrs::run_tasks`] runs the due tasks and [`Cqrs::catch_up`] lets the views catch up with the journal.
pub mod build;
mod memory;
pub mod module;
pub mod outcome;
pub mod setup;
#[cfg(test)]
mod test_support;
pub mod views;

use crate::{
    cqrs::{
        CqrsFramework,
        aggregate::Aggregate,
        command::{
            CommandInfo,
            executor::CommandExecutorCore,
            observer::{AnyCommandObserver, AnyCommandReport, CommandObserver, CommandReport},
            wakeup::CommitWakeup,
        },
        projection::{
            RebuildableProjection, View, ViewProjection, checkpoint::CheckpointStore, processor::ProjectionProcessor,
            repository::ViewRepository, runtime::ProjectionRuntime,
        },
        reactions::{Reactions, ReactionsPlanner},
        rejection::Rejection,
        store::{EventStore, persisted::PersistedEventStore, repository::PersistedEventRepository},
        testing::{mem_event_repository::MemEventRepository, mem_view_repository::MemViewRepository},
    },
    engine::{build::BuildError, module::ProjectionObservers, setup::Settings, views::Registry},
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
    executors: Executors,
    wakeups: HashMap<TypeId, CommitWakeup>,
    dispatcher: Option<Arc<TaskDispatcher<BoxedTaskAdapter>>>,
    views: Registry,
    memory: memory::MemoryBackend,
    processors: Mutex<ProjectionRuntime>,
    shutdown: CancellationToken,
    /// Cancels `shutdown` when the engine is dropped.
    _stop: DropGuard,
}

impl Cqrs {
    /// An engine whose stores are in memory: the journal with its outbox and task queue, the checkpoints, a repository
    /// per view.
    pub fn in_memory() -> build::Builder {
        build::Builder::default()
    }

    /// The dependency of type `T` a module or the builder provided.
    pub fn dependency<T: ?Sized + 'static>(&self) -> Result<Arc<T>, MissingDependency> {
        self.dependencies.shared::<T>().ok_or(MissingDependency(type_name::<T>()))
    }

    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// Installs the executor of `A` over the journal in memory, with the rules its module registered.
    fn install_aggregate<A>(&mut self, rules: Box<dyn Any + Send>) -> Result<(), BuildError>
    where
        A: Reactions + Clone + 'static,
        A::Command: CommandInfo + Clone,
        A::Error: Rejection + Send + 'static,
    {
        // The entry of `A` holds the rules of `A`.
        rules.downcast::<A::Rules>().map_or(Ok(()), |rules| {
            let executor = Arc::new(self.executor::<A, _>(self.store::<A>(), *rules));
            let executors = self.executors.clone().with::<A, _>(executor);
            self.executors = executors.map_err(|twice| BuildError::AggregateTwice(twice.0))?;
            Ok(())
        })
    }

    /// The store of the commands of `A`: the journal in memory, a snapshot every `snapshot_every` events.
    fn store<A: Aggregate>(&self) -> PersistedEventStore<MemEventRepository, A> {
        PersistedEventStore::new_snapshot_store(self.memory.journal.clone(), self.settings.snapshot_every.get())
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
        let executor = CommandExecutorCore::new(framework, self.settings.conflict_retry, self.settings.lock_limits)
            .with_task_planner(ReactionsPlanner::<A>::new(self.policies.clone()))
            .with_commit_wakeup(self.wakeup::<A>());
        match &self.command_observer {
            Some(observer) => executor.with_observer(Arc::new(Commands(observer.clone()))),
            None => executor,
        }
    }

    /// Installs the processor of `V` over the journal, the checkpoints and a repository in memory, which the queries
    /// read.
    fn install_view<V: View<A> + PartialEq + 'static, A: Aggregate + 'static>(&mut self) {
        let documents = Arc::new(MemViewRepository::<V, A>::default());
        let journal = Arc::new(self.memory.journal.clone());
        let processor = self.processor::<V, A, _, _, _>(journal, self.memory.checkpoints.clone(), documents.clone());
        let processors = self.processors.get_mut().unwrap_or_else(PoisonError::into_inner);
        *processors = std::mem::take(processors).with_processor(processor);
        self.views.register::<V, A>(documents, self.memory.journal.clone());
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
        let processor = ProjectionProcessor::new(V::NAME, journal, checkpoints, projection, self.settings.processor)
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
