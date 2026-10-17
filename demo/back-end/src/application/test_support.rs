//! Use-case test doubles: the cqrsx `MemStore` behind the same `CqrsCommandExecutor` the
//! PostgreSQL wiring uses, and an event store that is never reachable.
use cqrsx::{
    cqrs::{
        AggregateError, CqrsFramework,
        aggregate::Aggregate,
        command::{executor::CqrsCommandExecutor, retry::AggregateConflictRetryPolicy},
        event::EventEnvelope,
        store::EventStore,
        testing::mem_store::MemStoreAggregateContext,
    },
    task::NewTask,
};
use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
};

pub type TestFramework<A, S> = Box<dyn Fn(Vec<NewTask>) -> CqrsFramework<A, S> + Send + Sync>;
pub type TestExecutor<A, S> = CqrsCommandExecutor<A, TestFramework<A, S>>;

/// Every task list handed to the executor, in call order and whatever the outcome.
#[derive(Clone, Default)]
pub struct BoundTasks(Arc<Mutex<Vec<NewTask>>>);

impl BoundTasks {
    pub fn take(&self) -> Vec<NewTask> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

/// A command executor over `store` without event handlers, recording the bound tasks.
pub fn executor<A, S>(store: S, services: A::Services, bound: BoundTasks) -> TestExecutor<A, S>
where
    A: Aggregate + 'static,
    A::Services: Clone + 'static,
    S: EventStore<A> + Clone + 'static,
{
    let build: TestFramework<A, S> = Box::new(move |tasks| {
        bound.0.lock().unwrap().extend(tasks);
        CqrsFramework::new(store.clone(), vec![], services.clone())
    });
    CqrsCommandExecutor::new(build, AggregateConflictRetryPolicy::default())
}

/// An event store that is never reachable: proves that a path does not touch storage, and drives
/// the "storage unavailable" branches.
#[derive(Clone, Copy, Default)]
pub struct UnreachableStore;

fn unreachable<E: Error>() -> AggregateError<E> {
    AggregateError::DatabaseConnectionError("the event store is unreachable".into())
}

impl<A: Aggregate> EventStore<A> for UnreachableStore {
    type AC = MemStoreAggregateContext<A>;

    async fn load_events(&self, _: &str) -> Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>> {
        Err(unreachable())
    }

    async fn load_aggregate(&self, _: &str) -> Result<Self::AC, AggregateError<A::Error>> {
        Err(unreachable())
    }

    async fn commit(
        &self,
        _: Vec<A::Event>,
        _: Self::AC,
        _: HashMap<String, String>,
    ) -> Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>> {
        Err(unreachable())
    }
}
