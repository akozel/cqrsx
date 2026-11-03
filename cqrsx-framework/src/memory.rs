//! The stores of the framework in memory, over `cqrsx::memory` (the core's feature `test-utils`, which this feature
//! enables); `InMemory` lets a builder choose them.
use cqrsx::{
    domain::aggregate::Aggregate,
    memory::{
        checkpoint_store::MemoryCheckpointStore, event_repository::MemoryEventRepository, task_queue::MemoryTaskQueue,
        view_repository::MemoryViewRepository,
    },
    view::View,
};
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::Arc,
};

/// The stores in memory as the choice of a builder: `Builder::event_store(InMemory)` for the journal, with its outbox
/// and the task queue, and `Builder::projections(InMemory)` for the views and their checkpoints. For tests: a framework
/// that keeps its data in memory loses it with the process.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InMemory;

/// The stores of the framework in memory: the journal with its outbox, the task queue over that outbox, the checkpoints
/// of the processors and the repository of every view.
pub(crate) struct MemoryBackend {
    pub(crate) journal: MemoryEventRepository,
    pub(crate) queue: Arc<MemoryTaskQueue>,
    pub(crate) checkpoints: Arc<MemoryCheckpointStore>,
    /// The repository of every view in memory, by the view's type: an `Arc<MemoryViewRepository<V, A>>`.
    pub(crate) documents: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl MemoryBackend {
    pub(crate) fn new() -> Self {
        let journal = MemoryEventRepository::default();
        Self {
            queue: Arc::new(MemoryTaskQueue::new(journal.clone())),
            journal,
            checkpoints: Arc::default(),
            documents: HashMap::new(),
        }
    }

    /// The repository of the view `V` in memory, made on the first call.
    pub(crate) fn documents<V: View<A> + 'static, A: Aggregate + 'static>(&mut self) -> Arc<MemoryViewRepository<V, A>> {
        let documents = self
            .documents
            .entry(TypeId::of::<V>())
            .or_insert_with(|| Arc::new(MemoryViewRepository::<V, A>::default()));
        documents.clone().downcast().unwrap_or_default()
    }
}
