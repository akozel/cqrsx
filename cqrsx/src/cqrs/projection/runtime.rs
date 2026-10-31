//! Several projection processors run as one: for a host that starts the read side next to its task runtime.
use crate::{
    cqrs::projection::{checkpoint::CheckpointStore, processor::ProjectionProcessor},
    domain::aggregate::Aggregate,
    journal::repository::PersistedEventRepository,
};
use std::{collections::HashMap, fmt, future::Future, pin::Pin};
use thiserror::Error;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::instrument::WithSubscriber;

type Start = Box<dyn FnOnce(CancellationToken) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// The processors of a process, started together and stopped by one token. Building it starts nothing;
/// `run` starts every processor on its first poll and ends when all of them have stopped. A processor that
/// panics (an observer that panics, a bug) stops the others, so the read side never runs half: the error
/// names it.
#[derive(Default)]
#[must_use = "the processors run only while `run` runs"]
pub struct ProjectionRuntime {
    processors: Vec<(String, Start)>,
}

// By hand: a processor prints as its name; how it starts is a closure.
impl fmt::Debug for ProjectionRuntime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&str> = self.processors.iter().map(|(name, _)| name.as_str()).collect();
        f.debug_struct("ProjectionRuntime")
            .field("processors", &names)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProjectionRuntimeError {
    #[error("projection processor {projection} panicked")]
    Panicked { projection: String },
}

impl ProjectionRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_processor<A, R, C>(mut self, processor: ProjectionProcessor<A, R, C>) -> Self
    where
        A: Aggregate + 'static,
        R: PersistedEventRepository + 'static,
        C: CheckpointStore + 'static,
    {
        let name = processor.name().to_owned();
        self.processors
            .push((name, Box::new(move |shutdown| Box::pin(processor.run(shutdown)))));
        self
    }

    pub fn len(&self) -> usize {
        self.processors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.processors.is_empty()
    }

    /// Runs every processor until `shutdown` is cancelled and all of them have stopped; dropping the future
    /// cancels the token, so the processors stop on their own.
    pub async fn run(self, shutdown: CancellationToken) -> Result<(), ProjectionRuntimeError> {
        let _cancel_on_exit = shutdown.clone().drop_guard();
        let mut tasks = JoinSet::new();
        let mut names = HashMap::new();
        for (name, start) in self.processors {
            let handle = tasks.spawn(start(shutdown.clone()).with_current_subscriber());
            names.insert(handle.id(), name);
        }
        let mut first = None;
        while let Some(result) = tasks.join_next_with_id().await {
            if let Err(error) = result {
                shutdown.cancel();
                let projection = names.remove(&error.id()).unwrap_or_default();
                first.get_or_insert(ProjectionRuntimeError::Panicked { projection });
            }
        }
        first.map_or(Ok(()), Err)
    }
}
