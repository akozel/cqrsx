//! The executor of one aggregate type over the engine's stores, and the adapter of the observer of every command.
#[cfg(feature = "event-store-postgres")]
use crate::postgres::outbox::PostgresEventRepositoryWithOutbox;
#[cfg(feature = "test-utils")]
use cqrsx::memory::event_repository::MemoryEventRepository;
use cqrsx::{
    command::{
        CommandError, CommandExecutor, CommitReport,
        executor::StoreCommandExecutor,
        observer::{AnyCommandObserver, AnyCommandReport, CommandObserver, CommandReport},
    },
    domain::{aggregate::Aggregate, command::Command, rejection::Rejection},
    journal::store::EventSourcedStore,
};
use std::{collections::HashMap, sync::Arc};

/// The executor of one aggregate type over the engine's journal: the variants are the stores, so a command from an
/// entry point waits for the store's own future, without an allocation.
pub(crate) enum Executor<A: Aggregate> {
    #[cfg(feature = "test-utils")]
    Memory(StoreCommandExecutor<A, EventSourcedStore<MemoryEventRepository, A>>),
    #[cfg(feature = "event-store-postgres")]
    Postgres(StoreCommandExecutor<A, EventSourcedStore<PostgresEventRepositoryWithOutbox, A>>),
}

impl<A> CommandExecutor<A> for Executor<A>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone + 'static,
    A::Rejection: Send + 'static,
{
    async fn execute_with_metadata(
        &self,
        id: &A::Id,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, CommandError<A::Rejection>> {
        match self {
            #[cfg(feature = "test-utils")]
            Self::Memory(executor) => executor.execute_with_metadata(id, command, metadata).await,
            #[cfg(feature = "event-store-postgres")]
            Self::Postgres(executor) => executor.execute_with_metadata(id, command, metadata).await,
        }
    }
}

/// The observer of every command as the observer of the commands of one aggregate type.
pub(crate) struct AnyObserverAdapter(pub(crate) Arc<dyn AnyCommandObserver>);

impl<A: Aggregate> CommandObserver<A> for AnyObserverAdapter
where
    A::Command: Command,
    A::Rejection: Rejection,
{
    fn observe(&self, report: &CommandReport<'_, A>) {
        self.0.observe(&AnyCommandReport::of(report));
    }
}
