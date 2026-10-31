//! The executor of one aggregate type over the engine's stores, and the adapter of the observer of every command.
#[cfg(feature = "postgres")]
use cqrsx::postgres::outbox::PostgresEventRepositoryWithOutbox;
use cqrsx::{
    command::{
        CommandExecutor, CommandGatewayError, CommitReport,
        executor::StoreCommandExecutor,
        observer::{AnyCommandObserver, AnyCommandReport, CommandObserver, CommandReport},
    },
    domain::{aggregate::Aggregate, command::Command, rejection::Rejection},
    journal::persisted::PersistedEventStore,
    memory::event_repository::MemEventRepository,
};
use std::{collections::HashMap, sync::Arc};

/// The executor of one aggregate type over the engine's journal: the variants are the stores, so a command from an
/// entry point waits for the store's own future, without an allocation.
pub(crate) enum Executor<A: Aggregate> {
    Memory(StoreCommandExecutor<A, PersistedEventStore<MemEventRepository, A>>),
    #[cfg(feature = "postgres")]
    Postgres(StoreCommandExecutor<A, PersistedEventStore<PostgresEventRepositoryWithOutbox, A>>),
}

impl<A> CommandExecutor<A> for Executor<A>
where
    A: Aggregate + Clone + 'static,
    A::Command: Clone + 'static,
    A::Error: Send + 'static,
{
    async fn execute_with_metadata(
        &self,
        id: &A::Id,
        command: A::Command,
        metadata: HashMap<String, String>,
    ) -> Result<CommitReport<A>, CommandGatewayError<A::Error>> {
        match self {
            Self::Memory(executor) => executor.execute_with_metadata(id, command, metadata).await,
            #[cfg(feature = "postgres")]
            Self::Postgres(executor) => executor.execute_with_metadata(id, command, metadata).await,
        }
    }
}

/// The observer of every command as the observer of the commands of one aggregate type.
pub(crate) struct Commands(pub(crate) Arc<dyn AnyCommandObserver>);

impl<A: Aggregate> CommandObserver<A> for Commands
where
    A::Command: Command,
    A::Error: Rejection,
{
    fn command(&self, report: &CommandReport<'_, A>) {
        self.0.command(&AnyCommandReport::of(report));
    }
}
