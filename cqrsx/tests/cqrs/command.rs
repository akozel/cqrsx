use crate::support::ledger::*;
use cqrsx::cqrs::command::CommandExecutor;
use std::{
    collections::HashMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use {
    cqrsx::cqrs::AggregateError, cqrsx::cqrs::CqrsFramework, cqrsx::cqrs::command::CommandGatewayError,
    cqrsx::cqrs::command::executor::CqrsCommandExecutor, cqrsx::cqrs::command::retry::AggregateConflictRetryPolicy,
    cqrsx::cqrs::event::EventEnvelope, cqrsx::cqrs::store::EventStore,
};
#[derive(Clone)]
struct LostAcknowledgementStore {
    inner: Arc<cqrsx::cqrs::testing::mem_store::MemStore<Ledger>>,
    commits: Arc<AtomicUsize>,
}

impl EventStore<Ledger> for LostAcknowledgementStore {
    type AC = <cqrsx::cqrs::testing::mem_store::MemStore<Ledger> as EventStore<Ledger>>::AC;
    async fn load_events(&self, id: &str) -> Result<Vec<EventEnvelope<Ledger>>, AggregateError<Rejected>> {
        self.inner.load_events(id).await
    }
    async fn load_aggregate(&self, id: &str) -> Result<Self::AC, AggregateError<Rejected>> {
        self.inner.load_aggregate(id).await
    }
    async fn commit(
        &self,
        events: Vec<Delta>,
        context: Self::AC,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EventEnvelope<Ledger>>, AggregateError<Rejected>> {
        self.inner.commit(events, context, metadata).await?;
        self.commits.fetch_add(1, Ordering::SeqCst);
        Err(AggregateError::DatabaseConnectionError(Box::new(io::Error::other(
            "acknowledgement lost after commit",
        ))))
    }
}

#[tokio::test]
async fn unknown_commit_outcome_never_fast_retries_an_already_committed_command() {
    let store = LostAcknowledgementStore {
        inner: Default::default(),
        commits: Arc::default(),
    };
    let observed = store.clone();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let commands = seen.clone();
    let executor = CqrsCommandExecutor::<Ledger, _>::new(
        move |_| CqrsFramework::new(store.clone(), vec![], Services(commands.clone())),
        AggregateConflictRetryPolicy::new(3, Duration::ZERO, Duration::ZERO, 0.0).unwrap(),
    );
    assert_eq!(
        CommandExecutor::execute(&executor, "lost-ack", Command::Adjust(vec![1]), vec![]).await,
        Err(CommandGatewayError::Unavailable)
    );
    assert_eq!(observed.commits.load(Ordering::SeqCst), 1);
    assert_eq!(*seen.lock().unwrap(), vec!["handle"]);
    assert_eq!(observed.inner.load_events("lost-ack").await.unwrap().len(), 1);
}
