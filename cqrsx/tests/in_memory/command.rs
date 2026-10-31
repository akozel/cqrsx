use crate::support::ledger::*;
use cqrsx::command::CommandExecutor;
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
    cqrsx::command::CommandGatewayError, cqrsx::command::executor::StoreCommandExecutor,
    cqrsx::command::retry::AggregateConflictRetryPolicy, cqrsx::domain::event::EventEnvelope, cqrsx::journal::EventStore,
};
#[derive(Clone)]
struct LostAcknowledgementStore {
    inner: Arc<cqrsx::testing::mem_store::MemStore<Ledger>>,
    commits: Arc<AtomicUsize>,
}

impl EventStore<Ledger> for LostAcknowledgementStore {
    type Context = <cqrsx::testing::mem_store::MemStore<Ledger> as EventStore<Ledger>>::Context;
    async fn load_events(&self, id: &str) -> Result<Vec<EventEnvelope<Ledger>>, cqrsx::journal::PersistenceError> {
        self.inner.load_events(id).await
    }
    async fn load_aggregate(&self, id: &str) -> Result<Self::Context, cqrsx::journal::PersistenceError> {
        self.inner.load_aggregate(id).await
    }
    async fn commit(
        &self,
        events: Vec<Delta>,
        context: Self::Context,
        metadata: HashMap<String, String>,
        tasks: &[cqrsx::task::NewTask],
    ) -> Result<Vec<EventEnvelope<Ledger>>, cqrsx::journal::PersistenceError> {
        self.inner.commit(events, context, metadata, tasks).await?;
        self.commits.fetch_add(1, Ordering::SeqCst);
        Err(cqrsx::journal::PersistenceError::ConnectionError(Box::new(io::Error::other(
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
    let executor = StoreCommandExecutor::new(
        store,
        Services(commands),
        AggregateConflictRetryPolicy::try_new(3, Duration::ZERO, Duration::ZERO, 0.0).unwrap(),
        cqrsx::command::locks::LockLimits::default(),
    );
    assert_eq!(
        CommandExecutor::execute(&executor, &"lost-ack".into(), Command::Adjust(1)).await,
        Err(CommandGatewayError::Unavailable)
    );
    assert_eq!(observed.commits.load(Ordering::SeqCst), 1);
    assert_eq!(*seen.lock().unwrap(), vec!["decide"]);
    assert_eq!(observed.inner.load_events("lost-ack").await.unwrap().len(), 1);
}
