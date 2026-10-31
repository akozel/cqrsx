// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use std::collections::HashMap;

use cqrsx::command::AggregateError;
use cqrsx::command::framework::CqrsFramework;
use cqrsx::journal::EventStore;
use cqrsx::testing::mem_store::MemStore;

use crate::support::scenario::*;

#[tokio::test]
async fn framework_test() {
    let event_store = MemStore::<TestAggregate>::default();

    let cqrs = CqrsFramework::new(event_store.clone(), TestService);
    let uuid = uuid::Uuid::new_v4().to_string();
    let id = uuid.clone();
    let metadata = metadata();
    let committed = cqrs
        .execute_with_metadata(
            &id,
            TestCommand::ConfirmTest(ConfirmTest { test_name: uuid.clone() }),
            metadata,
        )
        .await
        .unwrap();

    assert_eq!(1, event_store.load_events(&id).await.unwrap().len());
    assert_eq!(committed.events.iter().map(|e| e.sequence).collect::<Vec<_>>(), [1]);

    let test = "TEST_A";
    let id = uuid.clone();
    let committed = cqrs
        .execute(
            &id,
            TestCommand::ConfirmTest(ConfirmTest {
                test_name: test.to_string(),
            }),
        )
        .await
        .unwrap();

    assert_eq!(committed.events.iter().map(|e| e.sequence).collect::<Vec<_>>(), [2]);
    let stored_event_count = event_store.load_events(uuid.clone().as_str()).await.unwrap().len();
    assert_eq!(2, stored_event_count);

    let id = uuid.clone();
    let err = cqrs
        .execute(
            &id,
            TestCommand::ConfirmTest(ConfirmTest {
                test_name: test.to_string(),
            }),
        )
        .await
        .unwrap_err();
    match err {
        AggregateError::Domain(payload) => {
            assert_eq!("test already performed", payload.0.as_str());
        }
        _ => panic!("not the expected error"),
    }

    let stored_event_count = event_store.load_events(uuid.clone().as_str()).await.unwrap().len();
    assert_eq!(2, stored_event_count);
}

mod contracts {
    use super::*;
    use cqrsx::{
        command::{CommandGatewayError, executor::CommandExecutorCore, locks::LockLimits, retry::AggregateConflictRetryPolicy},
        domain::event::EventEnvelope,
        journal::wakeup::CommitWakeup,
        task::NewTask,
    };
    use std::{
        future::Future,
        pin::pin,
        sync::{Arc, Mutex},
        task::{Context, Waker},
        time::Duration,
    };
    use {
        crate::support::ledger::*, cqrsx::journal::AggregateContext, cqrsx::journal::EventStore,
        cqrsx::testing::mem_store::MemStore, cqrsx::testing::mem_store::MemStoreAggregateContext,
    };

    type Calls = Arc<Mutex<Vec<&'static str>>>;

    #[derive(Clone)]
    struct Store {
        inner: MemStore<Ledger>,
        calls: Calls,
        failure: Option<&'static str>,
    }
    impl EventStore<Ledger> for Store {
        type Context = MemStoreAggregateContext<Ledger>;
        async fn load_events(&self, id: &str) -> Result<Vec<EventEnvelope<Ledger>>, AggregateError<Rejected>> {
            self.inner.load_events(id).await
        }
        async fn load_aggregate(&self, id: &str) -> Result<Self::Context, AggregateError<Rejected>> {
            self.calls.lock().unwrap().push("load");
            if self.failure == Some("load") {
                return Err(AggregateError::DatabaseConnectionError("load failed".into()));
            }
            self.inner.load_aggregate(id).await
        }
        async fn commit(
            &self,
            events: Vec<Delta>,
            context: Self::Context,
            metadata: HashMap<String, String>,
            tasks: &[NewTask],
        ) -> Result<Vec<EventEnvelope<Ledger>>, AggregateError<Rejected>> {
            self.calls.lock().unwrap().push("commit");
            // Commit receives the unchanged committed state.
            assert!(context.aggregate().history.is_empty());
            if self.failure == Some("commit") {
                return Err(AggregateError::AggregateConflict);
            }
            self.inner.commit(events, context, metadata, tasks).await
        }
    }
    /// An executor over `store` with one attempt per command that wakes `wakeup` after a commit with an event.
    fn executor<S>(store: S, calls: &Calls, wakeup: &CommitWakeup) -> CommandExecutorCore<Ledger, S>
    where
        S: EventStore<Ledger> + Send + Sync,
    {
        let services = Services(calls.clone());
        let one_attempt = AggregateConflictRetryPolicy::try_new(1, Duration::ZERO, Duration::ZERO, 0.0).unwrap();
        CommandExecutorCore::new(CqrsFramework::new(store, services), one_attempt, LockLimits::default())
            .with_commit_wakeup(wakeup.clone())
    }

    #[tokio::test]
    async fn load_decide_commit_preserves_the_rules_and_metadata_and_returns_the_event() {
        let calls = Calls::default();
        let store = Store {
            inner: MemStore::default(),
            calls: Arc::clone(&calls),
            failure: None,
        };
        let framework = CqrsFramework::new(store, Services(Arc::clone(&calls)));
        let metadata = HashMap::from([("trace".into(), "данные 🦀".into())]);
        let events = framework
            .execute_with_metadata(&"account".into(), Command::Adjust(5), metadata.clone())
            .await
            .unwrap()
            .events;
        assert_eq!(*calls.lock().unwrap(), ["load", "decide", "commit"]);
        assert_eq!(events.len(), 1);
        for (i, event) in events.iter().enumerate() {
            assert_eq!(event.aggregate_id, "account");
            assert_eq!(event.sequence, i + 1);
            assert_eq!(event.payload, Delta([5][i]));
            assert_eq!(event.metadata, metadata);
        }
    }
    #[tokio::test]
    async fn every_failure_boundary_wakes_nobody() {
        for (failure, command, expected) in [
            (Some("load"), Command::Adjust(5), vec!["load"]),
            (Some("commit"), Command::Adjust(5), vec!["load", "decide", "commit"]),
            (None, Command::Adjust(-6), vec!["load", "decide"]),
        ] {
            let calls = Calls::default();
            let wakeup = CommitWakeup::new();
            let inner = MemStore::default();
            let store = Store {
                inner: inner.clone(),
                calls: calls.clone(),
                failure,
            };
            let executor = executor(store, &calls, &wakeup);
            // A processor waiting for the next commit, as it would be at its poll.
            let mut woken = pin!(wakeup.wait());
            assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
            let result = executor.execute(&"account".into(), command).await;
            match failure {
                Some("load") => assert_eq!(result, Err(CommandGatewayError::Unavailable)),
                Some("commit") => assert_eq!(result, Err(CommandGatewayError::Conflict)),
                _ => assert_eq!(result, Err(CommandGatewayError::Domain(Rejected))),
            }
            assert_eq!(*calls.lock().unwrap(), expected);
            assert!(woken.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
            assert!(inner.load_events("account").await.unwrap().is_empty());
        }
    }
}
