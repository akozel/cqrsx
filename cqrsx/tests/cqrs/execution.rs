// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use std::collections::HashMap;

use cqrsx::cqrs::AggregateError;
use cqrsx::cqrs::CqrsFramework;
use cqrsx::cqrs::event_handler::EventHandler;
use cqrsx::cqrs::store::EventStore;
use cqrsx::cqrs::testing::mem_store::MemStore;

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
    assert_eq!(committed.iter().map(|e| e.sequence).collect::<Vec<_>>(), [1]);

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

    assert_eq!(committed.iter().map(|e| e.sequence).collect::<Vec<_>>(), [2]);
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
    use async_trait::async_trait;
    use cqrsx::{
        cqrs::{
            command::{
                CommandGatewayError, FailedHandler, executor::CqrsCommandExecutor, locks::LockLimits,
                retry::AggregateConflictRetryPolicy,
            },
            event::EventEnvelope,
            event_handler::delivery::DeliveryConfig,
        },
        task::NewTask,
    };
    use std::{
        sync::{Arc, Mutex},
        time::Duration,
    };
    use {
        crate::support::ledger::*, cqrsx::cqrs::store::AggregateContext, cqrsx::cqrs::store::EventStore,
        cqrsx::cqrs::testing::mem_store::MemStore, cqrsx::cqrs::testing::mem_store::MemStoreAggregateContext,
    };

    type Calls = Arc<Mutex<Vec<&'static str>>>;

    #[derive(Clone)]
    struct Store {
        inner: MemStore<Ledger>,
        calls: Calls,
        failure: Option<&'static str>,
    }
    impl EventStore<Ledger> for Store {
        type AC = MemStoreAggregateContext<Ledger>;
        async fn load_events(&self, id: &str) -> Result<Vec<EventEnvelope<Ledger>>, AggregateError<Rejected>> {
            self.inner.load_events(id).await
        }
        async fn load_aggregate(&self, id: &str) -> Result<Self::AC, AggregateError<Rejected>> {
            self.calls.lock().unwrap().push("load");
            if self.failure == Some("load") {
                return Err(AggregateError::DatabaseConnectionError("load failed".into()));
            }
            self.inner.load_aggregate(id).await
        }
        async fn commit(
            &self,
            events: Vec<Delta>,
            mut context: Self::AC,
            metadata: HashMap<String, String>,
        ) -> Result<Vec<EventEnvelope<Ledger>>, AggregateError<Rejected>> {
            self.calls.lock().unwrap().push("commit");
            // Commit receives the unchanged committed state.
            assert!(context.aggregate().history.is_empty());
            if self.failure == Some("commit") {
                return Err(AggregateError::AggregateConflict);
            }
            self.inner.commit(events, context, metadata).await
        }
    }
    struct Observer {
        calls: Calls,
        name: &'static str,
        seen: Arc<Mutex<Vec<EventEnvelope<Ledger>>>>,
    }
    #[async_trait]
    impl EventHandler<Ledger> for Observer {
        fn name(&self) -> String {
            self.name.to_owned()
        }

        async fn handle(&self, id: &str, events: &[EventEnvelope<Ledger>]) -> Result<(), cqrsx::cqrs::store::PersistenceError> {
            assert_eq!(id, "account");
            self.calls.lock().unwrap().push(self.name);
            self.seen.lock().unwrap().extend_from_slice(events);
            if self.name == "failed" {
                return Err(cqrsx::cqrs::store::PersistenceError::ConnectionError(
                    std::io::Error::other("original event handler cause").into(),
                ));
            }
            Ok(())
        }
    }

    /// Fails every call under its default name.
    struct Unnamed;
    #[async_trait]
    impl EventHandler<Ledger> for Unnamed {
        async fn handle(&self, _: &str, _: &[EventEnvelope<Ledger>]) -> Result<(), cqrsx::cqrs::store::PersistenceError> {
            Err(cqrsx::cqrs::store::PersistenceError::OptimisticLockError)
        }
    }

    /// An executor over `store` with one attempt per command and `handlers` in registration order.
    fn executor<S>(
        store: S,
        calls: &Calls,
        handlers: Vec<Arc<dyn EventHandler<Ledger>>>,
    ) -> CqrsCommandExecutor<Ledger, impl Fn(Vec<NewTask>) -> CqrsFramework<Ledger, S> + Send + Sync>
    where
        S: EventStore<Ledger> + Clone + Send + Sync,
    {
        let services = Services(calls.clone());
        let one_attempt = AggregateConflictRetryPolicy::new(1, Duration::ZERO, Duration::ZERO, 0.0).unwrap();
        let executor = CqrsCommandExecutor::new(
            move |_| CqrsFramework::new(store.clone(), services.clone()),
            one_attempt,
            LockLimits::default(),
        );
        handlers.into_iter().fold(executor, |executor, handler| {
            executor.with_event_handler(handler, DeliveryConfig::default())
        })
    }

    fn observers(
        calls: &Calls,
        seen: &Arc<Mutex<Vec<EventEnvelope<Ledger>>>>,
        names: &[&'static str],
    ) -> Vec<Arc<dyn EventHandler<Ledger>>> {
        names
            .iter()
            .map(|&name| {
                Arc::new(Observer {
                    calls: calls.clone(),
                    name,
                    seen: seen.clone(),
                }) as Arc<dyn EventHandler<Ledger>>
            })
            .collect()
    }

    #[tokio::test]
    async fn load_handle_commit_preserves_services_and_metadata_and_returns_the_events() {
        let calls = Calls::default();
        let store = Store {
            inner: MemStore::default(),
            calls: Arc::clone(&calls),
            failure: None,
        };
        let framework = CqrsFramework::new(store, Services(Arc::clone(&calls)));
        let metadata = HashMap::from([("trace".into(), "данные 🦀".into())]);
        let events = framework
            .execute_with_metadata("account", Command::Adjust(vec![5, -3]), metadata.clone())
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), ["load", "handle", "commit"]);
        assert_eq!(events.len(), 2);
        for (i, event) in events.iter().enumerate() {
            assert_eq!(event.aggregate_id, "account");
            assert_eq!(event.sequence, i + 1);
            assert_eq!(event.payload, Delta([5, -3][i]));
            assert_eq!(event.metadata, metadata);
        }
    }
    #[tokio::test]
    async fn empty_commands_and_all_failure_boundaries_do_not_dispatch() {
        for (failure, command, expected) in [
            (Some("load"), Command::Adjust(vec![5]), vec!["load"]),
            (Some("commit"), Command::Adjust(vec![5]), vec!["load", "handle", "commit"]),
            (None, Command::Adjust(vec![]), vec!["load", "handle", "commit"]),
            (None, Command::RejectAfterWrite, vec!["load", "handle"]),
            (None, Command::Adjust(vec![5, -6]), vec!["load", "handle"]),
        ] {
            let calls = Calls::default();
            let seen = Arc::new(Mutex::new(Vec::new()));
            let inner = MemStore::default();
            let store = Store {
                inner: inner.clone(),
                calls: calls.clone(),
                failure,
            };
            let executor = executor(store, &calls, observers(&calls, &seen, &["observer"]));
            let empty = matches!(&command, Command::Adjust(v) if v.is_empty());
            let result = executor.execute("account", command, vec![]).await;
            match failure {
                Some("load") => assert_eq!(result, Err(CommandGatewayError::Unavailable)),
                Some("commit") => assert_eq!(result, Err(CommandGatewayError::Conflict)),
                _ if empty => assert_eq!(result, Ok(())),
                _ => assert_eq!(result, Err(CommandGatewayError::Domain(Rejected))),
            }
            assert_eq!(*calls.lock().unwrap(), expected);
            assert!(seen.lock().unwrap().is_empty());
            assert!(executor.backlog().is_empty());
            assert!(inner.load_events("account").await.unwrap().is_empty());
        }
    }
    #[tokio::test]
    async fn committed_event_handler_failures_retain_indices_and_invoke_every_handler() {
        let store = MemStore::<Ledger>::default();
        store
            .commit(
                vec![Delta(4), Delta(5)],
                store.load_aggregate("account").await.unwrap(),
                HashMap::new(),
            )
            .await
            .unwrap();
        let calls = Calls::default();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut handlers = observers(&calls, &seen, &["failed", "second", "failed", "fourth"]);
        handlers.push(Arc::new(Unnamed));
        let executor = executor(store.clone(), &calls, handlers);
        // The workers log each handler's own error once; the gateway names the handlers.
        assert_eq!(
            executor.execute("account", Command::Adjust(vec![3, 2]), vec![]).await,
            Err(CommandGatewayError::CommittedWithEventHandlerFailure {
                last_sequence: 4,
                handlers: [(0, "failed"), (2, "failed"), (4, std::any::type_name::<Unnamed>())]
                    .map(|(index, name)| FailedHandler {
                        index,
                        name: name.to_owned(),
                    })
                    .to_vec(),
            })
        );
        // The handlers of one command run side by side, so only the command's own steps have an order.
        let mut calls = calls.lock().unwrap().clone();
        assert_eq!(calls.remove(0), "handle");
        calls.sort_unstable();
        assert_eq!(calls, ["failed", "failed", "fourth", "second"]);
        let sequences: Vec<usize> = seen.lock().unwrap().iter().map(|event| event.sequence).collect();
        assert_eq!(sequences.len(), 8);
        assert!(
            sequences.iter().all(|&sequence| sequence > 2),
            "only the command's own events"
        );
        assert_eq!(store.load_events("account").await.unwrap().len(), 4);
    }
}
