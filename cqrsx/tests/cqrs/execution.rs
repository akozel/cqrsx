// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use cqrsx::cqrs::AggregateError;
use cqrsx::cqrs::CqrsFramework;
use cqrsx::cqrs::event::EventEnvelope;
use cqrsx::cqrs::event_handler::EventHandler;
use cqrsx::cqrs::store::EventStore;
use cqrsx::cqrs::testing::mem_store::MemStore;

use crate::support::scenario::*;

struct CapturingEventHandler {
    events: Arc<RwLock<Vec<EventEnvelope<TestAggregate>>>>,
}

impl CapturingEventHandler {
    fn new(events: Arc<RwLock<Vec<EventEnvelope<TestAggregate>>>>) -> Self {
        Self { events }
    }
}
#[async_trait]
impl EventHandler<TestAggregate> for CapturingEventHandler {
    async fn handle(
        &self,
        _aggregate_id: &str,
        events: &[EventEnvelope<TestAggregate>],
    ) -> Result<(), cqrsx::cqrs::store::PersistenceError> {
        for event in events {
            let mut event_list = self.events.write().unwrap();
            event_list.push(event.clone());
        }
        Ok(())
    }
}

#[tokio::test]
async fn framework_test() {
    let event_store = MemStore::<TestAggregate>::default();

    let delivered_events = Arc::default();
    let view = CapturingEventHandler::new(Arc::clone(&delivered_events));

    let cqrs = CqrsFramework::new(event_store.clone(), vec![Box::new(view)], TestService);
    let uuid = uuid::Uuid::new_v4().to_string();
    let id = uuid.clone();
    let metadata = metadata();
    cqrs.execute_with_metadata(
        &id,
        TestCommand::ConfirmTest(ConfirmTest { test_name: uuid.clone() }),
        metadata,
    )
    .await
    .unwrap_or_default();

    assert_eq!(1, event_store.load_events(&id).await.unwrap().len());
    assert_eq!(1, delivered_events.read().unwrap().len());

    let test = "TEST_A";
    let id = uuid.clone();
    cqrs.execute(
        &id,
        TestCommand::ConfirmTest(ConfirmTest {
            test_name: test.to_string(),
        }),
    )
    .await
    .unwrap_or_default();

    assert_eq!(2, delivered_events.read().unwrap().len());
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

    assert_eq!(2, delivered_events.read().unwrap().len());
    let stored_event_count = event_store.load_events(uuid.clone().as_str()).await.unwrap().len();
    assert_eq!(2, stored_event_count);
}

mod contracts {
    use super::*;
    use async_trait::async_trait;
    use cqrsx::cqrs::event::EventEnvelope;
    use std::sync::{Arc, Mutex};
    use {
        crate::support::ledger::*, cqrsx::cqrs::store::AggregateContext, cqrsx::cqrs::store::EventStore,
        cqrsx::cqrs::testing::mem_store::MemStore, cqrsx::cqrs::testing::mem_store::MemStoreAggregateContext,
    };

    struct Store {
        inner: MemStore<Ledger>,
        calls: Arc<Mutex<Vec<&'static str>>>,
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
        calls: Arc<Mutex<Vec<&'static str>>>,
        name: &'static str,
        seen: Arc<Mutex<Vec<EventEnvelope<Ledger>>>>,
    }
    #[async_trait]
    impl EventHandler<Ledger> for Observer {
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
    #[tokio::test]
    async fn load_handle_commit_notify_preserves_services_metadata_and_appended_handlers() {
        let calls = Arc::default();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let store = Store {
            inner: MemStore::default(),
            calls: Arc::clone(&calls),
            failure: None,
        };
        let framework = CqrsFramework::new(
            store,
            vec![Box::new(Observer {
                calls: Arc::clone(&calls),
                name: "first",
                seen: seen.clone(),
            })],
            Services(Arc::clone(&calls)),
        )
        .append_event_handler(Box::new(Observer {
            calls: Arc::clone(&calls),
            name: "second",
            seen: seen.clone(),
        }));
        let metadata = HashMap::from([("trace".into(), "данные 🦀".into())]);
        framework
            .execute_with_metadata("account", Command::Adjust(vec![5, -3]), metadata.clone())
            .await
            .unwrap();
        assert_eq!(*calls.lock().unwrap(), ["load", "handle", "commit", "first", "second"]);
        let events = seen.lock().unwrap();
        assert_eq!(events.len(), 4);
        for chunk in events.chunks(2) {
            for (i, event) in chunk.iter().enumerate() {
                assert_eq!(event.aggregate_id, "account");
                assert_eq!(event.sequence, i + 1);
                assert_eq!(event.payload, Delta([5, -3][i]));
                assert_eq!(event.metadata, metadata);
            }
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
            let calls = Arc::new(Mutex::new(Vec::new()));
            let seen = Arc::new(Mutex::new(Vec::new()));
            let inner = MemStore::default();
            let framework = CqrsFramework::new(
                Store {
                    inner: inner.clone(),
                    calls: calls.clone(),
                    failure,
                },
                vec![Box::new(Observer {
                    calls: calls.clone(),
                    name: "handle",
                    seen: seen.clone(),
                })],
                Services(calls.clone()),
            );
            let empty = matches!(&command, Command::Adjust(v) if v.is_empty());
            let result = framework.execute("account", command).await;
            match failure {
                Some("load") => assert!(matches!(result, Err(AggregateError::DatabaseConnectionError(_)))),
                Some("commit") => assert!(matches!(result, Err(AggregateError::AggregateConflict))),
                _ if empty => result.unwrap(),
                _ => assert!(matches!(result, Err(AggregateError::Domain(Rejected)))),
            }
            assert_eq!(*calls.lock().unwrap(), expected);
            assert!(seen.lock().unwrap().is_empty());
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
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let event_handlers: Vec<Box<dyn EventHandler<Ledger>>> = ["failed", "second", "failed", "fourth"]
            .into_iter()
            .map(|name| {
                Box::new(Observer {
                    calls: calls.clone(),
                    name,
                    seen: seen.clone(),
                }) as Box<dyn EventHandler<Ledger>>
            })
            .collect();
        let framework = CqrsFramework::new(store.clone(), event_handlers, Services(calls.clone()));
        let Err(AggregateError::CommittedWithEventHandlerFailure(failure)) =
            framework.execute("account", Command::Adjust(vec![3, 2])).await
        else {
            panic!("commit outcome must be explicit")
        };
        assert_eq!(failure.last_sequence, 4);
        assert_eq!(
            failure.to_string(),
            "events committed through sequence 4; event handling failed"
        );
        assert!(!failure.to_string().contains("original event handler cause"));
        assert_eq!(failure.errors.iter().map(|e| e.handler_index).collect::<Vec<_>>(), [0, 2]);
        for error in failure.errors {
            assert!(
                matches!(error.error,cqrsx::cqrs::store::PersistenceError::ConnectionError(ref source) if source.downcast_ref::<std::io::Error>().unwrap().to_string()=="original event handler cause")
            );
        }
        assert_eq!(*calls.lock().unwrap(), ["handle", "failed", "second", "failed", "fourth"]);
        assert_eq!(seen.lock().unwrap().len(), 8);
        assert_eq!(store.load_events("account").await.unwrap().len(), 4);
    }
}
