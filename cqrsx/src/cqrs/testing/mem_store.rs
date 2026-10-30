// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
// A test helper the engine does not use (rule 8 of the code guidelines): a poisoned lock fails the test that poisoned it.
#![allow(clippy::unwrap_used)]
use crate::{
    cqrs::{
        AggregateError,
        aggregate::Aggregate,
        event::EventEnvelope,
        store::{AggregateContext, EventStore},
    },
    task::NewTask,
};
use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, RwLock},
};

///  Simple memory store useful for application development and testing purposes.
///
/// Creation and use in a constructing a `CqrsFramework`:
/// ```
/// # mod example {
/// # use async_trait::async_trait;
/// # use serde::Deserialize;
/// # use serde::Serialize;
/// # use serde_json::Value;
/// # use cqrsx::cqrs::aggregate::Aggregate;
/// # use cqrsx::cqrs::event::DomainEvent;
/// # use cqrsx::cqrs::event::EventEnvelope;
/// # use cqrsx::cqrs::event_handler::EventHandler;
/// # use cqrsx::cqrs::projection::View;
/// # use cqrsx::cqrs::command::decide::Decide;
/// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
/// # use cqrsx::cqrs::store::PersistenceError;
/// # use cqrsx::cqrs::store::serialized_event::SerializedEvent;
/// # use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
/// # use cqrsx::cqrs::projection::repository::ViewContext;
/// # use cqrsx::cqrs::projection::repository::ViewRepository;
/// #
/// # #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
/// # pub enum MyEvents {
/// #     SomethingWasDone,
/// #     SomethingElseWasDone,
/// # }
/// #
/// # impl DomainEvent for MyEvents {
/// #     fn event_type(&self) -> String {
/// #         format!("{:?}", &self)
/// #     }
/// #     fn event_version(&self) -> String {
/// #         "0.1.0".to_string()
/// #     }
/// # }
/// #
/// # #[derive(Debug, Serialize, Deserialize)]
/// # pub enum MyCommands {
/// #     DoSomething,
/// #     BadCommand,
/// # }
/// #
/// # #[derive(Clone, Debug, Default, Serialize, Deserialize)]
/// # pub struct MyAggregate;
/// #
/// # impl Aggregate for MyAggregate {
/// #     const TYPE: &'static str = "MyAggregate";
/// #     type Id = String;
/// #     type Command = MyCommands;
/// #     type Event = MyEvents;
/// #     type Error = MyUserError;
/// #     type Rules = MyService;
/// #
/// #     fn apply(&mut self, _event: Self::Event) {}
/// # }
/// #
/// # impl Decide<MyAggregate> for MyCommands {
/// #     fn decide(self, _: &MyAggregate, _: &MyService) -> Result<MyEvents, MyUserError> {
/// #         match self {
/// #             MyCommands::DoSomething => Ok(MyEvents::SomethingWasDone),
/// #             MyCommands::BadCommand => Err("the expected error message".into()),
/// #         }
/// #     }
/// # }
/// #
/// # #[derive(Debug, PartialEq, Eq, thiserror::Error)]
/// # #[error("{0}")]
/// # pub struct MyUserError(pub String);
/// #
/// # impl From<&str> for MyUserError {
/// #     fn from(msg: &str) -> Self {
/// #         Self(msg.to_string())
/// #     }
/// # }
/// #
/// # #[derive(Debug, Clone, Default)]
/// # pub struct MyService;
/// # }
/// # use example::MyAggregate;
/// # use example::MyService;
/// use cqrsx::cqrs::CqrsFramework;
/// use cqrsx::cqrs::testing::mem_store::MemStore;
///
/// let store = MemStore::<MyAggregate>::default();
/// let cqrs = CqrsFramework::new(store, MyService);
/// ```
#[derive(Debug, Clone)]
pub struct MemStore<A: Aggregate + Send + Sync> {
    events: Arc<LockedEventEnvelopeMap<A>>,
}

impl<A: Aggregate> Default for MemStore<A> {
    fn default() -> Self {
        let events = Arc::default();
        Self { events }
    }
}

type LockedEventEnvelopeMap<A> = RwLock<HashMap<String, Vec<EventEnvelope<A>>>>;

impl<A: Aggregate> MemStore<A> {
    fn load_committed_events(&self, aggregate_id: &str) -> Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>> {
        let committed_events = self
            .events
            .read()
            .unwrap() // uninteresting unwrap: this will not be used in production, for tests only
            .get(aggregate_id)
            .into_iter()
            .flatten()
            .cloned()
            .collect();

        Ok(committed_events)
    }

    fn aggregate_id(events: &[EventEnvelope<A>]) -> String {
        // uninteresting unwrap: this is not a struct for production use
        let &first_event = events.iter().peekable().peek().unwrap();
        first_event.aggregate_id.to_string()
    }
}

impl<A: Aggregate> EventStore<A> for MemStore<A> {
    type Context = MemStoreAggregateContext<A>;

    async fn load_events(&self, aggregate_id: &str) -> Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>> {
        let events = self.load_committed_events(aggregate_id)?;
        Ok(events)
    }

    async fn load_aggregate(&self, aggregate_id: &str) -> Result<MemStoreAggregateContext<A>, AggregateError<A::Error>> {
        let committed_events = self.load_events(aggregate_id).await?;
        let mut aggregate = A::default();
        let mut current_sequence = 0;
        for envelope in committed_events {
            current_sequence = envelope.sequence;
            let event = envelope.payload;
            aggregate.apply(event);
        }
        Ok(MemStoreAggregateContext {
            aggregate_id: aggregate_id.to_string(),
            aggregate,
            current_sequence,
        })
    }

    /// Accepts tasks with events and keeps nothing of them (a `MemEventRepository` keeps an outbox); tasks
    /// without an event are refused, as by the PostgreSQL outbox.
    async fn commit(
        &self,
        events: Vec<A::Event>,
        context: MemStoreAggregateContext<A>,
        metadata: HashMap<String, String>,
        tasks: &[NewTask],
    ) -> Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>> {
        if events.is_empty() && !tasks.is_empty() {
            return Err(AggregateError::UnexpectedError(
                "outbox tasks require a committed domain event".into(),
            ));
        }
        let aggregate_id = context.aggregate_id;
        let current_sequence = context.current_sequence;
        crate::cqrs::store::checked_counter_add(current_sequence, events.len())?;
        let wrapped_events = Self::wrap_events(&aggregate_id, current_sequence, events, &metadata);
        let new_events_qty = wrapped_events.len();
        if new_events_qty == 0 {
            return Ok(Vec::default());
        }
        let aggregate_id = Self::aggregate_id(&wrapped_events);
        let mut new_events = self.load_committed_events(&aggregate_id).unwrap();
        for event in &wrapped_events {
            new_events.push(event.clone());
        }
        // uninteresting unwrap: this is not a struct for production use
        self.events.write().unwrap().insert(aggregate_id, new_events);
        Ok(wrapped_events)
    }
}

impl<A: Aggregate> MemStore<A> {
    /// Method to wrap a set of events with the additional metadata needed for persistence and publishing
    fn wrap_events(
        aggregate_id: &str,
        current_sequence: usize,
        resultant_events: Vec<A::Event>,
        base_metadata: &HashMap<String, String>,
    ) -> Vec<EventEnvelope<A>> {
        resultant_events
            .into_iter()
            .enumerate()
            .map(|(offset, payload)| {
                let sequence = current_sequence + offset + 1;
                EventEnvelope {
                    aggregate_id: aggregate_id.to_string(),
                    sequence,
                    payload,
                    metadata: base_metadata.clone(),
                }
            })
            .collect()
    }
}
/// Holds context for a pure event store implementation for MemStore.
///
/// This is used internally by the `CqrsFramework`.
pub struct MemStoreAggregateContext<A>
where
    A: Aggregate,
{
    /// The aggregate ID of the aggregate instance that has been loaded.
    pub aggregate_id: String,
    /// The current state of the aggregate instance.
    pub aggregate: A,
    /// The last committed event sequence number for this aggregate instance.
    pub current_sequence: usize,
}

// By hand: the aggregate need not be `Debug`.
impl<A: Aggregate> fmt::Debug for MemStoreAggregateContext<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemStoreAggregateContext")
            .field("aggregate_id", &self.aggregate_id)
            .field("current_sequence", &self.current_sequence)
            .finish_non_exhaustive()
    }
}

impl<A> AggregateContext<A> for MemStoreAggregateContext<A>
where
    A: Aggregate,
{
    fn aggregate(&self) -> &A {
        &self.aggregate
    }
}

#[cfg(test)]
mod tests {
    // Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
    // Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
    use crate::cqrs::{aggregate::Aggregate, command::decide::Decide, event::DomainEvent};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
    struct TestAggregate {
        id: String,
        description: String,
        tests: Vec<String>,
    }
    #[derive(Debug, thiserror::Error)]
    #[error("{0}")]
    struct TestError(String);

    impl From<&str> for TestError {
        fn from(msg: &str) -> Self {
            Self(msg.to_string())
        }
    }

    #[derive(Clone, Debug)]
    struct TestService;

    impl Aggregate for TestAggregate {
        const TYPE: &'static str = "TestAggregate";
        type Id = String;
        type Command = ();
        type Event = TestEvent;
        type Error = TestError;
        type Rules = TestService;

        fn apply(&mut self, event: Self::Event) {
            match event {
                TestEvent::Created(e) => {
                    self.id = e.id;
                }
                TestEvent::Tested(e) => {
                    self.tests.push(e.test_name);
                }
                TestEvent::SomethingElse(e) => {
                    self.description = e.description;
                }
            }
        }
    }

    impl Decide<TestAggregate> for () {
        fn decide(self, _: &TestAggregate, _: &TestService) -> Result<TestEvent, TestError> {
            Ok(TestEvent::Created(Created { id: String::new() }))
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
    enum TestEvent {
        Created(Created),
        Tested(Tested),
        SomethingElse(SomethingElse),
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
    struct Created {
        id: String,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
    struct Tested {
        test_name: String,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
    struct SomethingElse {
        description: String,
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> String {
            match self {
                Self::Created(_) => "Created".to_string(),
                Self::Tested(_) => "Tested".to_string(),
                Self::SomethingElse(_) => "SomethingElse".to_string(),
            }
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    fn metadata() -> HashMap<String, String> {
        let now = "2021-03-18T12:32:45.930Z".to_string();
        let mut metadata = HashMap::new();
        metadata.insert("time".to_string(), now);
        metadata
    }

    use super::*;

    #[tokio::test]
    async fn memory_sequence_overflow_is_atomic_and_last_sequence_is_supported() {
        use crate::cqrs::aggregate::tests::{Delta, Ledger};
        let store = MemStore::<Ledger>::default();
        for (sequence, count) in [(usize::MAX, 1), (usize::MAX - 1, 2)] {
            let context = MemStoreAggregateContext {
                aggregate_id: "overflow".into(),
                aggregate: Ledger::default(),
                current_sequence: sequence,
            };
            assert!(matches!(
                store.commit(vec![Delta(1); count], context, HashMap::new(), &[]).await,
                Err(AggregateError::UnexpectedError(_))
            ));
            assert!(store.load_events("overflow").await.unwrap().is_empty());
        }
        let context = MemStoreAggregateContext {
            aggregate_id: "last".into(),
            aggregate: Ledger::default(),
            current_sequence: usize::MAX - 1,
        };
        assert_eq!(
            store.commit(vec![Delta(1)], context, HashMap::new(), &[]).await.unwrap()[0].sequence,
            usize::MAX
        );
    }
    #[tokio::test]
    async fn test_mem_store() {
        let event_store = MemStore::<TestAggregate>::default();
        let id = "test_id_A";
        let initial_events = event_store.load_events(id).await.unwrap();
        assert_eq!(0, initial_events.len());
        let agg_context = event_store.load_aggregate(id).await.unwrap();
        assert_eq!(
            format!("{agg_context:?}"),
            r#"MemStoreAggregateContext { aggregate_id: "test_id_A", current_sequence: 0, .. }"#
        );

        event_store
            .commit(
                vec![TestEvent::Created(Created {
                    id: "test_event_A".to_string(),
                })],
                agg_context,
                metadata(),
                &[],
            )
            .await
            .unwrap();
        let stored_events = event_store.load_events(id).await.unwrap();
        assert_eq!(1, stored_events.len());
        let agg_context = event_store.load_aggregate(id).await.unwrap();

        event_store
            .commit(
                vec![
                    TestEvent::Tested(Tested {
                        test_name: "test A".to_string(),
                    }),
                    TestEvent::Tested(Tested {
                        test_name: "test B".to_string(),
                    }),
                    TestEvent::SomethingElse(SomethingElse {
                        description: "something else happening here".to_string(),
                    }),
                ],
                agg_context,
                metadata(),
                &[],
            )
            .await
            .unwrap();
        let stored_envelopes = event_store.load_events(id).await.unwrap();

        let mut agg = TestAggregate::default();
        for stored_envelope in stored_envelopes {
            let event = stored_envelope.payload;
            agg.apply(event);
        }
        assert_eq!(
            agg,
            TestAggregate {
                id: "test_event_A".to_string(),
                description: "something else happening here".to_string(),
                tests: vec!["test A".to_string(), "test B".to_string()],
            }
        );
    }
}

#[cfg(test)]
mod store_tests {
    use super::*;
    use crate::cqrs::aggregate::tests::*;
    #[tokio::test]
    async fn clones_observe_the_same_envelopes() {
        let store = MemStore::<Ledger>::default();
        let clone = store.clone();
        let metadata = HashMap::from([("trace".into(), "🦀".into())]);
        clone
            .commit(
                vec![Delta(5)],
                clone.load_aggregate("a").await.unwrap(),
                metadata.clone(),
                &[],
            )
            .await
            .unwrap();
        let events = store.load_events("a").await.unwrap();
        let [event] = events.as_slice() else {
            panic!("one committed event is visible through the other clone, got {}", events.len())
        };
        assert_eq!(event.aggregate_id, "a");
        assert_eq!(event.sequence, 1);
        assert_eq!(event.payload, Delta(5));
        assert_eq!(event.metadata, metadata);
        assert!(store.load_events("b").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn tasks_commit_only_with_an_event() {
        use crate::cqrs::aggregate::tests::{Delta, Ledger};
        let store = MemStore::<Ledger>::default();
        let task = NewTask::from_payload(
            crate::task::format::TaskFormat::try_new("notify", 1).unwrap(),
            serde_json::json!({}),
        )
        .unwrap();
        let context = || MemStoreAggregateContext {
            aggregate_id: "a".into(),
            aggregate: Ledger::default(),
            current_sequence: 0,
        };
        assert!(matches!(
            store.commit(vec![], context(), HashMap::new(), std::slice::from_ref(&task)).await,
            Err(AggregateError::UnexpectedError(error)) if error.to_string() == "outbox tasks require a committed domain event"
        ));
        assert_eq!(
            store
                .commit(vec![Delta(1)], context(), HashMap::new(), &[task])
                .await
                .unwrap()
                .len(),
            1
        );
    }
}
