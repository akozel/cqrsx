// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::{
    AggregateError,
    aggregate::Aggregate,
    event::EventEnvelope,
    store::{AggregateContext, EventStore},
};
use std::{
    collections::HashMap,
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
/// # use cqrsx::cqrs::event_sink::EventSink;
/// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
/// # use cqrsx::cqrs::store::PersistenceError;
/// # use cqrsx::cqrs::store::stream::ReplayStream;
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
/// #     type Command = MyCommands;
/// #     type Event = MyEvents;
/// #     type Error = MyUserError;
/// #     type Services = MyService;
/// #
/// #     async fn handle(
/// #         &mut self,
/// #         command: Self::Command,
/// #         _service: &Self::Services,
/// #         sink: &EventSink<Self>,
/// #     ) -> Result<(), Self::Error> {
/// #         match command {
/// #             MyCommands::DoSomething => {
/// #                 sink.write(MyEvents::SomethingWasDone, self).await;
/// #                 Ok(())
/// #             }
/// #             MyCommands::BadCommand => Err("the expected error message".into()),
/// #         }
/// #     }
/// #
/// #     fn apply(&mut self, _event: Self::Event) {}
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
    /// Get a shared copy of the events stored within the event store.
    ///
    /// This can be used to verify the state of events that have been committed.
    /// Example of reading and displaying stored events:
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
    /// # use cqrsx::cqrs::event_sink::EventSink;
    /// # use cqrsx::cqrs::store::repository::PersistedEventRepository;
    /// # use cqrsx::cqrs::store::PersistenceError;
    /// # use cqrsx::cqrs::store::stream::ReplayStream;
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
    /// #     type Command = MyCommands;
    /// #     type Event = MyEvents;
    /// #     type Error = MyUserError;
    /// #     type Services = MyService;
    /// #
    /// #     async fn handle(
    /// #         &mut self,
    /// #         command: Self::Command,
    /// #         _service: &Self::Services,
    /// #         sink: &EventSink<Self>,
    /// #     ) -> Result<(), Self::Error> {
    /// #         match command {
    /// #             MyCommands::DoSomething => {
    /// #                 sink.write(MyEvents::SomethingWasDone, self).await;
    /// #                 Ok(())
    /// #             }
    /// #             MyCommands::BadCommand => Err("the expected error message".into()),
    /// #         }
    /// #     }
    /// #
    /// #     fn apply(&mut self, _event: Self::Event) {}
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
    /// # use cqrsx::cqrs::event::EventEnvelope;
    /// # use cqrsx::cqrs::testing::mem_store::MemStore;
    /// let store = MemStore::<MyAggregate>::default();
    /// //...
    /// let all_locked_events = store.get_events();
    /// let unlocked_events = all_locked_events.read().unwrap();
    /// if let Some(events) = unlocked_events.get("test-aggregate-id-C450D1A") {
    ///     for event in events {
    ///         println!("{:?}", event);
    ///     }
    /// };
    /// ```
    #[deprecated(since = "0.4.9", note = "clone the MemStore instead")]
    pub fn get_events(&self) -> Arc<LockedEventEnvelopeMap<A>> {
        Arc::clone(&self.events)
    }

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
    type AC = MemStoreAggregateContext<A>;

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

    async fn commit(
        &self,
        events: Vec<A::Event>,
        context: MemStoreAggregateContext<A>,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>> {
        let aggregate_id = context.aggregate_id;
        let current_sequence = context.current_sequence;
        crate::cqrs::store::checked_counter_add(current_sequence, events.len())?;
        let wrapped_events = Self::wrap_events(&aggregate_id, current_sequence, events, metadata);
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
        base_metadata: HashMap<String, String>,
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

impl<A> AggregateContext<A> for MemStoreAggregateContext<A>
where
    A: Aggregate,
{
    fn aggregate(&mut self) -> &mut A {
        &mut self.aggregate
    }
}

#[cfg(test)]
mod tests {
    // Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
    // Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
    use crate::cqrs::{aggregate::Aggregate, event::DomainEvent, event_sink::EventSink};
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
        type Command = ();
        type Event = TestEvent;
        type Error = TestError;
        type Services = TestService;

        async fn handle(&mut self, _: Self::Command, _: &Self::Services, _: &EventSink<Self>) -> Result<(), Self::Error> {
            Ok(())
        }

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
        use crate::cqrs::event_sink::tests::{Delta, Ledger};
        let store = MemStore::<Ledger>::default();
        for (sequence, count) in [(usize::MAX, 1), (usize::MAX - 1, 2)] {
            let context = MemStoreAggregateContext {
                aggregate_id: "overflow".into(),
                aggregate: Ledger::default(),
                current_sequence: sequence,
            };
            assert!(matches!(
                store.commit(vec![Delta(1); count], context, HashMap::new()).await,
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
            store.commit(vec![Delta(1)], context, HashMap::new()).await.unwrap()[0].sequence,
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

        event_store
            .commit(
                vec![TestEvent::Created(Created {
                    id: "test_event_A".to_string(),
                })],
                agg_context,
                metadata(),
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
mod legacy_api_tests {
    use super::*;
    use crate::cqrs::event_sink::tests::*;
    #[tokio::test]
    #[allow(deprecated)]
    async fn legacy_event_handle_and_clones_observe_the_same_envelopes() {
        let store = MemStore::<Ledger>::default();
        let clone = store.clone();
        let first = store.get_events();
        let second = clone.get_events();
        assert!(Arc::ptr_eq(&first, &second));
        let metadata = HashMap::from([("trace".into(), "🦀".into())]);
        clone
            .commit(vec![Delta(5)], clone.load_aggregate("a").await.unwrap(), metadata.clone())
            .await
            .unwrap();
        let events = first.read().unwrap();
        let event = &events["a"][0];
        assert_eq!(event.aggregate_id, "a");
        assert_eq!(event.sequence, 1);
        assert_eq!(event.payload, Delta(5));
        assert_eq!(event.metadata, metadata);
        assert!(!events.contains_key("b"));
    }
}
