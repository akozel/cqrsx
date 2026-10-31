// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::{
    command::AggregateError,
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::{
        AggregateContext, EventStore, PersistenceError,
        repository::PersistedEventRepository,
        serialized_event::{SerializedEvent, deserialize_events, serialize_events},
        snapshot::{SerializedSnapshot, SnapshotUpdate},
        upcaster::EventUpcaster,
    },
    task::NewTask,
};
use std::{collections::HashMap, fmt, marker::PhantomData, num::NonZeroUsize, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq)]
enum SnapshotPolicy {
    Disabled,
    /// A snapshot every this many events: never zero, so a commit never divides by it.
    Every(NonZeroUsize),
}

impl SnapshotPolicy {
    fn snapshot_prefix_len(&self, current_sequence: usize, num_events: usize) -> usize {
        match self {
            Self::Disabled => 0,
            Self::Every(snapshot_interval) => {
                let snapshot_interval = snapshot_interval.get();
                let next_snapshot_at = snapshot_interval - (current_sequence % snapshot_interval);
                if num_events < next_snapshot_at {
                    0
                } else {
                    let addl_events_after_next_snapshot = num_events - next_snapshot_at;
                    let addl_events_after_next_snapshot_to_apply =
                        addl_events_after_next_snapshot - (addl_events_after_next_snapshot % snapshot_interval);
                    next_snapshot_at + addl_events_after_next_snapshot_to_apply
                }
            }
        }
    }
}

/// Storage engine using a database backing.
///
/// This defaults to an event-sourced store (i.e., events are the single source of truth),
/// and can use snapshots when a large number of events belong to one aggregate instance. A clone shares the
/// upcasters and clones the repository.
#[must_use]
pub struct PersistedEventStore<R, A>
where
    R: PersistedEventRepository,
    A: Aggregate + Send + Sync,
{
    repo: R,
    snapshot_policy: SnapshotPolicy,
    event_upcasters: Arc<[Box<dyn EventUpcaster>]>,
    _phantom: PhantomData<A>,
}

// By hand: the repository need not be `Debug`, and the upcasters are trait objects.
impl<R, A> fmt::Debug for PersistedEventStore<R, A>
where
    R: PersistedEventRepository,
    A: Aggregate + Send + Sync,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PersistedEventStore")
            .field("aggregate_type", &A::TYPE)
            .field("snapshot_policy", &self.snapshot_policy)
            .field("upcasters", &self.event_upcasters.len())
            .finish_non_exhaustive()
    }
}

impl<R, A> Clone for PersistedEventStore<R, A>
where
    R: PersistedEventRepository + Clone,
    A: Aggregate + Send + Sync,
{
    fn clone(&self) -> Self {
        Self {
            repo: self.repo.clone(),
            snapshot_policy: self.snapshot_policy,
            event_upcasters: self.event_upcasters.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<R, A> PersistedEventStore<R, A>
where
    R: PersistedEventRepository,
    A: Aggregate + Send + Sync,
{
    /// Creates a new `PersistedEventStore` from the provided event repository,
    /// using events as the single source of truth until [`with_snapshots`](Self::with_snapshots) adds snapshots.
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use cqrsx::journal::snapshot::SnapshotUpdate;
    /// # use cqrsx::domain::aggregate::Aggregate;
    /// # use cqrsx::domain::event::DomainEvent;
    /// # use cqrsx::domain::event::EventEnvelope;
    /// # use cqrsx::cqrs::event_handler::EventHandler;
    /// # use cqrsx::cqrs::projection::View;
    /// # use cqrsx::domain::command::Decide;
    /// # use cqrsx::journal::repository::PersistedEventRepository;
    /// # use cqrsx::journal::PersistenceError;
    /// # use cqrsx::journal::serialized_event::SerializedEvent;
    /// # use cqrsx::journal::snapshot::SerializedSnapshot;
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
    /// #     fn event_type(&self) -> &'static str {
    /// #         match self {
    /// #             Self::SomethingWasDone => "SomethingWasDone",
    /// #             Self::SomethingElseWasDone => "SomethingElseWasDone",
    /// #         }
    /// #     }
    /// #     fn event_version(&self) -> &'static str {
    /// #         "0.1.0"
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
    /// #
    /// # pub struct MyDatabaseConnection;
    /// #
    /// # pub struct MyEventRepository;
    /// #
    /// # impl MyEventRepository {
    /// #     pub fn new(_db: MyDatabaseConnection) -> Self {
    /// #         Self
    /// #     }
    /// # }
    /// #
    /// # impl PersistedEventRepository for MyEventRepository {
    /// #     async fn events<A: Aggregate>(
    /// #         &self,
    /// #         _aggregate_id: &str,
    /// #     ) -> Result<Vec<SerializedEvent>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn events_after<A: Aggregate>(
    /// #         &self,
    /// #         _aggregate_id: &str,
    /// #         _after_sequence: usize,
    /// #     ) -> Result<Vec<SerializedEvent>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn snapshot<A: Aggregate>(
    /// #         &self,
    /// #         _aggregate_id: &str,
    /// #     ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn persist<A: Aggregate>(
    /// #         &self,
    /// #         _events: &[SerializedEvent],
    /// #         _snapshot_update: Option<cqrsx::journal::snapshot::SnapshotUpdate>,
    /// #     ) -> Result<(), PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn feed_after<A: Aggregate>(
    /// #         &self,
    /// #         _after: cqrsx::journal::feed::Position,
    /// #         _limit: usize,
    /// #     ) -> Result<Vec<cqrsx::journal::feed::FeedEntry>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn head<A: Aggregate>(&self) -> Result<cqrsx::journal::feed::Position, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn aggregate_ids_after<A: Aggregate>(
    /// #         &self,
    /// #         _after: &str,
    /// #         _limit: usize,
    /// #     ) -> Result<Vec<String>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// # }
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyService;
    /// # use example::MyDatabaseConnection;
    /// # use example::MyEventRepository;
    /// # use cqrsx::command::framework::CqrsFramework;
    /// # use cqrsx::journal::persisted::PersistedEventStore;
    /// # fn config(my_db_connection: MyDatabaseConnection) {
    /// let repo = MyEventRepository::new(my_db_connection);
    /// let store = PersistedEventStore::<MyEventRepository,MyAggregate>::new(repo);
    /// let service = MyService;
    /// let cqrs = CqrsFramework::new(store, service);
    /// # }
    /// ```
    pub fn new(repo: R) -> Self {
        Self {
            repo,
            snapshot_policy: SnapshotPolicy::Disabled,
            event_upcasters: Arc::new([]),
            _phantom: PhantomData,
        }
    }

    /// Keeps a snapshot of the aggregate every `snapshot_interval` events, which is never zero: a load reads the
    /// snapshot and the events after it instead of the whole history.
    ///
    /// ```
    /// # mod example {
    /// # use async_trait::async_trait;
    /// # use serde::Deserialize;
    /// # use serde::Serialize;
    /// # use cqrsx::journal::snapshot::SnapshotUpdate;
    /// # use cqrsx::domain::aggregate::Aggregate;
    /// # use cqrsx::domain::event::DomainEvent;
    /// # use cqrsx::domain::event::EventEnvelope;
    /// # use cqrsx::cqrs::event_handler::EventHandler;
    /// # use cqrsx::cqrs::projection::View;
    /// # use cqrsx::domain::command::Decide;
    /// # use cqrsx::journal::repository::PersistedEventRepository;
    /// # use cqrsx::journal::PersistenceError;
    /// # use cqrsx::journal::serialized_event::SerializedEvent;
    /// # use cqrsx::journal::snapshot::SerializedSnapshot;
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
    /// #     fn event_type(&self) -> &'static str {
    /// #         match self {
    /// #             Self::SomethingWasDone => "SomethingWasDone",
    /// #             Self::SomethingElseWasDone => "SomethingElseWasDone",
    /// #         }
    /// #     }
    /// #     fn event_version(&self) -> &'static str {
    /// #         "0.1.0"
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
    /// #
    /// # pub struct MyDatabaseConnection;
    /// #
    /// # pub struct MyEventRepository;
    /// #
    /// # impl MyEventRepository {
    /// #     pub fn new(_db: MyDatabaseConnection) -> Self {
    /// #         Self
    /// #     }
    /// # }
    /// #
    /// # impl PersistedEventRepository for MyEventRepository {
    /// #     async fn events<A: Aggregate>(
    /// #         &self,
    /// #         _aggregate_id: &str,
    /// #     ) -> Result<Vec<SerializedEvent>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn events_after<A: Aggregate>(
    /// #         &self,
    /// #         _aggregate_id: &str,
    /// #         _after_sequence: usize,
    /// #     ) -> Result<Vec<SerializedEvent>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn snapshot<A: Aggregate>(
    /// #         &self,
    /// #         _aggregate_id: &str,
    /// #     ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn persist<A: Aggregate>(
    /// #         &self,
    /// #         _events: &[SerializedEvent],
    /// #         _snapshot_update: Option<cqrsx::journal::snapshot::SnapshotUpdate>,
    /// #     ) -> Result<(), PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn feed_after<A: Aggregate>(
    /// #         &self,
    /// #         _after: cqrsx::journal::feed::Position,
    /// #         _limit: usize,
    /// #     ) -> Result<Vec<cqrsx::journal::feed::FeedEntry>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn head<A: Aggregate>(&self) -> Result<cqrsx::journal::feed::Position, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn aggregate_ids_after<A: Aggregate>(
    /// #         &self,
    /// #         _after: &str,
    /// #         _limit: usize,
    /// #     ) -> Result<Vec<String>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// # }
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyService;
    /// # use example::MyDatabaseConnection;
    /// # use example::MyEventRepository;
    /// # use cqrsx::command::framework::CqrsFramework;
    /// # use cqrsx::journal::persisted::PersistedEventStore;
    /// # use std::num::NonZeroUsize;
    /// # fn config(my_db_connection: MyDatabaseConnection) {
    /// const SNAPSHOT_EVERY: NonZeroUsize = NonZeroUsize::new(100).unwrap();
    /// let repo = MyEventRepository::new(my_db_connection);
    /// let store = PersistedEventStore::<MyEventRepository,MyAggregate>::new(repo).with_snapshots(SNAPSHOT_EVERY);
    /// let cqrs = CqrsFramework::new(store, MyService);
    /// # }
    /// ```
    pub fn with_snapshots(mut self, snapshot_interval: NonZeroUsize) -> Self {
        self.snapshot_policy = SnapshotPolicy::Every(snapshot_interval);
        self
    }

    /// Configures the event store to use event upcasters when loading events.
    /// The EventUpcasters within the Vec should be placed in the
    /// order that they should be applied
    ///
    /// E.g., an upcaster for version 0.2.3 should be placed before an upcaster for version 0.2.4
    pub fn with_upcasters(self, event_upcasters: Vec<Box<dyn EventUpcaster>>) -> Self {
        Self {
            repo: self.repo,
            snapshot_policy: self.snapshot_policy,
            event_upcasters: event_upcasters.into(),
            _phantom: PhantomData,
        }
    }

    /// The interval of the snapshots, `None` without snapshots.
    #[cfg(test)]
    pub(crate) fn snapshot_interval(&self) -> Option<usize> {
        match self.snapshot_policy {
            SnapshotPolicy::Disabled => None,
            SnapshotPolicy::Every(interval) => Some(interval.get()),
        }
    }
}

impl<R, A> EventStore<A> for PersistedEventStore<R, A>
where
    R: PersistedEventRepository,
    A: Aggregate + Send + Sync,
{
    type Context = EventStoreAggregateContext<A>;

    async fn load_events(&self, aggregate_id: &str) -> Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>> {
        let serialized_events = self.repo.events::<A>(aggregate_id).await?;
        Ok(deserialize_events(serialized_events, &self.event_upcasters)?)
    }

    async fn load_aggregate(&self, aggregate_id: &str) -> Result<EventStoreAggregateContext<A>, AggregateError<A::Error>> {
        let mut context: EventStoreAggregateContext<A> = if self.snapshot_policy == SnapshotPolicy::Disabled {
            EventStoreAggregateContext::empty(aggregate_id)
        } else {
            let snapshot = self.repo.snapshot::<A>(aggregate_id).await?;
            match snapshot {
                Some(snapshot) => {
                    let version = snapshot.snapshot_version;
                    match snapshot.try_into() {
                        Err(PersistenceError::DeserializationError(_)) => {
                            // If aggregate structure changed, this will trigger replaying all the events
                            // and rebuilding fresh aggregate state.
                            let mut context = EventStoreAggregateContext::empty(aggregate_id);
                            context.snapshot_version = Some(version);
                            context
                        }
                        r => r?,
                    }
                }
                None => EventStoreAggregateContext::empty(aggregate_id),
            }
        };
        let events_to_apply = match self.snapshot_policy {
            SnapshotPolicy::Disabled => self.repo.events::<A>(aggregate_id).await?,
            SnapshotPolicy::Every(_) => self.repo.events_after::<A>(aggregate_id, context.current_sequence).await?,
        };
        // Decoded and applied one by one, never collected: an event that does not decode fails the load, and the
        // state the events before it built is dropped with the context.
        for event in events_to_apply {
            let envelope: EventEnvelope<A> = event.decode(&self.event_upcasters)?;
            context.current_sequence = envelope.sequence;
            context.aggregate.apply(envelope.payload);
        }
        Ok(context)
    }

    async fn commit(
        &self,
        events: Vec<A::Event>,
        context: EventStoreAggregateContext<A>,
        metadata: HashMap<String, String>,
        tasks: &[NewTask],
    ) -> Result<Vec<EventEnvelope<A>>, AggregateError<A::Error>> {
        let aggregate_id = context.aggregate_id.clone();
        let last_sequence = context.current_sequence;
        crate::journal::checked_counter_add(last_sequence, events.len())?;

        let snapshot_prefix_len = self
            .snapshot_policy
            .snapshot_prefix_len(context.current_sequence, events.len());
        let snapshot_update = if snapshot_prefix_len == 0 {
            None
        } else {
            let next_version = crate::journal::checked_counter_add(context.snapshot_version.unwrap_or(0), 1)?;
            Some(Self::update_snapshot_with_events(
                &events,
                context,
                snapshot_prefix_len,
                next_version,
            )?)
        };
        let wrapped_events = Self::wrap_events(&aggregate_id, last_sequence, events, &metadata);
        let serialized_events: Vec<SerializedEvent> = serialize_events(&wrapped_events)?;
        self.repo
            .persist_with_tasks::<A>(&serialized_events, snapshot_update, tasks)
            .await?;
        Ok(wrapped_events)
    }
}

impl<R, A> PersistedEventStore<R, A>
where
    A: Aggregate + Send + Sync,
    R: PersistedEventRepository,
{
    fn update_snapshot_with_events(
        events: &[A::Event],
        mut context: EventStoreAggregateContext<A>,
        prefix: usize,
        next_version: usize,
    ) -> Result<SnapshotUpdate, AggregateError<A::Error>> {
        for event in events.iter().take(prefix).cloned() {
            context.aggregate.apply(event);
        }
        Ok(SnapshotUpdate {
            aggregate_id: context.aggregate_id,
            payload: serde_json::to_value(context.aggregate)?,
            last_sequence: context.current_sequence + prefix,
            next_version,
        })
    }

    /// Method to wrap a set of events with the additional metadata needed for persistence and publishing
    fn wrap_events(
        aggregate_id: &str,
        last_sequence: usize,
        resultant_events: Vec<A::Event>,
        base_metadata: &HashMap<String, String>,
    ) -> Vec<EventEnvelope<A>> {
        resultant_events
            .into_iter()
            .enumerate()
            .map(|(offset, payload)| EventEnvelope {
                aggregate_id: aggregate_id.to_string(),
                sequence: last_sequence + offset + 1,
                payload,
                metadata: base_metadata.clone(),
            })
            .collect()
    }
}

/// Last committed aggregate state and optimistic metadata for `PersistedEventStore`.
pub struct EventStoreAggregateContext<A: Aggregate> {
    /// The aggregate ID of the aggregate instance that has been loaded.
    pub aggregate_id: String,
    /// The current state of the aggregate instance.
    pub aggregate: A,
    /// The last committed event sequence number for this aggregate instance.
    pub current_sequence: usize,
    /// The last committed snapshot version for this aggregate instance.
    pub snapshot_version: Option<usize>,
}

// By hand: the aggregate need not be `Debug`.
impl<A: Aggregate> fmt::Debug for EventStoreAggregateContext<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventStoreAggregateContext")
            .field("aggregate_id", &self.aggregate_id)
            .field("current_sequence", &self.current_sequence)
            .field("snapshot_version", &self.snapshot_version)
            .finish_non_exhaustive()
    }
}

impl<A: Aggregate> EventStoreAggregateContext<A> {
    pub(crate) fn empty(aggregate_id: &str) -> Self {
        Self {
            aggregate_id: aggregate_id.to_string(),
            aggregate: A::default(),
            current_sequence: 0,
            snapshot_version: None,
        }
    }
}

impl<A: Aggregate> AggregateContext<A> for EventStoreAggregateContext<A> {
    fn aggregate(&self) -> &A {
        &self.aggregate
    }
}

impl<A: Aggregate> TryFrom<SerializedSnapshot> for EventStoreAggregateContext<A> {
    type Error = PersistenceError;

    fn try_from(snapshot: SerializedSnapshot) -> Result<Self, Self::Error> {
        let aggregate = serde_json::from_value(snapshot.aggregate)?;
        Ok(Self {
            aggregate_id: snapshot.aggregate_id,
            aggregate,
            current_sequence: snapshot.last_sequence,
            snapshot_version: Some(snapshot.snapshot_version),
        })
    }
}

#[cfg(test)]
pub(crate) mod tests {
    // Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
    // Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
    use crate::{
        domain::{aggregate::Aggregate, command::Decide, event::DomainEvent},
        journal::{
            PersistenceError, repository::PersistedEventRepository, serialized_event::SerializedEvent,
            snapshot::SerializedSnapshot,
        },
    };
    use serde::{Deserialize, Serialize};
    use std::{collections::HashMap, num::NonZeroUsize, sync::Mutex};

    /// A snapshot interval of `events` events, which is positive in every test.
    pub(crate) fn every(events: usize) -> NonZeroUsize {
        NonZeroUsize::new(events).expect("a snapshot interval of a test is positive")
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
    pub(crate) enum TestEvents {
        Started,
        SomethingWasDone,
    }

    impl DomainEvent for TestEvents {
        fn event_type(&self) -> &'static str {
            match self {
                Self::Started => "Started",
                Self::SomethingWasDone => "SomethingWasDone",
            }
        }
        fn event_version(&self) -> &'static str {
            EVENT_VERSION
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) enum TestCommands {
        DoSomething,
        BadCommand,
    }

    #[derive(Debug, thiserror::Error)]
    #[error("{0}")]
    pub(crate) struct TestError(String);

    impl From<&str> for TestError {
        fn from(msg: &str) -> Self {
            Self(msg.to_string())
        }
    }

    #[derive(Clone)]
    pub(crate) struct TestService;

    #[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
    pub(crate) struct TestAggregate {
        pub(crate) something_happened: usize,
    }

    impl Aggregate for TestAggregate {
        const TYPE: &'static str = "TestAggregate";
        type Id = String;
        type Command = TestCommands;
        type Event = TestEvents;
        type Error = TestError;
        type Rules = TestService;

        fn apply(&mut self, event: Self::Event) {
            match event {
                TestEvents::Started => {}
                TestEvents::SomethingWasDone => {
                    self.something_happened += 1;
                }
            }
        }
    }

    impl Decide<TestAggregate> for TestCommands {
        fn decide(self, _: &TestAggregate, _: &TestService) -> Result<TestEvents, TestError> {
            match self {
                TestCommands::DoSomething => Ok(TestEvents::SomethingWasDone),
                TestCommands::BadCommand => Err("the expected error message".into()),
            }
        }
    }

    pub(crate) struct MockRepo {
        persist_error: Mutex<Option<PersistenceError>>,
        events_result: Mutex<Option<Result<Vec<SerializedEvent>, PersistenceError>>>,
        last_events_result: Mutex<Option<Result<Vec<SerializedEvent>, PersistenceError>>>,
        snapshot_result: Mutex<Option<Result<Option<SerializedSnapshot>, PersistenceError>>>,
        #[allow(clippy::type_complexity)]
        persist_check:
            Mutex<Option<Box<dyn FnOnce(&[SerializedEvent], Option<crate::journal::snapshot::SnapshotUpdate>) + Send>>>,
    }

    impl MockRepo {
        pub(crate) fn with_commit_error(error: PersistenceError) -> Self {
            let repo = Self::with_commit(Box::new(|_, _| panic!("failed persist must not succeed")));
            *repo.persist_error.lock().unwrap() = Some(error);
            repo
        }
        pub(crate) fn with_events(result: Result<Vec<SerializedEvent>, PersistenceError>) -> Self {
            Self {
                events_result: Mutex::new(Some(result)),
                last_events_result: Mutex::new(None),
                snapshot_result: Mutex::new(None),
                persist_error: Mutex::new(None),
                persist_check: Mutex::new(None),
            }
        }
        pub(crate) fn with_last_events(
            last_events: Result<Vec<SerializedEvent>, PersistenceError>,
            snapshot: Result<Option<SerializedSnapshot>, PersistenceError>,
        ) -> Self {
            Self {
                events_result: Mutex::new(None),
                last_events_result: Mutex::new(Some(last_events)),
                snapshot_result: Mutex::new(Some(snapshot)),
                persist_error: Mutex::new(None),
                persist_check: Mutex::new(None),
            }
        }
        pub(crate) fn with_snapshot(result: Result<Option<SerializedSnapshot>, PersistenceError>) -> Self {
            Self {
                events_result: Mutex::new(None),
                last_events_result: Mutex::new(None),
                snapshot_result: Mutex::new(Some(result)),
                persist_error: Mutex::new(None),
                persist_check: Mutex::new(None),
            }
        }
        #[allow(clippy::type_complexity)]
        pub(crate) fn with_commit(
            test_function: Box<dyn FnOnce(&[SerializedEvent], Option<crate::journal::snapshot::SnapshotUpdate>) + Send>,
        ) -> Self {
            Self {
                events_result: Mutex::new(None),
                last_events_result: Mutex::new(None),
                snapshot_result: Mutex::new(None),
                persist_error: Mutex::new(None),
                persist_check: Mutex::new(Some(test_function)),
            }
        }
    }

    impl PersistedEventRepository for MockRepo {
        async fn events<A: Aggregate>(&self, _aggregate_id: &str) -> Result<Vec<SerializedEvent>, PersistenceError> {
            self.events_result.lock().unwrap().take().unwrap()
        }
        async fn events_after<A: Aggregate>(
            &self,
            _aggregate_id: &str,
            _after_sequence: usize,
        ) -> Result<Vec<SerializedEvent>, PersistenceError> {
            self.last_events_result.lock().unwrap().take().unwrap()
        }
        async fn snapshot<A: Aggregate>(&self, _aggregate_id: &str) -> Result<Option<SerializedSnapshot>, PersistenceError> {
            self.snapshot_result.lock().unwrap().take().unwrap()
        }
        async fn persist<A: Aggregate>(
            &self,
            events: &[SerializedEvent],
            snapshot_update: Option<crate::journal::snapshot::SnapshotUpdate>,
        ) -> Result<(), PersistenceError> {
            if let Some(error) = self.persist_error.lock().unwrap().take() {
                return Err(error);
            }
            let test = self.persist_check.lock().unwrap().take().unwrap();
            test(events, snapshot_update);
            Ok(())
        }

        async fn feed_after<A: Aggregate>(
            &self,
            _: crate::journal::feed::Position,
            _: usize,
        ) -> Result<Vec<crate::journal::feed::FeedEntry>, PersistenceError> {
            panic!("not called by these tests")
        }

        async fn head<A: Aggregate>(&self) -> Result<crate::journal::feed::Position, PersistenceError> {
            panic!("not called by these tests")
        }

        async fn aggregate_ids_after<A: Aggregate>(&self, _: &str, _: usize) -> Result<Vec<String>, PersistenceError> {
            panic!("not called by these tests")
        }
    }

    pub(crate) const TEST_AGGREGATE_ID: &str = "test-aggregate-C";
    pub(crate) const EVENT_VERSION: &str = "1.0";

    pub(crate) fn test_serialized_event(seq: usize, event: TestEvents) -> SerializedEvent {
        let event_type = event.event_type().to_owned();
        let event_version = event.event_version().to_owned();
        let payload = serde_json::to_value(event).unwrap();
        SerializedEvent {
            aggregate_id: TEST_AGGREGATE_ID.to_string(),
            sequence: seq,
            aggregate_type: "TestAggregate".to_string(),
            event_type,
            event_version,
            payload,
            metadata: serde_json::to_value(HashMap::<String, String>::new()).unwrap(),
        }
    }

    mod event_store_test {
        use std::collections::HashMap;

        use crate::command::AggregateError;
        use crate::domain::event::DomainEvent;
        use crate::journal::EventStore;
        use crate::journal::PersistenceError;
        use crate::journal::persisted::EventStoreAggregateContext;
        use crate::journal::persisted::PersistedEventStore;
        use crate::journal::persisted::tests::EVENT_VERSION;
        use crate::journal::persisted::tests::MockRepo;
        use crate::journal::persisted::tests::TEST_AGGREGATE_ID;
        use crate::journal::persisted::tests::TestAggregate;
        use crate::journal::persisted::tests::TestEvents;
        use crate::journal::persisted::tests::test_serialized_event;

        #[tokio::test]
        async fn load() {
            let repo = MockRepo::with_events(Ok(vec![test_serialized_event(1, TestEvents::SomethingWasDone)]));
            let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo);
            let events = store.load_events(TEST_AGGREGATE_ID).await.unwrap();
            let event = events.first().unwrap();
            assert_eq!(1, event.sequence);
            assert_eq!("SomethingWasDone", event.payload.event_type());
            assert_eq!(EVENT_VERSION, event.payload.event_version());
        }

        #[tokio::test]
        async fn load_error() {
            let repo = MockRepo::with_events(Err(PersistenceError::OptimisticLockError));
            let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo);

            match store.load_events(TEST_AGGREGATE_ID).await {
                Err(AggregateError::AggregateConflict) => {}
                _ => panic!("expected technical error"),
            }
        }

        #[tokio::test]
        async fn load_aggregate_new() {
            let repo = MockRepo::with_events(Ok(vec![]));
            let store = PersistedEventStore::new(repo);
            let agg_context = store.load_aggregate(TEST_AGGREGATE_ID).await.unwrap();
            assert_eq!(None, agg_context.snapshot_version);
            assert_eq!(0, agg_context.current_sequence);
            assert_eq!(TEST_AGGREGATE_ID, agg_context.aggregate_id);
            assert_eq!(TestAggregate::default(), agg_context.aggregate);
            assert_eq!(
                format!("{agg_context:?}"),
                format!(
                    "EventStoreAggregateContext {{ aggregate_id: {TEST_AGGREGATE_ID:?}, current_sequence: 0, snapshot_version: None, .. }}"
                )
            );
            assert_eq!(
                format!("{:?}", store.with_snapshots(super::every(3))),
                r#"PersistedEventStore { aggregate_type: "TestAggregate", snapshot_policy: Every(3), upcasters: 0, .. }"#
            );
        }

        #[tokio::test]
        async fn load_aggregate_existing() {
            let repo = MockRepo::with_events(Ok(vec![
                test_serialized_event(1, TestEvents::Started),
                test_serialized_event(2, TestEvents::SomethingWasDone),
            ]));
            let store = PersistedEventStore::new(repo);
            let snapshot_context = store.load_aggregate(TEST_AGGREGATE_ID).await.unwrap();
            assert_eq!(None, snapshot_context.snapshot_version);
            assert_eq!(2, snapshot_context.current_sequence);
            assert_eq!(TEST_AGGREGATE_ID, snapshot_context.aggregate_id);
            assert_eq!(TestAggregate { something_happened: 1 }, snapshot_context.aggregate);
        }

        #[tokio::test]
        async fn load_aggregate_error() {
            let repo = MockRepo::with_events(Err(PersistenceError::OptimisticLockError));
            let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo);
            let result = store.load_aggregate(TEST_AGGREGATE_ID).await;
            match result {
                Err(AggregateError::AggregateConflict) => {}
                _ => panic!("expected aggregate conflict"),
            }
        }

        #[tokio::test]
        async fn commit() {
            let repo = MockRepo::with_commit(Box::new(|events, snapshot_update| {
                assert_eq!(3, events.len());
                let event = events.get(2).unwrap();
                assert_eq!(TEST_AGGREGATE_ID, event.aggregate_id);
                assert_eq!(3, event.sequence);

                assert!(snapshot_update.is_none());
            }));
            let store = PersistedEventStore::new(repo);
            let context = EventStoreAggregateContext {
                aggregate_id: TEST_AGGREGATE_ID.to_string(),
                aggregate: TestAggregate::default(),
                current_sequence: 0,
                snapshot_version: None,
            };
            let event_envelopes = store
                .commit(
                    vec![
                        TestEvents::Started,
                        TestEvents::SomethingWasDone,
                        TestEvents::SomethingWasDone,
                    ],
                    context,
                    HashMap::default(),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(3, event_envelopes.len());
            let event = event_envelopes.first().unwrap();
            assert_eq!(TEST_AGGREGATE_ID, event.aggregate_id);
            assert_eq!(TestEvents::Started, event.payload);
            assert_eq!(TestEvents::SomethingWasDone, event_envelopes.get(2).unwrap().payload);
        }
    }

    mod snapshotted_store_test {
        use std::collections::HashMap;

        use serde_json::json;

        use crate::command::AggregateError;
        use crate::domain::event::DomainEvent;
        use crate::journal::EventStore;
        use crate::journal::PersistenceError;
        use crate::journal::persisted::EventStoreAggregateContext;
        use crate::journal::persisted::PersistedEventStore;
        use crate::journal::persisted::tests::EVENT_VERSION;
        use crate::journal::persisted::tests::MockRepo;
        use crate::journal::persisted::tests::TEST_AGGREGATE_ID;
        use crate::journal::persisted::tests::TestAggregate;
        use crate::journal::persisted::tests::TestEvents;
        use crate::journal::persisted::tests::every;
        use crate::journal::persisted::tests::test_serialized_event;
        use crate::journal::snapshot::SerializedSnapshot;

        #[tokio::test]
        async fn load() {
            let repo = MockRepo::with_events(Ok(vec![test_serialized_event(1, TestEvents::SomethingWasDone)]));
            let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo).with_snapshots(every(2));
            let events = store.load_events(TEST_AGGREGATE_ID).await.unwrap();
            let event = events.first().unwrap();
            assert_eq!(1, event.sequence);
            assert_eq!("SomethingWasDone", event.payload.event_type());
            assert_eq!(EVENT_VERSION, event.payload.event_version());
        }

        #[tokio::test]
        async fn load_error() {
            let repo = MockRepo::with_events(Err(PersistenceError::OptimisticLockError));
            let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo).with_snapshots(every(2));
            let result = store.load_events(TEST_AGGREGATE_ID).await.unwrap_err();
            match result {
                AggregateError::AggregateConflict => {}
                _ => panic!("expected technical error"),
            }
        }

        #[tokio::test]
        async fn load_aggregate_new() {
            let repo = MockRepo::with_last_events(Ok(vec![]), Ok(None));
            let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo).with_snapshots(every(2));
            let snapshot_context = store.load_aggregate(TEST_AGGREGATE_ID).await.unwrap();
            assert_eq!(None, snapshot_context.snapshot_version);
            assert_eq!(0, snapshot_context.current_sequence);
            assert_eq!(TEST_AGGREGATE_ID, snapshot_context.aggregate_id);
            assert_eq!(TestAggregate::default(), snapshot_context.aggregate);
        }

        #[tokio::test]
        async fn load_aggregate_existing() {
            let repo = MockRepo::with_last_events(
                Ok(vec![test_serialized_event(4, TestEvents::SomethingWasDone)]),
                Ok(Some(SerializedSnapshot {
                    aggregate_id: TEST_AGGREGATE_ID.to_string(),
                    aggregate: serde_json::to_value(TestAggregate { something_happened: 3 }).unwrap(),
                    last_sequence: 3,
                    snapshot_version: 2,
                })),
            );
            let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo).with_snapshots(every(2));
            let snapshot_context = store.load_aggregate(TEST_AGGREGATE_ID).await.unwrap();
            assert_eq!(Some(2), snapshot_context.snapshot_version);
            assert_eq!(4, snapshot_context.current_sequence);
            assert_eq!(TEST_AGGREGATE_ID, snapshot_context.aggregate_id);
            assert_eq!(TestAggregate { something_happened: 4 }, snapshot_context.aggregate);
        }

        #[tokio::test]
        async fn load_aggregate_error() {
            let repo = MockRepo::with_snapshot(Err(PersistenceError::OptimisticLockError));
            let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo).with_snapshots(every(2));
            let result = store.load_aggregate(TEST_AGGREGATE_ID).await;
            match result {
                Err(AggregateError::AggregateConflict) => {}
                _ => panic!("expected technical error"),
            }
        }

        #[tokio::test]
        async fn load_aggregate_after_deserialize_error() {
            let repo = MockRepo::with_last_events(
                Ok(vec![
                    test_serialized_event(1, TestEvents::SomethingWasDone),
                    test_serialized_event(2, TestEvents::SomethingWasDone),
                ]),
                Ok(Some(SerializedSnapshot {
                    aggregate_id: TEST_AGGREGATE_ID.to_string(),
                    aggregate: serde_json::to_value(json!({
                        "incompatible": "structure", // should reapply events
                    }))
                    .unwrap(),
                    last_sequence: 2,
                    snapshot_version: 1,
                })),
            );

            let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo).with_snapshots(every(2));
            let snapshot_context = store.load_aggregate(TEST_AGGREGATE_ID).await.unwrap();
            assert_eq!(Some(1), snapshot_context.snapshot_version);
            assert_eq!(2, snapshot_context.current_sequence);
            assert_eq!(TEST_AGGREGATE_ID, snapshot_context.aggregate_id);
            assert_eq!(TestAggregate { something_happened: 2 }, snapshot_context.aggregate);
        }

        #[tokio::test]
        async fn commit_one_event() {
            let repo = MockRepo::with_commit(Box::new(|events, snapshot_update| {
                assert_eq!(1, events.len());
                let event = events.first().unwrap();
                assert_eq!(TEST_AGGREGATE_ID, event.aggregate_id);
                assert_eq!(1, event.sequence);

                assert_eq!(None, snapshot_update);
            }));
            let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo).with_snapshots(every(2));
            let context = EventStoreAggregateContext {
                aggregate_id: TEST_AGGREGATE_ID.to_string(),
                aggregate: TestAggregate::default(),
                current_sequence: 0,
                snapshot_version: Some(0),
            };
            let event_envelopes = store
                .commit(vec![TestEvents::Started], context, HashMap::default(), &[])
                .await
                .unwrap();
            assert_eq!(1, event_envelopes.len());
            let event = event_envelopes.first().unwrap();
            assert_eq!(TEST_AGGREGATE_ID, event.aggregate_id);
            assert_eq!(TestEvents::Started, event.payload);
        }

        #[tokio::test]
        async fn commit_one_event_with_previous() {
            let repo = MockRepo::with_commit(Box::new(|events, snapshot_update| {
                assert_eq!(1, events.len());
                let event = events.first().unwrap();
                assert_eq!(TEST_AGGREGATE_ID, event.aggregate_id);
                assert_eq!(3, event.sequence);

                assert_eq!(None, snapshot_update);
            }));
            let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo).with_snapshots(every(2));
            let context = EventStoreAggregateContext {
                aggregate_id: TEST_AGGREGATE_ID.to_string(),
                aggregate: TestAggregate::default(),
                current_sequence: 2,
                snapshot_version: Some(1),
            };
            let event_envelopes = store
                .commit(vec![TestEvents::SomethingWasDone], context, HashMap::default(), &[])
                .await
                .unwrap();
            assert_eq!(1, event_envelopes.len());
            let event = event_envelopes.first().unwrap();
            assert_eq!(TEST_AGGREGATE_ID, event.aggregate_id);
            assert_eq!(TestEvents::SomethingWasDone, event.payload);
        }

        #[tokio::test]
        async fn commit_three_events() {
            let repo = MockRepo::with_commit(Box::new(|events, snapshot_update| {
                assert_eq!(3, events.len());
                let event = events.get(2).unwrap();
                assert_eq!(TEST_AGGREGATE_ID, event.aggregate_id);
                assert_eq!(3, event.sequence);

                let snapshot_update = snapshot_update.unwrap();
                let aggregate_id = snapshot_update.aggregate_id;
                let aggregate = snapshot_update.payload;
                let snapshot_version = snapshot_update.next_version;
                assert_eq!(TEST_AGGREGATE_ID, aggregate_id.as_str());
                assert_eq!(1, snapshot_version);
                assert_eq!(json!(TestAggregate { something_happened: 1 }), aggregate);
            }));
            let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo).with_snapshots(every(2));
            let context = EventStoreAggregateContext {
                aggregate_id: TEST_AGGREGATE_ID.to_string(),
                aggregate: TestAggregate::default(),
                current_sequence: 0,
                snapshot_version: Some(0),
            };
            let event_envelopes = store
                .commit(
                    vec![
                        TestEvents::Started,
                        TestEvents::SomethingWasDone,
                        TestEvents::SomethingWasDone,
                    ],
                    context,
                    HashMap::default(),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(3, event_envelopes.len());
            let event = event_envelopes.first().unwrap();
            assert_eq!(TEST_AGGREGATE_ID, event.aggregate_id);
            assert_eq!(TestEvents::Started, event.payload);
            assert_eq!(TestEvents::SomethingWasDone, event_envelopes.get(2).unwrap().payload);
        }

        #[tokio::test]
        async fn commit_two_with_existing() {
            let repo = MockRepo::with_commit(Box::new(|events, snapshot_update| {
                assert_eq!(2, events.len());
                let event = events.get(1).unwrap();
                assert_eq!(TEST_AGGREGATE_ID, event.aggregate_id);
                assert_eq!(3, event.sequence);

                let snapshot_update = snapshot_update.unwrap();
                let aggregate_id = snapshot_update.aggregate_id;
                let aggregate = snapshot_update.payload;
                let snapshot_version = snapshot_update.next_version;
                assert_eq!(TEST_AGGREGATE_ID, aggregate_id.as_str());
                assert_eq!(2, snapshot_version);
                assert_eq!(json!(TestAggregate { something_happened: 1 }), aggregate);
            }));
            let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo).with_snapshots(every(2));
            let context = EventStoreAggregateContext {
                aggregate_id: TEST_AGGREGATE_ID.to_string(),
                aggregate: TestAggregate::default(),
                current_sequence: 1,
                snapshot_version: Some(1),
            };
            let event_envelopes = store
                .commit(
                    vec![TestEvents::SomethingWasDone, TestEvents::SomethingWasDone],
                    context,
                    HashMap::default(),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(2, event_envelopes.len());
            let first_event = event_envelopes.first().unwrap();
            assert_eq!(TEST_AGGREGATE_ID, first_event.aggregate_id);
            assert_eq!(TestEvents::SomethingWasDone, first_event.payload);
            let last_event = event_envelopes.get(1).unwrap();
            assert_eq!(TEST_AGGREGATE_ID, last_event.aggregate_id);
            assert_eq!(TestEvents::SomethingWasDone, last_event.payload);
        }

        #[tokio::test]
        async fn commit_five() {
            let repo = MockRepo::with_commit(Box::new(|events, snapshot_update| {
                assert_eq!(5, events.len());
                let event = events.get(4).unwrap();
                assert_eq!(TEST_AGGREGATE_ID, event.aggregate_id);
                assert_eq!(5, event.sequence);

                let snapshot_update = snapshot_update.unwrap();
                let aggregate_id = snapshot_update.aggregate_id;
                let aggregate = snapshot_update.payload;
                let snapshot_version = snapshot_update.next_version;
                assert_eq!(TEST_AGGREGATE_ID, aggregate_id.as_str());
                assert_eq!(1, snapshot_version);
                assert_eq!(json!(TestAggregate { something_happened: 3 }), aggregate);
            }));
            let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo).with_snapshots(every(2));
            let context = EventStoreAggregateContext {
                aggregate_id: TEST_AGGREGATE_ID.to_string(),
                aggregate: TestAggregate::default(),
                current_sequence: 0,
                snapshot_version: Some(0),
            };
            let event_envelopes = store
                .commit(
                    vec![
                        TestEvents::Started,
                        TestEvents::SomethingWasDone,
                        TestEvents::SomethingWasDone,
                        TestEvents::SomethingWasDone,
                        TestEvents::SomethingWasDone,
                    ],
                    context,
                    HashMap::default(),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(5, event_envelopes.len());
            let first_event = event_envelopes.first().unwrap();
            assert_eq!(TEST_AGGREGATE_ID, first_event.aggregate_id);
            assert_eq!(TestEvents::Started, first_event.payload);
            let last_event = event_envelopes.get(4).unwrap();
            assert_eq!(TEST_AGGREGATE_ID, last_event.aggregate_id);
            assert_eq!(TestEvents::SomethingWasDone, last_event.payload);
        }
    }

    #[tokio::test]
    async fn a_repository_without_an_outbox_refuses_tasks_before_writing_anything() {
        let repo = MockRepo::with_commit(Box::new(|_, _| panic!("a refused commit must not persist")));
        let store = PersistedEventStore::<MockRepo, TestAggregate>::new(repo);
        let task = crate::task::NewTask::from_payload(
            crate::task::format::TaskFormat::try_new("notify", 1).unwrap(),
            serde_json::json!({}),
        )
        .unwrap();
        let context = super::EventStoreAggregateContext::empty(TEST_AGGREGATE_ID);
        let result =
            crate::journal::EventStore::commit(&store, vec![TestEvents::Started], context, HashMap::new(), &[task]).await;
        let Err(crate::command::AggregateError::UnexpectedError(error)) = result else {
            panic!("a repository without an outbox refuses tasks");
        };
        assert_eq!(
            error.to_string(),
            "the event repository has no outbox: its commits cannot publish tasks"
        );
    }

    use super::*;
    #[test]
    fn test_source_of_truth() {
        assert_eq!(0, SnapshotPolicy::Disabled.snapshot_prefix_len(5, 3));
        assert_eq!(0, SnapshotPolicy::Every(every(5)).snapshot_prefix_len(5, 3));
        assert_eq!(3, SnapshotPolicy::Every(every(4)).snapshot_prefix_len(5, 3));
        assert_eq!(3, SnapshotPolicy::Every(every(4)).snapshot_prefix_len(5, 4));
        assert_eq!(7, SnapshotPolicy::Every(every(4)).snapshot_prefix_len(5, 8));
    }
}

#[cfg(test)]
mod contract_tests {
    use super::tests::*;
    use super::*;
    use crate::journal::serialized_event::tests::property_config;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(property_config())]
        #[test]
        fn property_snapshot_boundary_matches_enumerated_prefix(interval in 1usize..40, sequence in 0usize..100, count in 0usize..120) {
            let expected = (1..=count).filter(|offset| (sequence + offset).is_multiple_of(interval)).max().unwrap_or(0);
            prop_assert_eq!(SnapshotPolicy::Every(every(interval)).snapshot_prefix_len(sequence, count), expected);
            prop_assert_eq!(SnapshotPolicy::Disabled.snapshot_prefix_len(sequence, count), 0);
        }
    }
    #[tokio::test]
    async fn persist_and_tail_technical_failures_are_not_conflicts() {
        let store = PersistedEventStore::<_, TestAggregate>::new(MockRepo::with_commit_error(PersistenceError::ConnectionError(
            "persist failed".into(),
        )));
        let context = EventStoreAggregateContext::empty("a");
        assert!(
            matches!(store.commit(vec![TestEvents::Started], context, HashMap::new(), &[]).await, Err(AggregateError::DatabaseConnectionError(e)) if e.to_string() == "persist failed")
        );
        let store = PersistedEventStore::<_, TestAggregate>::new(MockRepo::with_last_events(
            Err(PersistenceError::UnknownError("tail failed".into())),
            Ok(None),
        ))
        .with_snapshots(every(2));
        assert!(
            matches!(store.load_aggregate("a").await, Err(AggregateError::UnexpectedError(e)) if e.to_string() == "tail failed")
        );
        let mut bad = test_serialized_event(1, TestEvents::Started);
        bad.payload = serde_json::json!({"unknown":true});
        let store = PersistedEventStore::<_, TestAggregate>::new(MockRepo::with_last_events(Ok(vec![bad.clone()]), Ok(None)))
            .with_snapshots(every(2));
        assert!(matches!(
            store.load_aggregate("a").await,
            Err(AggregateError::DeserializationError(_))
        ));
        // A history that breaks in the middle: the events before the bad one are applied, then the load fails, with or
        // without snapshots; no state of the half-replayed aggregate reaches the caller.
        bad.sequence = 2;
        let history = vec![
            test_serialized_event(1, TestEvents::SomethingWasDone),
            bad,
            test_serialized_event(3, TestEvents::SomethingWasDone),
        ];
        let store = PersistedEventStore::<_, TestAggregate>::new(MockRepo::with_last_events(Ok(history.clone()), Ok(None)))
            .with_snapshots(every(2));
        assert!(matches!(
            store.load_aggregate("a").await,
            Err(AggregateError::DeserializationError(_))
        ));
        let store = PersistedEventStore::<_, TestAggregate>::new(MockRepo::with_events(Ok(history)));
        assert!(matches!(
            store.load_aggregate("a").await,
            Err(AggregateError::DeserializationError(_))
        ));
    }
    #[tokio::test]
    async fn committed_envelopes_preserve_every_field_and_nonzero_sequence() {
        let metadata = HashMap::from([("trace".into(), "🦀".into())]);
        let expected_metadata = metadata.clone();
        let store = PersistedEventStore::<_, TestAggregate>::new(MockRepo::with_commit(Box::new(move |events, snapshot| {
            assert!(snapshot.is_none());
            assert_eq!(events.len(), 2);
            for (i, event) in events.iter().enumerate() {
                assert_eq!(event.aggregate_id, "id");
                assert_eq!(event.sequence, 10 + i);
                assert_eq!(event.aggregate_type, TestAggregate::TYPE);
                assert_eq!(event.event_version, EVENT_VERSION);
                assert_eq!(event.metadata, serde_json::json!(expected_metadata));
                assert_eq!(
                    event.payload,
                    serde_json::json!([TestEvents::Started, TestEvents::SomethingWasDone][i])
                );
            }
        })));
        let mut context = EventStoreAggregateContext::empty("id");
        context.current_sequence = 9;
        let result = store
            .commit(
                vec![TestEvents::Started, TestEvents::SomethingWasDone],
                context,
                metadata.clone(),
                &[],
            )
            .await
            .unwrap();
        assert_eq!(result.len(), 2);
        for (i, event) in result.iter().enumerate() {
            assert_eq!(event.aggregate_id, "id");
            assert_eq!(event.sequence, 10 + i);
            assert_eq!(event.metadata, metadata);
        }
    }
}

#[cfg(test)]
mod serialization_failure_tests {
    use super::tests::MockRepo;
    use super::*;
    use crate::domain::{command::Decide, event::DomainEvent};
    use serde::{Deserialize, Serialize, Serializer};
    #[derive(Clone, Debug, PartialEq, Deserialize)]
    struct Event(bool);
    impl Serialize for Event {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            if self.0 {
                Err(serde::ser::Error::custom("event cannot serialize"))
            } else {
                serializer.serialize_bool(false)
            }
        }
    }
    impl DomainEvent for Event {
        fn event_type(&self) -> &'static str {
            "event"
        }
        fn event_version(&self) -> &'static str {
            "1"
        }
    }
    #[derive(Clone, Debug, Default, Deserialize)]
    struct State(bool);
    impl Serialize for State {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            if self.0 {
                Err(serde::ser::Error::custom("snapshot cannot serialize"))
            } else {
                serializer.serialize_bool(false)
            }
        }
    }
    impl Aggregate for State {
        const TYPE: &'static str = "broken_serializer";
        type Id = String;
        type Command = ();
        type Event = Event;
        type Error = std::convert::Infallible;
        type Rules = ();
        fn apply(&mut self, _: Event) {}
    }
    impl Decide<State> for () {
        fn decide(self, _: &State, _: &()) -> Result<Event, std::convert::Infallible> {
            Ok(Event(false))
        }
    }
    #[tokio::test]
    async fn serialization_failure_precedes_any_repository_write() {
        for snapshot in [false, true] {
            let repo = MockRepo::with_commit(Box::new(|_, _| {
                panic!("serialization failure must prevent repository writes")
            }));
            let store = if snapshot {
                PersistedEventStore::<_, State>::new(repo).with_snapshots(NonZeroUsize::MIN)
            } else {
                PersistedEventStore::<_, State>::new(repo)
            };
            let context = EventStoreAggregateContext {
                aggregate_id: "a".into(),
                aggregate: State(snapshot),
                current_sequence: 0,
                snapshot_version: None,
            };
            let error = store
                .commit(vec![Event(!snapshot)], context, HashMap::new(), &[])
                .await
                .unwrap_err();
            assert!(matches!(error, AggregateError::DeserializationError(_)));
        }
    }

    #[tokio::test]
    async fn sequence_and_snapshot_version_overflow_precede_apply_serialization_and_persist() {
        for (sequence, version, count) in [(usize::MAX, None, 1), (usize::MAX - 1, None, 2), (0, Some(usize::MAX), 1)] {
            let repo = MockRepo::with_commit(Box::new(|_, _| panic!("invalid batch reached persist")));
            let store = PersistedEventStore::<_, Guarded>::new(repo).with_snapshots(NonZeroUsize::MIN);
            let context = EventStoreAggregateContext {
                aggregate_id: "overflow".into(),
                aggregate: Guarded,
                current_sequence: sequence,
                snapshot_version: version,
            };
            let error = store
                .commit(vec![GuardedEvent; count], context, HashMap::new(), &[])
                .await
                .unwrap_err();
            assert!(matches!(error, AggregateError::UnexpectedError(_)));
        }
    }
    #[tokio::test]
    async fn last_representable_sequence_and_snapshot_version_are_committed() {
        use super::tests::{TestAggregate, TestEvents};
        for snapshot in [false, true] {
            let repo = MockRepo::with_commit(Box::new(move |events, update| {
                assert_eq!(events[0].sequence, usize::MAX);
                if snapshot {
                    assert_eq!(update.unwrap().next_version, usize::MAX);
                } else {
                    assert!(update.is_none());
                }
            }));
            let store = if snapshot {
                PersistedEventStore::<_, TestAggregate>::new(repo).with_snapshots(NonZeroUsize::MIN)
            } else {
                PersistedEventStore::new(repo)
            };
            let context = EventStoreAggregateContext {
                aggregate_id: "max".into(),
                aggregate: TestAggregate::default(),
                current_sequence: usize::MAX - 1,
                snapshot_version: Some(usize::MAX - 1),
            };
            assert_eq!(
                store
                    .commit(vec![TestEvents::SomethingWasDone], context, HashMap::new(), &[])
                    .await
                    .unwrap()[0]
                    .sequence,
                usize::MAX
            );
        }
    }
    #[derive(Clone, Debug, Default, Deserialize)]
    struct Guarded;
    impl Serialize for Guarded {
        fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            panic!("invalid batch serialized state")
        }
    }
    #[derive(Clone, Debug, PartialEq, Deserialize)]
    struct GuardedEvent;
    impl Serialize for GuardedEvent {
        fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            panic!("invalid batch serialized event")
        }
    }
    impl DomainEvent for GuardedEvent {
        fn event_type(&self) -> &'static str {
            "guarded"
        }
        fn event_version(&self) -> &'static str {
            "1"
        }
    }
    impl Aggregate for Guarded {
        const TYPE: &'static str = "guarded";
        type Id = String;
        type Command = ();
        type Event = GuardedEvent;
        type Error = std::convert::Infallible;
        type Rules = ();
        fn apply(&mut self, _: GuardedEvent) {
            panic!("invalid batch applied event")
        }
    }
    impl Decide<Guarded> for () {
        fn decide(self, _: &Guarded, _: &()) -> Result<GuardedEvent, std::convert::Infallible> {
            Ok(GuardedEvent)
        }
    }
}
