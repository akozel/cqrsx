// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::{
    aggregate::Aggregate,
    event::DomainEvent,
    event_handler::EventHandler,
    store::PersistenceErrorObserver,
    store::{PersistenceError, repository::PersistedEventRepository, stream::ReplayStream, upcaster::EventUpcaster},
};
use std::{marker::PhantomData, time::Duration};

/// One successfully replayed event, as a progress observer sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayProgress<'a> {
    pub aggregate_id: &'a str,
    pub sequence: usize,
    pub event_type: String,
    /// Events handled by this replay so far, this one included.
    pub replayed: u64,
}

/// Observes every event the handler accepted during a replay; it cannot change the replay.
pub type ReplayProgressObserver = dyn Fn(&ReplayProgress<'_>) + Send + Sync + 'static;

/// A utility for replaying committed events to a `EventHandler`.
///
/// ```rust
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
/// #
/// # #[derive(Debug, Default)]
/// # pub struct MyEventHandler;
/// #
/// # #[async_trait]
/// # impl EventHandler<MyAggregate> for MyEventHandler {
/// #     async fn handle(&self, _aggregate_id: &str, _events: &[EventEnvelope<MyAggregate>]) -> Result<(), cqrsx::cqrs::store::PersistenceError> { Ok(()) }
/// # }
/// #
/// # pub struct MyRepository;
/// #
/// # impl PersistedEventRepository for MyRepository {
/// #     async fn get_events<A: Aggregate>(
/// #         &self,
/// #         _aggregate_id: &str,
/// #     ) -> Result<Vec<SerializedEvent>, PersistenceError> {
/// #         todo!()
/// #     }
/// #
/// #     async fn get_events_after<A: Aggregate>(
/// #         &self,
/// #         _aggregate_id: &str,
/// #         _after_sequence: usize,
/// #     ) -> Result<Vec<SerializedEvent>, PersistenceError> {
/// #         todo!()
/// #     }
/// #
/// #     async fn get_snapshot<A: Aggregate>(
/// #         &self,
/// #         _aggregate_id: &str,
/// #     ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
/// #         todo!()
/// #     }
/// #
/// #     async fn persist<A: Aggregate>(
/// #         &self,
/// #         _events: &[SerializedEvent],
/// #         _snapshot_update: Option<cqrsx::cqrs::store::snapshot::SnapshotUpdate>,
/// #     ) -> Result<(), PersistenceError> {
/// #         todo!()
/// #     }
/// #
/// #     async fn stream_events<A: Aggregate>(
/// #         &self,
/// #         _aggregate_id: &str,
/// #     ) -> Result<ReplayStream, PersistenceError> {
/// #         todo!()
/// #     }
/// #
/// #     async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
/// #         todo!()
/// #     }
/// # }
/// # }
/// # use example::MyAggregate;
/// # use example::MyEventHandler;
/// # use example::MyRepository;
/// use cqrsx::cqrs::event_handler::replay::EventReplay;
///
/// fn update(repo: MyRepository, handler: MyEventHandler) {
///     let replay = EventReplay::new(repo, handler);
///     replay.replay_all();
/// }
/// ```
pub struct EventReplay<R, H, A>
where
    R: PersistedEventRepository,
    H: EventHandler<A>,
    A: Aggregate,
{
    repository: R,
    handler: H,
    event_upcasters: Vec<Box<dyn EventUpcaster>>,
    error_observer: Option<Box<PersistenceErrorObserver>>,
    progress_observer: Option<Box<ReplayProgressObserver>>,
    pace: Duration,
    phantom_data: PhantomData<A>,
}

impl<R, H, A> EventReplay<R, H, A>
where
    R: PersistedEventRepository,
    H: EventHandler<A>,
    A: Aggregate,
{
    /// Create a new replay utility using the provided event repository as the source and the
    /// handler as the target.
    pub fn new(repository: R, handler: H) -> Self {
        Self {
            repository,
            handler,
            event_upcasters: vec![],
            error_observer: None,
            progress_observer: None,
            pace: Duration::ZERO,
            phantom_data: PhantomData,
        }
    }

    /// Configures the handler replayer to use event upcasters when replaying.
    /// The EventUpcasters within the Vec should be placed in the
    /// order that they should be applied
    ///
    /// E.g., an upcaster for version 0.2.3 should be placed before an upcaster for version 0.2.4
    pub fn with_upcasters(mut self, event_upcasters: Vec<Box<dyn EventUpcaster>>) -> Self {
        self.event_upcasters = event_upcasters;
        self
    }

    /// Pause for `pace` before handing each event to the handler (default: no pause). Limits the
    /// write rate of a rebuild, or slows it down so people can watch it.
    pub fn with_pace(mut self, pace: Duration) -> Self {
        self.pace = pace;
        self
    }

    /// Observe progress: called after every event the handler accepted, never for an event that
    /// failed to decode or to be handled. `replayed` counts within one `replay`/`replay_all` call.
    pub fn set_progress_observer(&mut self, progress_observer: Box<ReplayProgressObserver>) {
        self.progress_observer = Some(progress_observer);
    }

    /// Allows the user to apply a custom error handler to the handler replay.
    ///
    /// _Example: An error handler that panics on any error._
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
    /// #
    /// # #[derive(Debug, Default)]
    /// # pub struct MyEventHandler;
    /// #
    /// # #[async_trait]
    /// # impl EventHandler<MyAggregate> for MyEventHandler {
    /// #     async fn handle(&self, _aggregate_id: &str, _events: &[EventEnvelope<MyAggregate>]) -> Result<(), cqrsx::cqrs::store::PersistenceError> { Ok(()) }
    /// # }
    /// #
    /// # pub struct MyRepository;
    /// #
    /// # impl PersistedEventRepository for MyRepository {
    /// #     async fn get_events<A: Aggregate>(
    /// #         &self,
    /// #         _aggregate_id: &str,
    /// #     ) -> Result<Vec<SerializedEvent>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn get_events_after<A: Aggregate>(
    /// #         &self,
    /// #         _aggregate_id: &str,
    /// #         _after_sequence: usize,
    /// #     ) -> Result<Vec<SerializedEvent>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn get_snapshot<A: Aggregate>(
    /// #         &self,
    /// #         _aggregate_id: &str,
    /// #     ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn persist<A: Aggregate>(
    /// #         &self,
    /// #         _events: &[SerializedEvent],
    /// #         _snapshot_update: Option<cqrsx::cqrs::store::snapshot::SnapshotUpdate>,
    /// #     ) -> Result<(), PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn stream_events<A: Aggregate>(
    /// #         &self,
    /// #         _aggregate_id: &str,
    /// #     ) -> Result<ReplayStream, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn stream_all_events<A: Aggregate>(&self) -> Result<ReplayStream, PersistenceError> {
    /// #         todo!()
    /// #     }
    /// # }
    /// # }
    /// # use example::MyAggregate;
    /// # use example::MyEventHandler;
    /// # use example::MyRepository;
    /// # use cqrsx::cqrs::projection::ViewProjection;
    /// # use cqrsx::cqrs::event_handler::replay::EventReplay;
    /// # use cqrsx::cqrs::store::stream::ReplayStream;
    /// # fn config(mut replay: EventReplay<MyRepository,MyEventHandler,MyAggregate>) {
    /// replay.set_error_observer(Box::new(|e|panic!("{}",e)));
    /// # }
    /// ```
    pub fn set_error_observer(&mut self, error_observer: Box<PersistenceErrorObserver>) {
        self.error_observer = Some(error_observer);
    }

    /// Replay one aggregate. On failure the applied prefix remains partial;
    /// rebuild into a clean projection rather than repeating it automatically.
    pub async fn replay(&self, aggregate_id: &str) -> Result<(), PersistenceError> {
        let result = async {
            let stream = self.repository.stream_events::<A>(aggregate_id).await?;
            self.run_stream(stream).await
        }
        .await;
        result.inspect_err(|error| self.report(error))
    }

    /// Replay all aggregates, stopping on the first open/read/decode/handle error.
    pub async fn replay_all(&self) -> Result<(), PersistenceError> {
        let result = async {
            let stream = self.repository.stream_all_events::<A>().await?;
            self.run_stream(stream).await
        }
        .await;
        result.inspect_err(|error| self.report(error))
    }

    async fn run_stream(&self, mut stream: ReplayStream) -> Result<(), PersistenceError> {
        let mut replayed = 0;
        while let Some(event) = stream.next::<A>(&self.event_upcasters).await {
            let event = event?;
            if !self.pace.is_zero() {
                tokio::time::sleep(self.pace).await;
            }
            self.handler.handle(&event.aggregate_id, std::slice::from_ref(&event)).await?;
            replayed += 1;
            if let Some(observer) = &self.progress_observer {
                observer(&ReplayProgress {
                    aggregate_id: &event.aggregate_id,
                    sequence: event.sequence,
                    event_type: event.payload.event_type(),
                    replayed,
                });
            }
        }
        Ok(())
    }

    fn report(&self, error: &PersistenceError) {
        if let Some(handler) = &self.error_observer {
            handler(error);
        }
    }
}

#[cfg(test)]
mod test {
    use crate::cqrs::store::PersistenceError;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use serde_json::Map;
    use serde_json::Value;
    use serde_json::Value::Object;
    use serde_json::json;

    use crate::cqrs::event::EventEnvelope;
    use crate::cqrs::event_handler::EventHandler;
    use crate::cqrs::event_handler::replay::EventReplay;
    use crate::cqrs::store::persisted::tests::MockRepo;
    use crate::cqrs::store::serialized_event::SerializedEvent;
    use crate::cqrs::store::stream::tests::MyAggregate;
    use crate::cqrs::store::stream::tests::MyEvents;
    use crate::cqrs::store::upcaster::NumericVersionUpcaster;

    #[derive(Debug)]
    struct CapturingEventHandler {
        events: Arc<Mutex<Vec<EventEnvelope<MyAggregate>>>>,
        fail: Option<(usize, bool)>,
    }

    impl CapturingEventHandler {
        fn new() -> (Self, Arc<Mutex<Vec<EventEnvelope<MyAggregate>>>>) {
            let events: Arc<Mutex<Vec<EventEnvelope<MyAggregate>>>> = Arc::default();
            let handler = Self {
                events: events.clone(),
                fail: None,
            };
            (handler, events)
        }
    }

    #[async_trait]
    impl EventHandler<MyAggregate> for CapturingEventHandler {
        async fn handle(
            &self,
            _aggregate_id: &str,
            events: &[EventEnvelope<MyAggregate>],
        ) -> Result<(), crate::cqrs::store::PersistenceError> {
            let mut event_list = self.events.lock().unwrap();
            for event in events {
                if self.fail == Some((event.sequence, false)) {
                    return Err(PersistenceError::ConnectionError(
                        std::io::Error::other("handle failed").into(),
                    ));
                }
                event_list.push(event.to_owned());
                if self.fail == Some((event.sequence, true)) {
                    return Err(PersistenceError::ConnectionError(
                        std::io::Error::other("handle failed after write").into(),
                    ));
                }
            }
            Ok(())
        }
    }

    const AGGREGATE_ID: &str = "test_aggregate";

    #[tokio::test]
    async fn event_replay() {
        let expected_events = vec![EventEnvelope {
            aggregate_id: AGGREGATE_ID.to_string(),
            sequence: 1,
            payload: MyEvents::SomethingWasDone,
            metadata: HashMap::default(),
        }];
        let ser_events: Vec<SerializedEvent> = expected_events
            .iter()
            .map(|e| SerializedEvent::try_from(e).unwrap())
            .collect();
        let event_repo = MockRepo::with_events(Ok(ser_events.clone()));
        let (handler, event_list) = CapturingEventHandler::new();
        let event_replay = EventReplay::new(event_repo, handler);
        event_replay.replay(AGGREGATE_ID).await.unwrap();

        let events = event_list.lock().unwrap().to_owned();
        assert_events_eq(&events, &expected_events);

        // handler all
        let event_repo = MockRepo::with_events(Ok(ser_events));
        let (handler, event_list) = CapturingEventHandler::new();
        let event_replay = EventReplay::new(event_repo, handler);
        event_replay.replay_all().await.unwrap();

        let events = event_list.lock().unwrap().to_owned();
        assert_events_eq(&events, &expected_events);
    }

    #[tokio::test]
    async fn event_replay_with_upcasting() {
        let expected_events = vec![EventEnvelope {
            aggregate_id: AGGREGATE_ID.to_string(),
            sequence: 1,
            payload: MyEvents::SomethingWasDone,
            metadata: HashMap::default(),
        }];

        // a "legacy" event that contains a single property
        // that the upcasting function should remove to upcast
        // to the expected event
        let ser_events: Vec<SerializedEvent> = vec![SerializedEvent {
            aggregate_id: AGGREGATE_ID.to_string(),
            sequence: 1,
            aggregate_type: "MyAggregate".to_string(),
            event_type: "SomethingWasDone".to_string(),
            event_version: "0.0.1".to_string(),
            payload: json!({"LegacyName": ()}),
            metadata: Object(Map::default()),
        }];

        fn upcasting_fn(_payload: Value) -> Value {
            // We return an implemented event, which indicates
            // that an "upcast" has happened
            json!({"SomethingWasDone": ()})
        }

        let event_repo = MockRepo::with_events(Ok(ser_events.clone()));
        let (handler, event_list) = CapturingEventHandler::new();
        let event_replay = EventReplay::new(event_repo, handler).with_upcasters(vec![Box::new(NumericVersionUpcaster::new(
            "SomethingWasDone",
            "0.1.0",
            Box::new(upcasting_fn),
        ))]);

        event_replay.replay(AGGREGATE_ID).await.unwrap();

        let events = event_list.lock().unwrap().to_owned();
        assert_events_eq(&expected_events, &events);

        // handler all
        let event_repo = MockRepo::with_events(Ok(ser_events));
        let (handler, event_list) = CapturingEventHandler::new();
        let event_replay = EventReplay::new(event_repo, handler).with_upcasters(vec![Box::new(NumericVersionUpcaster::new(
            "SomethingWasDone",
            "0.1.0",
            Box::new(upcasting_fn),
        ))]);
        event_replay.replay_all().await.unwrap();

        let events = event_list.lock().unwrap().to_owned();
        assert_events_eq(&expected_events, &events);
    }

    fn assert_events_eq(expected: &[EventEnvelope<MyAggregate>], found: &[EventEnvelope<MyAggregate>]) {
        assert_eq!(expected.len(), found.len());
        for i in 0..expected.len() {
            let ex = expected.get(i).unwrap();
            let f = found.get(i).unwrap();
            assert_eq!(ex.aggregate_id, f.aggregate_id);
            assert_eq!(ex.sequence, f.sequence);
            assert_eq!(ex.payload, f.payload);
            assert_eq!(ex.metadata, f.metadata);
        }
    }
    #[tokio::test]
    async fn empty_replay_and_open_failures_have_observable_outcomes() {
        for all in [false, true] {
            let (handler, seen) = CapturingEventHandler::new();
            let replay = EventReplay::new(MockRepo::with_events(Ok(vec![])), handler);
            if all {
                replay.replay_all().await.unwrap();
            } else {
                replay.replay("empty").await.unwrap();
            }
            assert!(seen.lock().unwrap().is_empty());
            let (handler, seen) = CapturingEventHandler::new();
            let replay = EventReplay::new(
                MockRepo::with_events(Err(PersistenceError::ConnectionError("open failed".into()))),
                handler,
            );
            let result = if all {
                replay.replay_all().await
            } else {
                replay.replay("empty").await
            };
            assert!(matches!(result, Err(PersistenceError::ConnectionError(_))));
            assert!(seen.lock().unwrap().is_empty());
        }
    }
    #[tokio::test]
    async fn incomplete_replay_stops_and_returns_the_first_decode_failure() {
        for observe_errors in [false, true] {
            let envelope = EventEnvelope::<MyAggregate> {
                aggregate_id: AGGREGATE_ID.into(),
                sequence: 1,
                payload: MyEvents::SomethingWasDone,
                metadata: Default::default(),
            };
            let first = SerializedEvent::try_from(&envelope).unwrap();
            let mut corrupt = first.clone();
            corrupt.sequence = 2;
            corrupt.payload = json!({"unknown": true});
            let mut last = first.clone();
            last.sequence = 3;
            let (handler, seen) = CapturingEventHandler::new();
            let mut replay = EventReplay::new(MockRepo::with_events(Ok(vec![first, corrupt, last])), handler);
            let errors = Arc::new(Mutex::new(Vec::new()));
            if observe_errors {
                let errors = errors.clone();
                replay.set_error_observer(Box::new(move |e| errors.lock().unwrap().push(e.to_string())));
            }
            assert!(replay.replay_all().await.is_err());
            let events = seen.lock().unwrap();
            assert_eq!(events.iter().map(|e| e.sequence).collect::<Vec<_>>(), [1]);
            assert!(
                events
                    .iter()
                    .all(|e| e.aggregate_id == AGGREGATE_ID && e.payload == MyEvents::SomethingWasDone && e.metadata.is_empty())
            );
            assert_eq!(errors.lock().unwrap().len(), usize::from(observe_errors));
        }
    }
    struct StreamRepository {
        items: Mutex<Vec<Result<SerializedEvent, PersistenceError>>>,
        fail_open: bool,
    }
    impl crate::cqrs::store::repository::PersistedEventRepository for StreamRepository {
        async fn get_events<A: crate::cqrs::aggregate::Aggregate>(
            &self,
            _: &str,
        ) -> Result<Vec<SerializedEvent>, PersistenceError> {
            unreachable!()
        }
        async fn get_events_after<A: crate::cqrs::aggregate::Aggregate>(
            &self,
            _: &str,
            _: usize,
        ) -> Result<Vec<SerializedEvent>, PersistenceError> {
            unreachable!()
        }
        async fn get_snapshot<A: crate::cqrs::aggregate::Aggregate>(
            &self,
            _: &str,
        ) -> Result<Option<crate::cqrs::store::snapshot::SerializedSnapshot>, PersistenceError> {
            unreachable!()
        }
        async fn persist<A: crate::cqrs::aggregate::Aggregate>(
            &self,
            _: &[SerializedEvent],
            _: Option<crate::cqrs::store::snapshot::SnapshotUpdate>,
        ) -> Result<(), PersistenceError> {
            unreachable!()
        }
        async fn stream_events<A: crate::cqrs::aggregate::Aggregate>(
            &self,
            _: &str,
        ) -> Result<super::ReplayStream, PersistenceError> {
            self.stream_all_events::<A>().await
        }
        async fn stream_all_events<A: crate::cqrs::aggregate::Aggregate>(&self) -> Result<super::ReplayStream, PersistenceError> {
            if self.fail_open {
                return Err(PersistenceError::ConnectionError(std::io::Error::other("open failed").into()));
            }
            let items = std::mem::take(&mut *self.items.lock().unwrap());
            let (mut feed, stream) = super::ReplayStream::new(items.len().max(1));
            for item in items {
                feed.push(item).await?;
            }
            Ok(stream)
        }
    }
    #[tokio::test]
    async fn replay_and_replay_all_return_first_failure_and_report_it_once() {
        for all in [false, true] {
            for observe_errors in [false, true] {
                for stage in ["open", "read", "decode", "handle", "written", "success", "empty"] {
                    let envelope = EventEnvelope::<MyAggregate> {
                        aggregate_id: AGGREGATE_ID.into(),
                        sequence: 1,
                        payload: MyEvents::SomethingWasDone,
                        metadata: HashMap::from([("trace".into(), "данные".into())]),
                    };
                    let first = SerializedEvent::try_from(&envelope).unwrap();
                    let mut second = first.clone();
                    second.sequence = 2;
                    let mut third = first.clone();
                    third.sequence = 3;
                    if stage == "decode" {
                        second.payload = json!({"unknown":true});
                    }
                    let middle = if stage == "read" {
                        Err(PersistenceError::ConnectionError(std::io::Error::other("read failed").into()))
                    } else {
                        Ok(second)
                    };
                    let items = if stage == "empty" {
                        vec![]
                    } else {
                        vec![Ok(first), middle, Ok(third)]
                    };
                    let (mut handler, seen) = CapturingEventHandler::new();
                    handler.fail = match stage {
                        "handle" => Some((2, false)),
                        "written" => Some((2, true)),
                        _ => None,
                    };
                    let mut replay = EventReplay::new(
                        StreamRepository {
                            items: Mutex::new(items),
                            fail_open: stage == "open",
                        },
                        handler,
                    );
                    let errors = Arc::new(Mutex::new(Vec::new()));
                    if observe_errors {
                        let errors = errors.clone();
                        replay.set_error_observer(Box::new(move |error| errors.lock().unwrap().push(error.to_string())));
                    }
                    let result = if all {
                        replay.replay_all().await
                    } else {
                        replay.replay(AGGREGATE_ID).await
                    };
                    let expected = match stage {
                        "open" | "empty" => vec![],
                        "written" => vec![1, 2],
                        "success" => vec![1, 2, 3],
                        _ => vec![1],
                    };
                    assert_eq!(seen.lock().unwrap().iter().map(|e| e.sequence).collect::<Vec<_>>(), expected);
                    if stage == "success" || stage == "empty" {
                        result.unwrap();
                        assert!(errors.lock().unwrap().is_empty());
                    } else {
                        let error = result.unwrap_err();
                        if stage == "decode" {
                            assert!(
                                matches!(&error,PersistenceError::DeserializationError(source) if source.is::<serde_json::Error>())
                            );
                        } else {
                            assert!(matches!(&error,PersistenceError::ConnectionError(source) if source.is::<std::io::Error>()));
                        }
                        assert_eq!(
                            *errors.lock().unwrap(),
                            if observe_errors { vec![error.to_string()] } else { vec![] }
                        );
                    }
                }
            }
        }
    }

    type Seen = Arc<Mutex<Vec<(String, usize, String, u64)>>>;

    fn observe(replay: &mut EventReplay<StreamRepository, CapturingEventHandler, MyAggregate>) -> Seen {
        let seen = Seen::default();
        let sink = seen.clone();
        replay.set_progress_observer(Box::new(move |progress| {
            sink.lock().unwrap().push((
                progress.aggregate_id.to_owned(),
                progress.sequence,
                progress.event_type.clone(),
                progress.replayed,
            ));
        }));
        seen
    }

    fn stream(ids_and_sequences: &[(&str, usize)]) -> StreamRepository {
        let items = ids_and_sequences
            .iter()
            .map(|(id, sequence)| {
                Ok(SerializedEvent::try_from(&EventEnvelope::<MyAggregate> {
                    aggregate_id: (*id).into(),
                    sequence: *sequence,
                    payload: MyEvents::SomethingWasDone,
                    metadata: HashMap::new(),
                })
                .unwrap())
            })
            .collect();
        StreamRepository {
            items: Mutex::new(items),
            fail_open: false,
        }
    }

    #[tokio::test]
    async fn progress_is_observed_for_every_handled_event_counting_from_one() {
        for all in [false, true] {
            let (handler, _) = CapturingEventHandler::new();
            let mut replay = EventReplay::new(stream(&[("a", 1), ("b", 1), ("a", 2)]), handler);
            let seen = observe(&mut replay);
            if all {
                replay.replay_all().await.unwrap();
            } else {
                replay.replay("a").await.unwrap();
            }
            let done = "SomethingWasDone".to_owned();
            assert_eq!(
                *seen.lock().unwrap(),
                [
                    ("a".into(), 1, done.clone(), 1),
                    ("b".into(), 1, done.clone(), 2),
                    ("a".into(), 2, done, 3)
                ]
            );
        }
    }

    #[tokio::test]
    async fn an_event_that_fails_to_decode_or_to_be_handled_is_not_reported() {
        let (mut handler, _) = CapturingEventHandler::new();
        handler.fail = Some((2, true));
        let mut replay = EventReplay::new(stream(&[("a", 1), ("a", 2), ("a", 3)]), handler);
        let seen = observe(&mut replay);
        assert!(replay.replay_all().await.is_err());
        assert_eq!(seen.lock().unwrap().iter().map(|e| e.1).collect::<Vec<_>>(), [1]);

        let mut items = stream(&[("a", 1), ("a", 2)]);
        if let Ok(event) = &mut items.items.get_mut().unwrap()[1] {
            event.payload = json!({"unknown": true});
        }
        let (handler, _) = CapturingEventHandler::new();
        let mut replay = EventReplay::new(items, handler);
        let seen = observe(&mut replay);
        assert!(matches!(
            replay.replay_all().await,
            Err(PersistenceError::DeserializationError(_))
        ));
        assert_eq!(seen.lock().unwrap().iter().map(|e| e.1).collect::<Vec<_>>(), [1]);
    }

    #[tokio::test(start_paused = true)]
    async fn the_pace_delays_every_event_and_a_zero_pace_never_sleeps() {
        let pace = std::time::Duration::from_millis(250);
        let (handler, seen) = CapturingEventHandler::new();
        let replay = EventReplay::new(stream(&[("a", 1), ("a", 2), ("a", 3)]), handler).with_pace(pace);
        let started = tokio::time::Instant::now();
        replay.replay_all().await.unwrap();
        assert!(started.elapsed() >= pace * 3);
        assert_eq!(seen.lock().unwrap().len(), 3);

        let (handler, _) = CapturingEventHandler::new();
        let replay = EventReplay::new(stream(&[("a", 1), ("a", 2)]), handler).with_pace(std::time::Duration::ZERO);
        let started = tokio::time::Instant::now();
        replay.replay_all().await.unwrap();
        assert_eq!(started.elapsed(), std::time::Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn upcasters_and_pace_survive_each_other_in_either_order() {
        fn upcast(_: Value) -> Value {
            json!({"SomethingWasDone": ()})
        }
        let legacy = || SerializedEvent {
            aggregate_id: AGGREGATE_ID.to_string(),
            sequence: 1,
            aggregate_type: "MyAggregate".to_string(),
            event_type: "SomethingWasDone".to_string(),
            event_version: "0.0.1".to_string(),
            payload: json!({"LegacyName": ()}),
            metadata: Object(Map::default()),
        };
        let pace = std::time::Duration::from_secs(1);
        for upcasters_first in [false, true] {
            let upcasters: Vec<Box<dyn crate::cqrs::store::upcaster::EventUpcaster>> = vec![Box::new(
                NumericVersionUpcaster::new("SomethingWasDone", "0.1.0", Box::new(upcast)),
            )];
            let (handler, seen) = CapturingEventHandler::new();
            let replay = EventReplay::new(MockRepo::with_events(Ok(vec![legacy()])), handler);
            let replay = if upcasters_first {
                replay.with_upcasters(upcasters).with_pace(pace)
            } else {
                replay.with_pace(pace).with_upcasters(upcasters)
            };
            let started = tokio::time::Instant::now();
            replay.replay_all().await.unwrap();
            assert!(started.elapsed() >= pace);
            assert_eq!(seen.lock().unwrap()[0].payload, MyEvents::SomethingWasDone);
        }
    }
}
