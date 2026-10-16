// Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
// Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
use crate::cqrs::{
    aggregate::Aggregate,
    event::EventEnvelope,
    store::{PersistenceError, serialized_event::SerializedEvent, upcaster::EventUpcaster},
};
use tokio::sync::mpsc::{Receiver, Sender};

/// Reads saved event history for one aggregate ID or all IDs of an aggregate type.
/// This finite asynchronous stream does not subscribe to future event commits.
pub struct ReplayStream {
    queue: Receiver<Result<SerializedEvent, PersistenceError>>,
}

impl ReplayStream {
    /// Creates a new `ReplayStream` that will buffer events up to the `queue_size`.
    pub fn new(queue_size: usize) -> (ReplayFeed, Self) {
        let (sender, queue) = tokio::sync::mpsc::channel(queue_size);
        (ReplayFeed { sender }, Self { queue })
    }

    /// Receive the next upcasted event or error in the stream, if no event is available this will block.
    pub async fn next<A: Aggregate>(
        &mut self,
        upcasters: &[Box<dyn EventUpcaster>],
    ) -> Option<Result<EventEnvelope<A>, PersistenceError>> {
        self.queue
            .recv()
            .await
            .map(|result| result.and_then(|event| event.decode(upcasters)))
    }
}

/// Used to send events to a `ReplayStream` for replaying events.
pub struct ReplayFeed {
    sender: Sender<Result<SerializedEvent, PersistenceError>>,
}

impl ReplayFeed {
    /// Wait until the consumer closes, so a producer can cancel pending I/O.
    pub async fn closed(&self) {
        self.sender.closed().await;
    }

    /// Push the next event onto the stream.
    pub async fn push(&mut self, result: Result<SerializedEvent, PersistenceError>) -> Result<(), PersistenceError> {
        self.sender.send(result).await?;
        Ok(())
    }
}

#[cfg(test)]
pub(in crate::cqrs) mod tests {
    // Derived from cqrs-es 0.5.0 (Apache-2.0), copyright upstream contributors.
    // Modified for cqrsx; see doc/components.md#upstream-attribution and licenses/.
    use crate::cqrs::{aggregate::Aggregate, event::DomainEvent, event_sink::EventSink};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
    pub(in crate::cqrs) enum MyEvents {
        SomethingWasDone,
        SomethingElseWasDone,
    }

    impl DomainEvent for MyEvents {
        fn event_type(&self) -> String {
            format!("{:?}", self)
        }
        fn event_version(&self) -> String {
            "0.1.0".to_string()
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(in crate::cqrs) enum MyCommands {
        DoSomething,
        BadCommand,
    }

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    pub(in crate::cqrs) struct MyAggregate;

    impl Aggregate for MyAggregate {
        const TYPE: &'static str = "MyAggregate";
        type Command = MyCommands;
        type Event = MyEvents;
        type Error = MyUserError;
        type Services = MyService;

        async fn handle(
            &mut self,
            command: Self::Command,
            _service: &Self::Services,
            sink: &EventSink<Self>,
        ) -> Result<(), Self::Error> {
            match command {
                MyCommands::DoSomething => {
                    sink.write(MyEvents::SomethingWasDone, self).await;
                    Ok(())
                }
                MyCommands::BadCommand => Err("the expected error message".into()),
            }
        }

        fn apply(&mut self, _event: Self::Event) {}
    }

    #[derive(Debug, PartialEq, Eq, thiserror::Error)]
    #[error("{0}")]
    pub(in crate::cqrs) struct MyUserError(pub String);

    impl From<&str> for MyUserError {
        fn from(msg: &str) -> Self {
            Self(msg.to_string())
        }
    }

    #[derive(Debug, Clone, Default)]
    pub(in crate::cqrs) struct MyService;

    use crate::cqrs::store::PersistenceError;
    use crate::cqrs::store::stream::ReplayStream;

    #[tokio::test]
    async fn test_replay_stream() {
        let (mut feed, mut stream) = ReplayStream::new(5);
        feed.push(Err(PersistenceError::OptimisticLockError)).await.unwrap();
        drop(feed);
        let found = stream.next::<MyAggregate>(&[]).await;
        assert!(
            matches!(found.unwrap().unwrap_err(), PersistenceError::OptimisticLockError),
            "expected optimistic lock error"
        );
    }
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    use crate::cqrs::store::persisted::tests::{TestAggregate, TestEvents, test_serialized_event};
    use std::time::Duration;

    #[tokio::test]
    async fn capacity_one_backpressures_and_drains_before_eof() {
        let (mut feed, mut stream) = ReplayStream::new(1);
        let first = test_serialized_event(1, TestEvents::Started);
        feed.push(Ok(first.clone())).await.unwrap();
        let second = test_serialized_event(2, TestEvents::SomethingWasDone);
        {
            let pending = feed.push(Ok(second.clone()));
            tokio::pin!(pending);
            // Polling a full channel must leave the second send pending.
            assert!(tokio::time::timeout(Duration::from_millis(10), &mut pending).await.is_err());
            let found = stream.next::<TestAggregate>(&[]).await.unwrap().unwrap();
            assert_eq!(SerializedEvent::try_from(&found).unwrap(), first);
            tokio::time::timeout(Duration::from_secs(1), pending).await.unwrap().unwrap();
        }
        drop(feed);
        let found = stream.next::<TestAggregate>(&[]).await.unwrap().unwrap();
        assert_eq!(SerializedEvent::try_from(&found).unwrap(), second);
        assert!(stream.next::<TestAggregate>(&[]).await.is_none());
    }

    #[tokio::test]
    async fn consumer_close_fails_pending_producer_and_empty_producer_closes_stream() {
        let (mut feed, stream) = ReplayStream::new(1);
        drop(stream);
        assert!(matches!(
            feed.push(Ok(test_serialized_event(1, TestEvents::Started))).await,
            Err(PersistenceError::UnknownError(_))
        ));
        let (feed, mut stream) = ReplayStream::new(1);
        drop(feed);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), stream.next::<TestAggregate>(&[]))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn errors_follow_delivered_events_and_do_not_hide_later_events() {
        let (mut feed, mut stream) = ReplayStream::new(4);
        feed.push(Ok(test_serialized_event(1, TestEvents::Started))).await.unwrap();
        let mut corrupt = test_serialized_event(2, TestEvents::Started);
        corrupt.metadata = serde_json::json!({"bad": 42});
        feed.push(Ok(corrupt)).await.unwrap();
        feed.push(Err(PersistenceError::ConnectionError("stream failed".into())))
            .await
            .unwrap();
        feed.push(Ok(test_serialized_event(3, TestEvents::SomethingWasDone)))
            .await
            .unwrap();
        drop(feed);
        assert_eq!(stream.next::<TestAggregate>(&[]).await.unwrap().unwrap().sequence, 1);
        assert!(matches!(
            stream.next::<TestAggregate>(&[]).await.unwrap(),
            Err(PersistenceError::DeserializationError(_))
        ));
        assert!(matches!(
            stream.next::<TestAggregate>(&[]).await.unwrap(),
            Err(PersistenceError::ConnectionError(_))
        ));
        assert_eq!(stream.next::<TestAggregate>(&[]).await.unwrap().unwrap().sequence, 3);
        assert!(stream.next::<TestAggregate>(&[]).await.is_none());
    }
}
