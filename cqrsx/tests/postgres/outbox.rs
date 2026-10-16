//! Atomic publication and rollback of event/snapshot/task writes.
use super::*;
#[path = "outbox/concurrency.rs"]
mod concurrency;
#[path = "outbox/publication.rs"]
mod publication;
#[path = "outbox/rollback.rs"]
mod rollback;

use cqrsx::cqrs::event::EventEnvelope;
use cqrsx::cqrs::store::snapshot::SerializedSnapshot;
use cqrsx::cqrs::store::stream::ReplayStream;

const AGGREGATE_ID: &str = "outbox-test-aggregate";

#[derive(Clone, Default, Serialize, Deserialize)]
struct StreamAggregate;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum TestEvent {
    Recorded { value: String },
}

impl DomainEvent for TestEvent {
    fn event_type(&self) -> String {
        "recorded".to_owned()
    }

    fn event_version(&self) -> String {
        "1.0".to_owned()
    }
}

impl Aggregate for StreamAggregate {
    const TYPE: &'static str = "transactional_outbox_test";

    type Command = ();
    type Event = TestEvent;
    type Error = std::convert::Infallible;
    type Services = ();

    async fn handle(
        &mut self,
        _command: Self::Command,
        _service: &Self::Services,
        _sink: &EventSink<Self>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn apply(&mut self, _event: Self::Event) {}
}

fn serialized_event(sequence: usize, value: &str) -> SerializedEvent {
    let event = TestEvent::Recorded { value: value.to_owned() };
    SerializedEvent::new(
        AGGREGATE_ID.to_owned(),
        sequence,
        StreamAggregate::TYPE.to_owned(),
        event.event_type(),
        event.event_version(),
        serde_json::to_value(event).expect("test event must serialize"),
        json!({ "correlation_id": format!("correlation-{sequence}") }),
    )
}

async fn collect_stream(stream: &mut ReplayStream) -> Vec<EventEnvelope<StreamAggregate>> {
    let mut events = Vec::new();
    while let Some(event) = stream.next::<StreamAggregate>(&[]).await {
        events.push(event.expect("event stream must not fail"));
    }
    events
}

fn stream_values(events: Vec<EventEnvelope<StreamAggregate>>) -> Vec<(String, usize, TestEvent)> {
    events
        .into_iter()
        .map(|event| (event.aggregate_id, event.sequence, event.payload))
        .collect()
}

fn expected_stream_values(events: &[SerializedEvent]) -> Vec<(String, usize, TestEvent)> {
    events
        .iter()
        .map(|event| {
            (
                event.aggregate_id.clone(),
                event.sequence,
                serde_json::from_value(event.payload.clone()).expect("test event must deserialize"),
            )
        })
        .collect()
}

async fn row_count(pool: &PgPool, table: &str) -> i64 {
    let query = match table {
        "events" => "SELECT COUNT(*) FROM events WHERE aggregate_type = $1 AND aggregate_id = $2",
        "transactional_outbox" => "SELECT COUNT(*) FROM transactional_outbox WHERE aggregate_type = $1 AND aggregate_id = $2",
        _ => panic!("unsupported test table: {table}"),
    };
    sqlx::query_scalar(query)
        .bind(StreamAggregate::TYPE)
        .bind(AGGREGATE_ID)
        .fetch_one(pool)
        .await
        .expect("row count must succeed")
}
