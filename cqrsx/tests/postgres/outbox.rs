//! Atomic publication and rollback of event/snapshot/task writes.
use super::*;
#[path = "outbox/concurrency.rs"]
mod concurrency;
#[path = "outbox/publication.rs"]
mod publication;
#[path = "outbox/rollback.rs"]
mod rollback;

use cqrsx::cqrs::store::snapshot::SerializedSnapshot;

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
