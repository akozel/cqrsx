//! Atomic publication and rollback of event/snapshot/task writes.
use super::*;
#[path = "outbox/concurrency.rs"]
mod concurrency;
#[path = "outbox/publication.rs"]
mod publication;
#[path = "outbox/rollback.rs"]
mod rollback;

use cqrsx::journal::snapshot::SerializedSnapshot;

const AGGREGATE_ID: &str = "outbox-test-aggregate";

#[derive(Clone, Default, Serialize, Deserialize)]
struct StreamAggregate;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum TestEvent {
    Recorded { value: String },
}

impl Events for TestEvent {
    fn event_type(&self) -> &'static str {
        "recorded"
    }

    fn event_version(&self) -> &'static str {
        "1.0"
    }
}

impl Aggregate for StreamAggregate {
    const TYPE: &'static str = "transactional_outbox_test";
    type Id = String;

    type Command = ();
    type Event = TestEvent;
    type Rejection = std::convert::Infallible;
    type Rules = ();

    fn apply(&mut self, _event: Self::Event) {}
}

impl Decide<StreamAggregate> for () {
    fn decide(self, _: &StreamAggregate, _: &()) -> Result<TestEvent, std::convert::Infallible> {
        Ok(TestEvent::Recorded { value: String::new() })
    }
}

fn serialized_event(sequence: usize, value: &str) -> SerializedEvent {
    let event = TestEvent::Recorded { value: value.to_owned() };
    SerializedEvent {
        aggregate_id: AGGREGATE_ID.to_owned(),
        sequence,
        aggregate_type: StreamAggregate::TYPE.to_owned(),
        event_type: event.event_type().to_owned(),
        event_version: event.event_version().to_owned(),
        payload: serde_json::to_value(event).expect("test event must serialize"),
        metadata: json!({ "correlation_id": format!("correlation-{sequence}") }),
    }
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
