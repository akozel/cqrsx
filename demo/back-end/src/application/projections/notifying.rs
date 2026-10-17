//! Decorator around a projection handler: publishes `projection.updated` after success and
//! `projection.failed` on error, so the UI can show "the read model caught up" (or did not).
use crate::application::activity::ActivityHub;
use async_trait::async_trait;
use cqrsx::cqrs::{aggregate::Aggregate, event::EventEnvelope, event_handler::EventHandler, store::PersistenceError};
use serde_json::json;
use std::sync::Arc;

pub struct NotifyingHandler<H> {
    inner: H,
    projection: &'static str,
    activity: Arc<ActivityHub>,
}

impl<H> NotifyingHandler<H> {
    pub fn new(inner: H, projection: &'static str, activity: Arc<ActivityHub>) -> Self {
        Self {
            inner,
            projection,
            activity,
        }
    }
}

#[async_trait]
impl<A: Aggregate, H: EventHandler<A>> EventHandler<A> for NotifyingHandler<H> {
    async fn handle(&self, aggregate_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError> {
        let result = self.inner.handle(aggregate_id, events).await;
        let sequence = events.last().map(|e| e.sequence).unwrap_or_default();
        match &result {
            Ok(()) => {
                self.activity.publish(
                    "projection.updated",
                    json!({ "projection": self.projection, "view_id": aggregate_id, "sequence": sequence }),
                );
            }
            Err(error) => {
                self.activity.publish(
                    "projection.failed",
                    json!({ "projection": self.projection, "view_id": aggregate_id, "sequence": sequence, "message": error.to_string() }),
                );
            }
        }
        result
    }
}
