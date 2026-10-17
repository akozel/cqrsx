//! Decorator around a projection handler: publishes `projection.updated` after success and
//! `projection.failed` on error, so the UI can show "the read model caught up" (or did not). The
//! projection is reported under the name of the wrapped handler, which is also the name the
//! library reports when the handler fails after a commit.
use crate::application::activity::ActivityHub;
use async_trait::async_trait;
use cqrsx::cqrs::{aggregate::Aggregate, event::EventEnvelope, event_handler::EventHandler, store::PersistenceError};
use serde_json::json;
use std::sync::Arc;

pub struct NotifyingHandler<H> {
    inner: H,
    activity: Arc<ActivityHub>,
}

impl<H> NotifyingHandler<H> {
    pub fn new(inner: H, activity: Arc<ActivityHub>) -> Self {
        Self { inner, activity }
    }
}

#[async_trait]
impl<A: Aggregate, H: EventHandler<A>> EventHandler<A> for NotifyingHandler<H> {
    fn name(&self) -> String {
        self.inner.name()
    }

    async fn handle(&self, aggregate_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError> {
        let result = self.inner.handle(aggregate_id, events).await;
        let projection = self.inner.name();
        let sequence = events.last().map(|e| e.sequence).unwrap_or_default();
        match &result {
            Ok(()) => {
                self.activity.publish(
                    "projection.updated",
                    json!({ "projection": projection, "view_id": aggregate_id, "sequence": sequence }),
                );
            }
            Err(error) => {
                self.activity.publish(
                    "projection.failed",
                    json!({ "projection": projection, "view_id": aggregate_id, "sequence": sequence, "message": error.to_string() }),
                );
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        ConcertId,
        fan::{Fan, FanEvent, events::TicketCancelled},
        shared::venue::SeatId,
    };
    use serde_json::Value;
    use uuid::Uuid;

    /// A read model that succeeds or fails, under the collection name the activity log shows.
    struct Wallets {
        fail: bool,
    }

    #[async_trait]
    impl EventHandler<Fan> for Wallets {
        fn name(&self) -> String {
            "fan_wallets".into()
        }

        async fn handle(&self, _: &str, _: &[EventEnvelope<Fan>]) -> Result<(), PersistenceError> {
            if self.fail {
                Err(PersistenceError::OptimisticLockError)
            } else {
                Ok(())
            }
        }
    }

    fn cancelled(sequence: usize) -> EventEnvelope<Fan> {
        EventEnvelope {
            aggregate_id: "fox".into(),
            sequence,
            payload: FanEvent::TicketCancelled(TicketCancelled {
                booking_id: Uuid::nil(),
                cancellation_id: Uuid::nil(),
                concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
                seat: SeatId::parse("B4").unwrap(),
                refund: 80,
            }),
            metadata: Default::default(),
        }
    }

    #[tokio::test]
    async fn reports_the_wrapped_projection_by_its_name_after_success_and_failure() {
        let activity = Arc::new(ActivityHub::new(8, 8));
        for fail in [false, true] {
            let handler = NotifyingHandler::new(Wallets { fail }, activity.clone());
            assert_eq!(handler.name(), "fan_wallets");
            assert_eq!(handler.handle("fox", &[cancelled(2), cancelled(3)]).await.is_err(), fail);
        }
        let entries = activity.subscribe().0;
        let seen: Vec<(&str, &Value, &Value, &Value)> = entries
            .iter()
            .map(|e| {
                (
                    e.kind.as_str(),
                    &e.fields["projection"],
                    &e.fields["view_id"],
                    &e.fields["sequence"],
                )
            })
            .collect();
        assert_eq!(
            seen,
            [
                ("projection.updated", &json!("fan_wallets"), &json!("fox"), &json!(3)),
                ("projection.failed", &json!("fan_wallets"), &json!("fox"), &json!(3)),
            ]
        );
        assert_eq!(entries[1].fields["message"], "optimistic lock error");
    }
}
