//! Decorator for replay: reports progress (and slows down in slow-motion mode) around the same
//! projection handlers that run live. Used by `RebuildService` (Phase 6).
use async_trait::async_trait;
use cqrsx::cqrs::{
    aggregate::Aggregate,
    event::{DomainEvent, EventEnvelope},
    event_handler::EventHandler,
    store::PersistenceError,
};
use std::time::Duration;

pub type ProgressCallback = dyn Fn(&str, usize, &str) + Send + Sync;

pub struct ProgressHandler<H> {
    inner: H,
    delay: Option<Duration>,
    on_event: Box<ProgressCallback>,
}

impl<H> ProgressHandler<H> {
    pub fn new(inner: H, delay: Option<Duration>, on_event: Box<ProgressCallback>) -> Self {
        Self { inner, delay, on_event }
    }
}

#[async_trait]
impl<A, H> EventHandler<A> for ProgressHandler<H>
where
    A: Aggregate,
    H: EventHandler<A>,
{
    async fn handle(&self, aggregate_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError> {
        if let Some(delay) = self.delay {
            tokio::time::sleep(delay).await;
        }
        self.inner.handle(aggregate_id, events).await?;
        for event in events {
            (self.on_event)(aggregate_id, event.sequence, &event.payload.event_type());
        }
        Ok(())
    }
}
