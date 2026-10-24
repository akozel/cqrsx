//! How committed events reach an event handler: through a bounded buffer per aggregate key, handled in
//! commit order by a worker of that key, in batches.
//!
//! An executor keeps its handlers in registration order, each behind buffers of its own (the private
//! `ordered` module): the command hands its events over while it holds its aggregate's lock (the lock is
//! what puts the hand-overs of one key in commit order), releases the lock and only then waits for the
//! handlers' outcomes. The handlers of one command run side by side, each in its own buffer, not one after
//! another. Public are the bounds of a delivery ([`DeliveryConfig`]) and the [`DeliveryBacklog`], which
//! counts what was handed over and not handled yet, so maintenance and shutdown can wait for it.
mod ordered;

use crate::cqrs::{aggregate::Aggregate, event::EventEnvelope, event_handler::EventHandler};
#[allow(unused_imports)] // the projection processor uses the whole set
pub(crate) use ordered::{DeliveryError, DeliveryReceipt, DeliverySlot, Done, OrderedDelivery, Rebuilder};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use thiserror::Error;
use tokio::sync::Notify;

/// How many commands of one aggregate key may wait in one handler's buffer unless configured otherwise.
pub const DEFAULT_DELIVERY_CAPACITY: NonZeroUsize = NonZeroUsize::new(64).unwrap();
/// How many commands one handler call takes at most unless configured otherwise.
pub const DEFAULT_DELIVERY_MAX_BATCH: NonZeroUsize = NonZeroUsize::new(32).unwrap();

/// Checked bounds of a delivery: the buffer per aggregate key and the largest batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeliveryConfig {
    capacity: NonZeroUsize,
    max_batch: NonZeroUsize,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum DeliveryConfigError {
    #[error("the delivery buffer capacity must be positive")]
    ZeroCapacity,
    #[error("the largest delivery batch must be positive")]
    ZeroMaxBatch,
}

impl DeliveryConfig {
    pub fn new(capacity: usize, max_batch: usize) -> Result<Self, DeliveryConfigError> {
        Ok(Self {
            capacity: NonZeroUsize::new(capacity).ok_or(DeliveryConfigError::ZeroCapacity)?,
            max_batch: NonZeroUsize::new(max_batch).ok_or(DeliveryConfigError::ZeroMaxBatch)?,
        })
    }

    /// How many commands of one key may be reserved, queued or in the handler at once. The next command
    /// of the key waits for space, so a slow handler slows its key down instead of growing memory.
    pub fn capacity(&self) -> usize {
        self.capacity.get()
    }

    /// How many queued commands one handler call takes at most. A batch is taken from what has already
    /// gathered and never waits for more; `1` hands every command over on its own.
    pub fn max_batch(&self) -> usize {
        self.max_batch.get()
    }
}

impl Default for DeliveryConfig {
    /// [`DEFAULT_DELIVERY_CAPACITY`] and [`DEFAULT_DELIVERY_MAX_BATCH`].
    fn default() -> Self {
        Self {
            capacity: DEFAULT_DELIVERY_CAPACITY,
            max_batch: DEFAULT_DELIVERY_MAX_BATCH,
        }
    }
}

/// The events handed over to the handlers and not handled yet; a cheap handle, clones share the count.
/// Every hand-over counts until its handler call has finished or the worker holding it is gone.
#[derive(Clone, Debug, Default)]
pub struct DeliveryBacklog(Arc<Backlog>);

#[derive(Debug, Default)]
struct Backlog {
    pending: AtomicUsize,
    idle: Notify,
}

impl DeliveryBacklog {
    /// Hand-overs not handled yet.
    pub fn len(&self) -> usize {
        self.0.pending.load(Ordering::SeqCst)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Resolves once everything handed over so far has been handled, at once when nothing is pending.
    /// Hand-overs that start meanwhile are waited for too, so stop them first (e.g. with a lock) when the
    /// wait must end.
    pub async fn drained(&self) {
        loop {
            let mut notified = std::pin::pin!(self.0.idle.notified());
            // Registered before the check, so a hand-over finishing in between still wakes this waiter.
            notified.as_mut().enable();
            if self.is_empty() {
                return;
            }
            notified.await;
        }
    }

    /// Counts one hand-over until the returned guard is dropped.
    pub(crate) fn pending(&self) -> PendingJob {
        self.0.pending.fetch_add(1, Ordering::SeqCst);
        PendingJob(self.0.clone())
    }
}

/// One hand-over counted in a [`DeliveryBacklog`]; dropping it, handled or not, uncounts it.
pub(crate) struct PendingJob(Arc<Backlog>);

impl Drop for PendingJob {
    fn drop(&mut self) {
        if self.0.pending.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

/// The event handlers of one executor in registration order, each behind buffers of its own; all count
/// their hand-overs in the executor's one backlog.
pub(crate) struct RegisteredHandlers<A: Aggregate> {
    entries: Vec<OrderedDelivery<A>>,
    backlog: DeliveryBacklog,
}

impl<A: Aggregate> Default for RegisteredHandlers<A> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            backlog: DeliveryBacklog::default(),
        }
    }
}

impl<A: Aggregate + 'static> RegisteredHandlers<A> {
    /// Appends a handler; its position is its index in post-commit failures.
    pub(crate) fn push(&mut self, handler: Arc<dyn EventHandler<A>>, config: DeliveryConfig) {
        self.entries
            .push(OrderedDelivery::with_backlog(handler, config, self.backlog.clone()));
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn backlog(&self) -> DeliveryBacklog {
        self.backlog.clone()
    }

    pub(crate) fn name(&self, position: usize) -> String {
        self.entries[position].name()
    }

    /// Reserves space in the key's buffer of every handler, in registration order. Ready at once without
    /// handlers or while the buffers have room.
    pub(crate) async fn reserve(&self, aggregate_id: &str) -> Reserved<A> {
        let mut slots = Vec::new();
        for (position, delivery) in self.entries.iter().enumerate() {
            slots.push((position, delivery.reserve(aggregate_id).await));
        }
        Reserved { slots }
    }
}

/// Space reserved for one command in the buffers of the handlers, before the command commits.
pub(crate) struct Reserved<A: Aggregate> {
    slots: Vec<(usize, DeliverySlot<A>)>,
}

impl<A: Aggregate + 'static> Reserved<A> {
    /// Hands `events` to every reserved buffer without waiting; the receipts carry registration positions.
    pub(crate) fn deliver(self, events: &Arc<[EventEnvelope<A>]>) -> Vec<(usize, DeliveryReceipt)> {
        self.slots
            .into_iter()
            .map(|(position, slot)| (position, slot.deliver(events.clone())))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_rejects_zeros_and_defaults_use_the_constants() {
        assert_eq!(DeliveryConfig::new(0, 1), Err(DeliveryConfigError::ZeroCapacity));
        assert_eq!(DeliveryConfig::new(1, 0), Err(DeliveryConfigError::ZeroMaxBatch));
        let config = DeliveryConfig::new(3, 2).unwrap();
        assert_eq!((config.capacity(), config.max_batch()), (3, 2));
        let defaults = DeliveryConfig::default();
        assert_eq!(
            (defaults.capacity(), defaults.max_batch()),
            (DEFAULT_DELIVERY_CAPACITY.get(), DEFAULT_DELIVERY_MAX_BATCH.get())
        );
        assert_eq!((defaults.capacity(), defaults.max_batch()), (64, 32));
        assert_eq!(
            DeliveryConfigError::ZeroCapacity.to_string(),
            "the delivery buffer capacity must be positive"
        );
        assert_eq!(
            DeliveryConfigError::ZeroMaxBatch.to_string(),
            "the largest delivery batch must be positive"
        );
    }

    #[tokio::test]
    async fn drained_returns_at_once_when_idle_and_after_the_last_pending_hand_over_otherwise() {
        let backlog = DeliveryBacklog::default();
        backlog.drained().await;
        let (first, second) = (backlog.pending(), backlog.clone().pending());
        assert_eq!((backlog.len(), backlog.is_empty()), (2, false));
        let mut drained = std::pin::pin!(backlog.drained());
        let pending = |drained: std::pin::Pin<&mut _>| {
            std::future::Future::poll(drained, &mut std::task::Context::from_waker(std::task::Waker::noop())).is_pending()
        };
        assert!(pending(drained.as_mut()));
        drop(first);
        assert!(pending(drained.as_mut()), "one hand-over is still pending");
        drop(second);
        assert!(!pending(drained.as_mut()));
        assert!(backlog.is_empty());
    }
}
