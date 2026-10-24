//! What a projection processor reports while it runs: for status pages, event feeds and tests. Every
//! method has an empty body, so an observer implements what it needs. Reports with borrowed strings come
//! by reference, the small `Copy` events by value.
use crate::cqrs::store::feed::Position;
use std::time::Duration;

/// Observes one processor. Called on the processor's task, so an observer returns quickly and never
/// panics: a panic stops the processor.
pub trait ProjectionObserver: Send + Sync {
    /// The handler accepted a live batch of one key.
    fn applied(&self, report: &Applied<'_>) {
        let _ = report;
    }

    /// The checkpoint moved.
    fn advanced(&self, report: &Advanced) {
        let _ = report;
    }

    /// A round failed and the processor backs off.
    fn failed(&self, report: &Failed<'_>) {
        let _ = report;
    }

    /// The document of one key was rebuilt from its history.
    fn rebuilt(&self, report: &Rebuilt<'_>) {
        let _ = report;
    }

    /// A rebuild of the whole journal changed state.
    fn rebuild(&self, event: RebuildEvent) {
        let _ = event;
    }

    /// The lease of the checkpoint changed hands.
    fn lease(&self, event: LeaseEvent) {
        let _ = event;
    }

    /// The journal had nothing new for the processor.
    fn waiting(&self, reason: Waiting) {
        let _ = reason;
    }
}

/// A live batch of one key was applied (or skipped by the contiguity rule, which the processor cannot tell apart).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Applied<'a> {
    pub aggregate_id: &'a str,
    /// The last event sequence of the batch.
    pub last_sequence: usize,
    /// Events in the batch.
    pub events: usize,
    /// The last position of the page the batch came from.
    pub position: Position,
}

/// The checkpoint moved to `position`; `applied` events were delivered by the pages behind the move.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Advanced {
    pub position: Position,
    pub applied: u64,
}

/// A round failed: a handler for one key, or the journal or the checkpoint store without a key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failed<'a> {
    pub aggregate_id: Option<&'a str>,
    /// The handler's name, or `journal` / `checkpoints`.
    pub handler: &'a str,
    /// A fixed word: `connection`, `deserialization`, `optimistic_lock`, `sequence_gap`, `unknown`, `panic`,
    /// `stopped`, `storage`, `corrupt`, `invalid`.
    pub category: &'static str,
    /// Where the processor stands: the page's position for a handler failure, the checkpoint otherwise.
    pub position: Position,
    /// Failed rounds in a row, this one included.
    pub failures: u32,
}

/// Why a key was rebuilt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RebuildReason {
    /// A rebuild of the journal or a request for this key.
    Requested,
    /// The document refused a live event as a gap.
    Healed,
}

/// The document of `aggregate_id` was replaced by the fold of its `events` events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rebuilt<'a> {
    pub aggregate_id: &'a str,
    pub events: usize,
    pub reason: RebuildReason,
}

/// A rebuild of the whole journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RebuildEvent {
    Started {
        pace: Duration,
    },
    /// A page of keys is recorded; `keys` counts since the start.
    Progress {
        keys: u64,
    },
    Finished {
        keys: u64,
        elapsed: Duration,
    },
    /// The owner stopped or lost the lease before the end; the next owner continues from the cursor.
    Interrupted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseEvent {
    Acquired,
    Lost,
    Released,
}

/// Why a poll found nothing to apply.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Waiting {
    /// The journal's head is above what the processor has read, but the page is empty: a slower
    /// transaction with a lower id still runs and holds the horizon.
    Horizon,
    /// Nothing new was written.
    Idle,
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Silent;
    impl ProjectionObserver for Silent {}

    #[test]
    fn the_default_observer_ignores_every_report() {
        let observer: Box<dyn ProjectionObserver> = Box::new(Silent);
        observer.applied(&Applied {
            aggregate_id: "k",
            last_sequence: 3,
            events: 2,
            position: Position::new(5),
        });
        observer.advanced(&Advanced {
            position: Position::new(5),
            applied: 2,
        });
        observer.failed(&Failed {
            aggregate_id: None,
            handler: "journal",
            category: "connection",
            position: Position::START,
            failures: 1,
        });
        observer.rebuilt(&Rebuilt {
            aggregate_id: "k",
            events: 9,
            reason: RebuildReason::Healed,
        });
        for event in [
            RebuildEvent::Started { pace: Duration::ZERO },
            RebuildEvent::Progress { keys: 1 },
            RebuildEvent::Finished {
                keys: 1,
                elapsed: Duration::ZERO,
            },
            RebuildEvent::Interrupted,
        ] {
            observer.rebuild(event);
        }
        for event in [LeaseEvent::Acquired, LeaseEvent::Lost, LeaseEvent::Released] {
            observer.lease(event);
        }
        observer.waiting(Waiting::Horizon);
        observer.waiting(Waiting::Idle);
        assert_ne!(RebuildReason::Requested, RebuildReason::Healed);
    }
}
