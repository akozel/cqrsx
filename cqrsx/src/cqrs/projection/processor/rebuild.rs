//! The rebuild of one key from its history, and the bound on the documents a processor writes at once.
use crate::{
    cqrs::{
        event_handler::EventHandler,
        projection::{RebuildableProjection, processor::delivery::Rebuilder},
    },
    diagnostics,
    domain::{aggregate::Aggregate, event::EventEnvelope},
    journal::{
        PersistenceError, repository::PersistedEventRepository, serialized_event::deserialize_events, upcaster::EventUpcaster,
    },
};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio_util::sync::CancellationToken;

/// How often a rebuild re-reads a history the document has already left behind before it gives up.
pub(super) const REBUILD_READS: usize = 3;

/// A permit to write a document. Nothing closes the semaphore; a closed one is a bug and fails the write.
async fn permit(permits: &Semaphore) -> Result<SemaphorePermit<'_>, PersistenceError> {
    permits.acquire().await.map_err(|_| {
        diagnostics::log_broken_invariant("projection", "the concurrency semaphore of a processor is never closed");
        PersistenceError::UnknownError("the concurrency semaphore of the projection processor is closed".into())
    })
}

/// The projection behind the processor's concurrency bound: every live write takes one of `permits`.
pub(super) struct Bounded<A: Aggregate> {
    projection: Arc<dyn RebuildableProjection<A>>,
    permits: Arc<Semaphore>,
}

impl<A: Aggregate> Bounded<A> {
    pub(super) fn new(projection: Arc<dyn RebuildableProjection<A>>, permits: Arc<Semaphore>) -> Self {
        Self { projection, permits }
    }
}

#[async_trait]
impl<A: Aggregate + 'static> EventHandler<A> for Bounded<A> {
    fn name(&self) -> String {
        self.projection.name()
    }

    async fn handle(&self, aggregate_id: &str, events: &[EventEnvelope<A>]) -> Result<(), PersistenceError> {
        let _permit = permit(&self.permits).await?;
        self.projection.handle(aggregate_id, events).await
    }
}

/// Rebuilds a key: reads its whole history with the processor's upcasters, replaces the document with the
/// fold (one write without a pace; with a pace the first event replaces the document and the rest apply one
/// by one, a pause before each) and reports how many events the history had. A document newer than the
/// history read means the read is stale: the history is read again, up to [`REBUILD_READS`] times. A pause
/// holds no write permit, and `stop` ends the pauses when the owner is gone.
pub(super) struct HistoryRebuilder<A: Aggregate, R> {
    repository: Arc<R>,
    upcasters: Arc<Vec<Box<dyn EventUpcaster>>>,
    projection: Arc<dyn RebuildableProjection<A>>,
    permits: Arc<Semaphore>,
}

impl<A: Aggregate, R> HistoryRebuilder<A, R> {
    pub(super) fn new(
        repository: Arc<R>,
        upcasters: Arc<Vec<Box<dyn EventUpcaster>>>,
        projection: Arc<dyn RebuildableProjection<A>>,
        permits: Arc<Semaphore>,
    ) -> Self {
        Self {
            repository,
            upcasters,
            projection,
            permits,
        }
    }
}

#[async_trait]
impl<A: Aggregate + 'static, R: PersistedEventRepository + 'static> Rebuilder for HistoryRebuilder<A, R> {
    async fn rebuild(&self, aggregate_id: &str, pace: Duration, stop: CancellationToken) -> Result<usize, PersistenceError> {
        let mut reads = 0;
        let history: Vec<EventEnvelope<A>> = loop {
            reads += 1;
            let history = deserialize_events::<A>(self.repository.events::<A>(aggregate_id).await?, self.upcasters.as_slice())?;
            if history.is_empty() {
                return Ok(0);
            }
            let prefix = if pace.is_zero() { history.len() } else { 1 };
            let written = {
                let _permit = permit(&self.permits).await?;
                self.projection.rebuild(aggregate_id, &history, prefix).await
            };
            match written {
                Err(PersistenceError::OptimisticLockError) if reads < REBUILD_READS => continue, // newer than the read
                Err(error) => return Err(error),
                Ok(()) => break history,
            }
        };
        if !pace.is_zero() {
            let after_first = history.iter().skip(1);
            for event in after_first {
                if stop.is_cancelled() {
                    return Err(PersistenceError::UnknownError(
                        "rebuild interrupted: the owner stopped".into(),
                    ));
                }
                tokio::time::sleep(pace).await;
                let _permit = permit(&self.permits).await?;
                self.projection.handle(aggregate_id, std::slice::from_ref(event)).await?;
            }
        }
        Ok(history.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cqrs::{
            projection::{View, ViewProjection, repository::ViewRepository},
            testing::{mem_event_repository::MemEventRepository, mem_view_repository::MemViewRepository},
        },
        domain::aggregate::tests::{Delta, Ledger},
        journal::serialized_event::SerializedEvent,
    };
    use serde::{Deserialize, Serialize};
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use tokio::time::Instant;

    #[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
    struct Sequences(Vec<usize>);

    impl View<Ledger> for Sequences {
        const NAME: &'static str = "sequences";
        fn update(&mut self, event: &EventEnvelope<Ledger>) {
            self.0.push(event.sequence);
        }
    }

    type Views = MemViewRepository<Sequences, Ledger>;

    fn delta(id: &str, sequence: usize) -> SerializedEvent {
        SerializedEvent::try_from(&EventEnvelope::<Ledger> {
            aggregate_id: id.into(),
            sequence,
            payload: Delta(1),
            metadata: HashMap::new(),
        })
        .unwrap()
    }

    async fn journal_with(id: &str, events: usize) -> MemEventRepository {
        let journal = MemEventRepository::default();
        let history: Vec<SerializedEvent> = (1..=events).map(|sequence| delta(id, sequence)).collect();
        journal.persist::<Ledger>(&history, None).await.unwrap();
        journal
    }

    /// Refuses the first `conflicts` rebuild writes as if the document were newer than the history read,
    /// and counts the rebuild calls.
    struct Stale {
        inner: ViewProjection<Views, Sequences, Ledger>,
        conflicts: AtomicUsize,
        calls: AtomicUsize,
    }

    #[async_trait]
    impl EventHandler<Ledger> for Stale {
        async fn handle(&self, aggregate_id: &str, events: &[EventEnvelope<Ledger>]) -> Result<(), PersistenceError> {
            self.inner.handle(aggregate_id, events).await
        }
    }

    #[async_trait]
    impl RebuildableProjection<Ledger> for Stale {
        async fn rebuild(
            &self,
            aggregate_id: &str,
            history: &[EventEnvelope<Ledger>],
            prefix: usize,
        ) -> Result<(), PersistenceError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self
                .conflicts
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
            {
                return Err(PersistenceError::OptimisticLockError);
            }
            self.inner.rebuild(aggregate_id, history, prefix).await
        }
    }

    fn rebuilder(
        journal: &MemEventRepository,
        projection: Arc<dyn RebuildableProjection<Ledger>>,
        permits: usize,
    ) -> (HistoryRebuilder<Ledger, MemEventRepository>, Arc<Semaphore>) {
        let permits = Arc::new(Semaphore::new(permits));
        (
            HistoryRebuilder::new(Arc::new(journal.clone()), Arc::new(Vec::new()), projection, permits.clone()),
            permits,
        )
    }

    #[tokio::test]
    async fn a_rebuild_without_a_pace_writes_the_whole_fold_once_and_an_empty_history_nothing() {
        let views = Views::default();
        let journal = journal_with("a", 4).await;
        let projection: Arc<dyn RebuildableProjection<Ledger>> = Arc::new(ViewProjection::new(Arc::new(views.clone())));
        let (rebuilder, _) = rebuilder(&journal, projection, 2);
        assert_eq!(
            rebuilder
                .rebuild("a", Duration::ZERO, CancellationToken::new())
                .await
                .unwrap(),
            4
        );
        let stored = views.load_with_context("a").await.unwrap().unwrap();
        assert_eq!(
            (stored.view, stored.version, stored.last_sequence),
            (Sequences(vec![1, 2, 3, 4]), 1, 4)
        );
        assert_eq!(
            rebuilder
                .rebuild("missing", Duration::ZERO, CancellationToken::new())
                .await
                .unwrap(),
            0
        );
        assert!(views.load("missing").await.unwrap().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn a_paced_rebuild_replaces_the_document_with_the_first_event_and_applies_the_rest_one_by_one() {
        let views = Views::default();
        views.put("a", 5, 3, &Sequences(vec![1, 2, 3])).unwrap();
        let journal = journal_with("a", 3).await;
        let projection: Arc<dyn RebuildableProjection<Ledger>> = Arc::new(ViewProjection::new(Arc::new(views.clone())));
        let (rebuilder, permits) = rebuilder(&journal, projection.clone(), 1);
        let pace = Duration::from_millis(80);
        let started = Instant::now();
        let running = tokio::spawn({
            let views = views.clone();
            let bounded = Bounded::new(projection, permits);
            async move {
                // Another key's live write goes through while the paced rebuild sleeps: the pause holds no permit.
                tokio::time::sleep(Duration::from_millis(40)).await;
                bounded
                    .handle(
                        "b",
                        &[EventEnvelope {
                            aggregate_id: "b".into(),
                            sequence: 1,
                            payload: Delta(1),
                            metadata: HashMap::new(),
                        }],
                    )
                    .await
                    .unwrap();
                let stored = views.load_with_context("a").await.unwrap().unwrap();
                assert_eq!(
                    (stored.view, stored.version, stored.last_sequence),
                    (Sequences(vec![1]), 6, 1)
                );
                assert_eq!(bounded.name(), Sequences::NAME);
            }
        });
        assert_eq!(rebuilder.rebuild("a", pace, CancellationToken::new()).await.unwrap(), 3);
        assert_eq!(started.elapsed(), pace * 2, "a pause before each event after the first");
        running.await.unwrap();
        let stored = views.load_with_context("a").await.unwrap().unwrap();
        assert_eq!(
            (stored.view, stored.version, stored.last_sequence),
            (Sequences(vec![1, 2, 3]), 8, 3)
        );
        assert_eq!(views.load("b").await.unwrap(), Some(Sequences(vec![1])));
    }

    #[tokio::test(start_paused = true)]
    async fn a_stopped_owner_ends_a_paced_rebuild_before_its_next_pause() {
        let views = Views::default();
        let journal = journal_with("a", 3).await;
        let projection: Arc<dyn RebuildableProjection<Ledger>> = Arc::new(ViewProjection::new(Arc::new(views.clone())));
        let (rebuilder, _) = rebuilder(&journal, projection, 1);
        let stop = CancellationToken::new();
        stop.cancel();
        let error = rebuilder.rebuild("a", Duration::from_millis(80), stop).await.unwrap_err();
        assert!(matches!(error, PersistenceError::UnknownError(_)));
        assert_eq!(error.to_string(), "rebuild interrupted: the owner stopped");
        let stored = views.load_with_context("a").await.unwrap().unwrap();
        assert_eq!(
            (stored.view, stored.last_sequence),
            (Sequences(vec![1]), 1),
            "the first event was written"
        );
    }

    #[tokio::test]
    async fn a_document_newer_than_the_history_makes_the_rebuild_read_again_up_to_three_times() {
        let views = Views::default();
        let journal = journal_with("a", 2).await;
        for (conflicts, expected_calls, ok) in [(1, 2, true), (2, 3, true), (3, 3, false)] {
            let stale = Arc::new(Stale {
                inner: ViewProjection::new(Arc::new(views.clone())),
                conflicts: AtomicUsize::new(conflicts),
                calls: AtomicUsize::new(0),
            });
            let (rebuilder, _) = rebuilder(&journal, stale.clone(), 1);
            let result = rebuilder.rebuild("a", Duration::ZERO, CancellationToken::new()).await;
            assert_eq!(result.is_ok(), ok, "{conflicts} conflicts");
            assert_eq!(stale.calls.load(Ordering::SeqCst), expected_calls);
        }
        assert!(matches!(
            rebuilder(
                &journal,
                Arc::new(Stale {
                    inner: ViewProjection::new(Arc::new(views.clone())),
                    conflicts: AtomicUsize::new(3),
                    calls: AtomicUsize::new(0),
                }),
                1
            )
            .0
            .rebuild("a", Duration::ZERO, CancellationToken::new())
            .await,
            Err(PersistenceError::OptimisticLockError)
        ));
        assert_eq!(views.load("a").await.unwrap(), Some(Sequences(vec![1, 2])));
    }

    #[tokio::test]
    async fn a_closed_concurrency_semaphore_fails_the_write_and_logs_the_bug() {
        let views = Views::default();
        let journal = journal_with("a", 2).await;
        let projection: Arc<dyn RebuildableProjection<Ledger>> = Arc::new(ViewProjection::new(Arc::new(views.clone())));
        let (rebuilder, permits) = rebuilder(&journal, projection.clone(), 1);
        permits.close();
        let bounded = Bounded::new(projection, permits);
        let ((rebuilt, handled), log) = crate::diagnostics::test_support::capture(async {
            (
                rebuilder.rebuild("a", Duration::ZERO, CancellationToken::new()).await,
                bounded.handle("a", &[]).await,
            )
        })
        .await;
        assert!(matches!(rebuilt, Err(PersistenceError::UnknownError(_))));
        assert!(matches!(handled, Err(PersistenceError::UnknownError(_))));
        assert_eq!(
            log.matches("the concurrency semaphore of a processor is never closed")
                .count(),
            2,
            "{log}"
        );
        assert!(views.load("a").await.unwrap().is_none(), "nothing was written");
    }
}
