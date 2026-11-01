//! Live delivery: pages, the window, failures of one key, poison events, concurrency.
use crate::projection::support::{Balance, Harness, Heal, POLL, RETRY_BASE, config, config_builder, delta, old_delta};
use crate::support::ledger::{Command, Ledger};
use cqrsx::{
    domain::event::EventEnvelope,
    journal::{feed::Position, repository::EventRepository, upcaster::EventUpcaster},
    memory::view_repository::MemoryViewRepository,
    projection::{
        checkpoint::CheckpointStore,
        processor::{
            ProjectionProcessor,
            config::{DeliveryLimits, ProcessorConfig},
        },
    },
    view::{View, projection::ViewProjection, repository::ViewRepository},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

const SHORT: Duration = Duration::from_millis(20);

#[tokio::test(start_paused = true)]
async fn an_event_committed_before_the_processor_started_is_applied() {
    let h = Harness::new();
    h.commit("a", 5).await;
    h.commit("a", 7).await;
    let node = h.node("n1");
    let status = h.caught_up(&["a"]).await;
    assert_eq!(
        h.view("a").await.unwrap().view,
        Balance {
            total: 12,
            history: vec![(1, 5), (2, 7)]
        }
    );
    assert_eq!((status.applied, status.owner.as_deref(), status.failures), (2, Some("n1"), 0));
    assert!(h.observer.count("applied:a:") >= 1 && h.observer.count("advanced:") >= 1);
    assert_eq!(h.observer.count("lease:Acquired"), 1);
    node.stop().await;
    assert_eq!(h.status().await.owner, None, "released on stop");
    assert_eq!(h.observer.count("lease:Released"), 1);
}

#[tokio::test(start_paused = true)]
async fn a_crash_while_reading_a_page_loses_nothing() {
    let h = Harness::new();
    h.commit("a", 1).await;
    let gate = h.journal.gate_feed();
    let node = h.node("n1");
    gate.reached().await;
    node.crash();
    gate.release(); // the crashed node is gone; the next node's reads pass
    assert!(h.view("a").await.is_none());
    assert_eq!(h.status().await.position, Position::START);
    let node = h.node("n2");
    let status = h.caught_up(&["a"]).await;
    assert_eq!(status.owner.as_deref(), Some("n2"), "after the lease of n1 ran out");
    assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1)]);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_crash_between_the_view_writes_and_the_advance_applies_nothing_twice() {
    let h = Harness::new();
    h.commit("a", 1).await;
    h.commit("b", 2).await;
    let gate = h.checkpoints.gate_before("advance");
    let node = h.node("n1");
    gate.reached().await;
    assert_eq!(
        h.view("a").await.unwrap().view.history,
        [(1, 1)],
        "written before the advance"
    );
    assert_eq!(h.status().await.position, Position::START, "not advanced");
    node.crash();
    gate.release();
    let node = h.node("n2");
    h.caught_up(&["a", "b"]).await;
    let stored = h.view("a").await.unwrap();
    assert_eq!((stored.view.history, stored.revision), (vec![(1, 1)], 1), "no second write");
    assert_eq!(h.views.writes(), 2);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_crash_during_the_advance_is_safe_either_way() {
    for after in [false, true] {
        let h = Harness::new();
        h.commit("a", 1).await;
        let gate = if after {
            h.checkpoints.gate_after("advance")
        } else {
            h.checkpoints.gate_before("advance")
        };
        let node = h.node("n1");
        gate.reached().await;
        assert_eq!(
            h.status().await.position != Position::START,
            after,
            "the effect depends on the moment"
        );
        node.crash();
        gate.release();
        h.commit("a", 2).await;
        let node = h.node("n2");
        let status = h.caught_up(&["a"]).await;
        assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1), (2, 2)]);
        assert_eq!(status.applied, 2, "a repeated event is skipped and not counted twice");
        node.stop().await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_failing_view_store_stalls_the_projection_before_the_first_failure_and_recovers() {
    let h = Harness::new();
    h.commit("a", 1).await;
    let node = h.node("n1");
    let before = h.caught_up(&["a"]).await.position;
    h.views.fail_next("*", 3);
    h.commit("b", 1).await;
    h.until(async || h.observer.count("failed:") == 3).await;
    let status = h.status().await;
    assert_eq!(
        (status.position, status.failures, status.last_error.as_deref()),
        (before, 3, Some("balances: connection"))
    );
    assert!(h.view("b").await.is_none());
    let failures = h.observer.timed("failed:");
    assert_eq!(failures[1] - failures[0], RETRY_BASE, "the first pause");
    assert_eq!(failures[2] - failures[1], RETRY_BASE * 2, "the pause doubles");
    assert_eq!(h.observer.count("lease:"), 1, "the lease was held throughout");
    let status = h.caught_up(&["a", "b"]).await;
    assert_eq!((status.failures, status.last_error), (0, None));
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_failure_of_one_key_moves_the_checkpoint_up_to_the_event_before_it() {
    let h = Harness::new();
    h.views.fail_next("c", 1);
    for key in ["a", "b", "c", "d", "e"] {
        h.commit(key, 1).await;
    }
    h.commit("c", 2).await;
    let node = h.node("n1");
    h.until(async || h.observer.count("failed:") == 1).await;
    let status = h.status().await;
    assert_eq!(status.position, Position::new(2), "the transaction before c's first");
    for key in ["a", "b", "d", "e"] {
        assert!(h.view(key).await.is_some(), "{key} was written in the same page");
    }
    assert!(h.view("c").await.is_none());
    assert_eq!(
        h.observer.events().iter().find(|event| event.starts_with("failed:")).unwrap(),
        "failed:balances:connection:c:1"
    );
    h.caught_up(&["a", "b", "c", "d", "e"]).await;
    assert_eq!(h.view("c").await.unwrap().view.history, [(1, 1), (2, 2)]);
    assert_eq!(h.views.writes(), 5, "the repeated events of a, b, d, e were skipped");
    assert_eq!(
        h.observer.count("rebuilt:"),
        0,
        "a gap right after a failure of the key is no reason to rebuild"
    );
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_poison_event_stalls_only_its_projection_until_an_upcaster_arrives() {
    let h = Harness::new();
    h.commit("a", 1).await;
    let poison = old_delta("p", 1);
    h.journal
        .inner
        .persist::<Ledger>(std::slice::from_ref(&poison), None)
        .await
        .unwrap();
    h.commit("b", 1).await;
    let node = h.node("n1");
    h.until(async || h.observer.count("failed:") >= 1).await;
    let status = h.status().await;
    assert_eq!(status.position, Position::new(1), "stops before the poison transaction");
    assert_eq!(status.last_error.as_deref(), Some("journal: deserialization"));
    assert_eq!(
        h.observer.events().iter().find(|event| event.starts_with("failed:")).unwrap(),
        "failed:journal:deserialization:p:1"
    );
    assert!(h.view("a").await.is_some() && h.view("b").await.is_none());
    // A sibling projection with an upcaster reads the same journal and goes past it.
    let upcaster = || -> Vec<Box<dyn EventUpcaster>> { vec![Box::new(Heal)] };
    let sibling = h.node_with("sibling", "n1", config(), upcaster());
    h.until(async || {
        h.checkpoints
            .status(Some("sibling"))
            .await
            .unwrap()
            .first()
            .is_some_and(|status| status.position == Position::new(3))
    })
    .await;
    assert_eq!(h.view("b").await.unwrap().view.history, [(1, 1)], "written by the sibling");
    sibling.stop().await;
    node.stop().await;
    let node = h.node_with(crate::projection::support::NAME, "n2", config(), upcaster());
    let status = h.caught_up(&["a", "b"]).await;
    assert_eq!((status.failures, status.last_error), (0, None));
    assert_eq!(h.view("p").await.unwrap().view.history, [(1, 0)], "the upcast event");
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_panic_in_the_handler_is_a_failure_of_its_key_only() {
    let h = Harness::new();
    h.views.panic_once("b");
    for key in ["a", "b", "c"] {
        h.commit(key, 1).await;
    }
    let node = h.node("n1");
    h.until(async || h.observer.count("failed:") == 1).await;
    assert_eq!(
        h.observer.events().iter().find(|event| event.starts_with("failed:")).unwrap(),
        "failed:balances:panic:b:1"
    );
    assert_eq!(h.status().await.position, Position::new(1));
    assert!(h.view("a").await.is_some() && h.view("c").await.is_some() && h.view("b").await.is_none());
    h.caught_up(&["a", "b", "c"]).await;
    assert_eq!(
        h.view("b").await.unwrap().view.history,
        [(1, 1)],
        "the key's lane survived the panic"
    );
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn concurrency_bounds_the_keys_in_flight() {
    let h = Harness::with_config(config_builder().write_concurrency(4).build().unwrap());
    let keys: Vec<String> = (0..50).map(|n| format!("k{n:02}")).collect();
    for key in &keys {
        h.journal.inner.persist::<Ledger>(&[delta(key, 1, 1)], None).await.unwrap();
    }
    let gate = h.views.gate("*");
    let node = h.node("n1");
    gate.reached().await;
    h.run_for(POLL).await;
    assert_eq!(h.views.most_in_flight(), 4, "the rest wait for a permit, not at the gate");
    gate.release();
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    h.caught_up(&refs).await;
    assert_eq!((h.views.writes(), h.views.most_in_flight()), (50, 4));
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_page_smaller_than_a_transaction_is_delivered_whole() {
    let h = Harness::with_config(
        ProcessorConfig::builder()
            .poll_interval(POLL)
            .page_size(1)
            .lease(crate::projection::support::LEASE)
            .delivery_limits(DeliveryLimits::default())
            .build()
            .unwrap(),
    );
    h.journal
        .inner
        .persist::<Ledger>(&[delta("a", 1, 1), delta("a", 2, 2), delta("a", 3, 3)], None)
        .await
        .unwrap();
    h.commit("b", 1).await;
    let node = h.node("n1");
    h.caught_up(&["a", "b"]).await;
    assert_eq!(h.observer.count("applied:a:3"), 1, "one job with the whole transaction");
    assert_eq!(h.views.writes(), 2);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn queued_pages_of_one_key_are_handled_in_one_call() {
    let h = Harness::new();
    let gate = h.views.gate("a");
    let node = h.node("n1");
    h.commit("a", 1).await;
    gate.reached().await;
    h.commit("a", 2).await;
    h.run_for(Duration::from_millis(10)).await;
    h.commit("a", 3).await;
    h.run_for(Duration::from_millis(10)).await;
    gate.release();
    h.caught_up(&["a"]).await;
    assert_eq!(h.observer.count("applied:a:"), 3, "one receipt per page");
    assert_eq!(h.views.writes(), 2, "the two queued pages were one handler call");
    assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1), (2, 2), (3, 3)]);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_window_is_bounded_by_the_buffer_capacity() {
    let h = Harness::with_config(
        ProcessorConfig::builder()
            .poll_interval(POLL)
            .page_size(1000)
            .lease(crate::projection::support::LEASE)
            .delivery_limits(DeliveryLimits::try_new(3, 32).unwrap())
            .build()
            .unwrap(),
    );
    let gate = h.views.gate("a");
    let node = h.node("n1");
    h.commit("a", 1).await;
    gate.reached().await; // page 1 is stuck in a's worker
    h.commit("a", 2).await;
    h.run_for(Duration::from_millis(10)).await; // page 2 is read: the window is full now
    h.commit("a", 3).await;
    h.commit("b", 1).await;
    h.run_for(POLL * 3).await;
    assert!(
        h.view("b").await.is_none(),
        "the third page is not read while the window is full"
    );
    assert_eq!(h.observer.count("applied:"), 0);
    gate.release();
    h.caught_up(&["a", "b"]).await;
    assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1), (2, 2), (3, 3)]);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn waiting_distinguishes_the_horizon_from_an_idle_journal() {
    let h = Harness::new();
    h.commit("a", 1).await;
    let node = h.node("n1");
    h.caught_up(&["a"]).await;
    h.until(async || h.observer.count("waiting:Idle") >= 1).await;
    h.journal.hold_horizon(1);
    h.until(async || h.observer.count("waiting:Horizon") >= 1).await;
    assert_eq!(h.observer.count("failed:"), 0);
    h.journal.hold_horizon(0);
    h.journal.fail("head", 1);
    h.observer.clear();
    h.until(async || h.observer.count("waiting:Idle") >= 1).await;
    assert_eq!(h.observer.count("failed:"), 0, "a failed head check is no round failure");
    assert_eq!(h.status().await.failures, 0);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_commit_wakes_an_idle_processor_before_the_poll() {
    let h = Harness::new();
    let node = h.node("n1");
    h.run_for(POLL * 2).await;
    let before = tokio::time::Instant::now();
    h.commit("a", 1).await;
    h.until(async || h.observer.count("applied:a:1") == 1).await;
    let applied = h.observer.timed("applied:a:1")[0];
    assert!(applied - before < POLL, "woken by the commit, not by the poll");
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_commit_with_several_events_is_one_transaction_and_one_job() {
    let h = Harness::new();
    let node = h.node("n1");
    h.commit_together("a", &[1, 2, 3]).await;
    h.caught_up(&["a"]).await;
    assert_eq!(h.observer.count("applied:a:3"), 1);
    assert_eq!(h.status().await.applied, 3);
    node.stop().await;
}

/// The reads the journal saw while `work` ran, over virtual time shorter than the poll.
async fn reads_during(h: &Harness, work: impl AsyncFnOnce()) -> usize {
    let before = h.journal.feed_reads();
    work().await;
    h.run_for(POLL / 5).await; // long enough to settle, short of the poll
    h.journal.feed_reads() - before
}

#[tokio::test(start_paused = true)]
async fn a_commit_costs_one_read_and_neither_the_end_of_the_page_nor_its_receipt_adds_another() {
    let h = Harness::new();
    let node = h.node("n1");
    h.run_for(POLL * 2).await;
    let reads = reads_during(&h, async || {
        h.commit("a", 1).await;
        h.until(async || h.observer.count("advanced:") >= 1).await;
    })
    .await;
    assert_eq!(
        reads, 1,
        "a page short of its size is the end of the journal; a receipt is no reason to read"
    );
    assert_eq!(h.view("a").await.unwrap().view.total, 1);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_full_page_is_followed_at_once_by_the_next_read_and_the_last_short_page_ends_them() {
    let config = ProcessorConfig::builder()
        .poll_interval(POLL)
        .page_size(2)
        .lease(Duration::from_secs(1))
        .delivery_limits(DeliveryLimits::default())
        .build()
        .unwrap();
    let h = Harness::with_config(config);
    for delta in 1..=5 {
        h.commit("a", delta).await;
    }
    let node = h.node("n1");
    let reads = reads_during(&h, async || {
        h.caught_up(&["a"]).await;
    })
    .await;
    assert_eq!(reads, 3, "pages of 2, 2 and 1 events: the short one is the last");
    assert_eq!(h.view("a").await.unwrap().view.total, 15);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_settle_time_gathers_the_commits_of_a_burst_into_one_page() {
    let burst = async |h: &Harness| {
        for delta in 1..=5 {
            h.commit("a", delta).await;
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    };
    let eager = Harness::new();
    let node = eager.node("n1");
    eager.run_for(POLL * 2).await;
    let eager_reads = reads_during(&eager, async || burst(&eager).await).await;
    assert_eq!(eager.view("a").await.unwrap().view.total, 15);
    node.stop().await;

    let patient = Harness::with_config(config_builder().wake_delay(SHORT).build().unwrap());
    let node = patient.node("n1");
    patient.run_for(POLL * 2).await;
    let patient_reads = reads_during(&patient, async || burst(&patient).await).await;
    assert_eq!(
        patient.view("a").await.unwrap().view.total,
        15,
        "the settle delays the reads, it loses nothing"
    );
    assert_eq!(patient_reads, 1, "five commits within the settle time are one page");
    assert!(
        eager_reads > patient_reads,
        "without it every commit is read on its own ({eager_reads} reads)"
    );
    assert_eq!(patient.observer.count("applied:a:"), 1);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_settling_owner_stops_on_shutdown_and_still_sees_a_commit_made_while_it_settles() {
    let h = Harness::with_config(config_builder().wake_delay(SHORT * 4).build().unwrap());
    let node = h.node("n1");
    h.run_for(POLL * 2).await;
    h.commit("a", 1).await;
    tokio::time::sleep(SHORT).await; // the owner has a failure due now
    h.commit("a", 2).await;
    h.caught_up(&["a"]).await;
    assert_eq!(h.view("a").await.unwrap().view.total, 3);
    h.commit("a", 4).await;
    tokio::time::sleep(SHORT).await;
    node.stop().await; // the stop interrupts the settle and releases the row
    assert_eq!(h.status().await.owner, None);
}

/// The `source` of every event a document saw, in order: what the events carried next to their payload.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Sources(Vec<String>);

impl View<Ledger> for Sources {
    const NAME: &'static str = "sources";
    fn apply(&mut self, event: &EventEnvelope<Ledger>) {
        self.0.push(event.metadata.get("source").cloned().unwrap_or_default());
    }
}

#[tokio::test(start_paused = true)]
async fn the_metadata_of_an_event_reaches_the_projection() {
    let h = Harness::new();
    let sources = Arc::new(MemoryViewRepository::<Sources, Ledger>::default());
    let processor = ProjectionProcessor::new(
        "sources",
        h.journal.clone(),
        h.checkpoints.clone(),
        Arc::new(ViewProjection::new(sources.clone())),
        config(),
    )
    .with_commit_wakeup(h.wakeup.clone());
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(processor.run(shutdown.clone()));
    let metadata = HashMap::from([("source".to_owned(), "box office".to_owned())]);
    h.execute_with_metadata("a", Command::Adjust(1), metadata.clone()).await;
    h.execute_with_metadata("a", Command::Adjust(2), metadata).await;
    h.commit("a", 3).await;
    h.until(async || sources.load("a").await.unwrap().is_some_and(|seen| seen.0.len() == 3))
        .await;
    assert_eq!(sources.load("a").await.unwrap().unwrap().0, ["box office", "box office", ""]);
    shutdown.cancel();
    running.await.unwrap();
}
