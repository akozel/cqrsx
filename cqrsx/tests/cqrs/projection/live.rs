//! Live delivery: pages, the window, failures of one key, poison events, concurrency.
use crate::projection::support::{Balance, Harness, POLL, RETRY_BASE, config, delta};
use crate::support::ledger::{Command, Ledger};
use cqrsx::cqrs::{
    event_handler::delivery::DeliveryConfig,
    projection::{checkpoint::CheckpointStore, processor::config::ProcessorConfig},
    store::{feed::Position, repository::PersistedEventRepository, serialized_event::SerializedEvent, upcaster::EventUpcaster},
};
use std::time::Duration;

/// Turns the version-0 delta nobody can read into a zero delta of the current version.
struct Heal;

impl EventUpcaster for Heal {
    fn can_upcast(&self, event_type: &str, event_version: &str) -> bool {
        event_type == "delta" && event_version == "0"
    }

    fn upcast(&self, mut event: SerializedEvent) -> SerializedEvent {
        event.payload = serde_json::json!(0);
        event.event_version = "1".into();
        event
    }
}

#[tokio::test(start_paused = true)]
async fn an_event_committed_before_the_processor_started_is_applied() {
    let h = Harness::new();
    h.commit("a", 5).await;
    h.commit("a", 7).await;
    let node = h.node("n1");
    let status = h.caught_up(&["a"]).await;
    assert_eq!(
        h.view("a").await.unwrap().0,
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
    assert_eq!(h.view("a").await.unwrap().0.history, [(1, 1)]);
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
    assert_eq!(h.view("a").await.unwrap().0.history, [(1, 1)], "written before the advance");
    assert_eq!(h.status().await.position, Position::START, "not advanced");
    node.crash();
    gate.release();
    let node = h.node("n2");
    h.caught_up(&["a", "b"]).await;
    let (view, context) = h.view("a").await.unwrap();
    assert_eq!((view.history, context.version), (vec![(1, 1)], 1), "no second write");
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
        assert_eq!(h.view("a").await.unwrap().0.history, [(1, 1), (2, 2)]);
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
    assert_eq!(h.view("c").await.unwrap().0.history, [(1, 1), (2, 2)]);
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
    let mut poison = delta("p", 1, 0);
    poison.event_version = "0".into();
    poison.payload = serde_json::json!({"unknown": true});
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
    assert_eq!(h.view("b").await.unwrap().0.history, [(1, 1)], "written by the sibling");
    sibling.stop().await;
    node.stop().await;
    let node = h.node_with(crate::projection::support::NAME, "n2", config(), upcaster());
    let status = h.caught_up(&["a", "b"]).await;
    assert_eq!((status.failures, status.last_error), (0, None));
    assert_eq!(h.view("p").await.unwrap().0.history, [(1, 0)], "the upcast event");
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
        h.view("b").await.unwrap().0.history,
        [(1, 1)],
        "the key's worker survived the panic"
    );
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn concurrency_bounds_the_keys_in_flight() {
    let h = Harness::with_config(config().with_concurrency(4).unwrap());
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
        ProcessorConfig::new(POLL, 1, crate::projection::support::LEASE, DeliveryConfig::default()).unwrap(),
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
    assert_eq!(h.view("a").await.unwrap().0.history, [(1, 1), (2, 2), (3, 3)]);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn the_window_is_bounded_by_the_buffer_capacity() {
    let h = Harness::with_config(
        ProcessorConfig::new(
            POLL,
            1000,
            crate::projection::support::LEASE,
            DeliveryConfig::new(3, 32).unwrap(),
        )
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
    assert_eq!(h.view("a").await.unwrap().0.history, [(1, 1), (2, 2), (3, 3)]);
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
async fn a_command_with_several_events_is_one_transaction_and_one_job() {
    let h = Harness::new();
    let node = h.node("n1");
    h.executor_execute("a", Command::Adjust(vec![1, 2, 3])).await;
    h.caught_up(&["a"]).await;
    assert_eq!(h.observer.count("applied:a:3"), 1);
    assert_eq!(h.status().await.applied, 3);
    node.stop().await;
}
