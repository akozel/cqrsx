//! Rebuilds: the whole journal by cursor, one key on request, a document healed after a gap, next to live delivery.
use crate::projection::support::{Harness, Heal, NAME, POLL, RETRY_BASE, config, delta, key, old_delta};
use crate::support::ledger::Ledger;
use cqrsx::cqrs::{
    projection::{
        checkpoint::{CheckpointStore, RebuildRequest},
        processor::config::{DeliveryLimits, ProcessorConfig},
    },
    store::repository::PersistedEventRepository,
};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

async fn versions(h: &Harness, keys: &[&str]) -> Vec<i64> {
    let mut versions = Vec::new();
    for key in keys {
        versions.push(h.view(key).await.map_or(0, |stored| stored.version));
    }
    versions
}

#[tokio::test(start_paused = true)]
async fn a_stale_rebuild_never_rolls_a_document_back() {
    let h = Harness::new();
    for value in 1..=3 {
        h.commit("a", value).await;
    }
    let a_store = h.fork_checkpoints();
    let a = h.node_isolated("A", a_store.clone());
    h.caught_up(&["a"]).await;
    let read = h.journal.gate_after_history();
    h.checkpoints.request_rebuild_key(&key(), "a").await.unwrap();
    read.reached().await; // A holds a history of three events
    a_store.fail("extend", usize::MAX);
    a_store.fail("claim", usize::MAX);
    let b = h.node("B");
    h.until(async || h.status().await.owner.as_deref() == Some("B")).await;
    h.commit("a", 4).await;
    h.until(async || h.view("a").await.is_some_and(|stored| stored.view.history.len() == 4))
        .await;
    read.release(); // A's rebuild goes on with its stale history
    h.run_for(POLL * 4).await;
    assert_eq!(
        h.view("a").await.unwrap().view.history,
        [(1, 1), (2, 2), (3, 3), (4, 4)],
        "never rolled back"
    );
    assert!(
        h.observer.events().iter().all(|event| event != "rebuilt:a:Requested:3"),
        "{:#?}",
        h.observer.events()
    );
    b.stop().await;
    a.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_rebuild_under_concurrent_commits_ends_in_the_fold_of_the_journal() {
    let h = Arc::new(Harness::new());
    let keys = ["a", "b", "c", "d", "e"];
    for key in keys {
        for value in 1..=4 {
            h.commit(key, value).await;
        }
    }
    let node = h.node("n1");
    h.caught_up(&keys).await;
    assert_eq!(
        h.checkpoints.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::Accepted
    );
    let commits = tokio::spawn({
        let h = h.clone();
        async move {
            for value in 5..=8 {
                for key in keys {
                    h.commit(key, value).await;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        }
    });
    commits.await.unwrap();
    h.until(async || {
        let status = h.status().await;
        status.rebuild_cursor.is_none() && status.rebuilt == 5
    })
    .await;
    h.caught_up(&keys).await;
    for key in keys {
        assert_eq!(
            h.view(key).await.unwrap().view.history,
            (1..=8).map(|n| (n, n as i32)).collect::<Vec<_>>()
        );
    }
    assert_eq!(
        (h.observer.count("rebuild:Started"), h.observer.count("rebuild:Finished")),
        (1, 1)
    );
    assert!(h.observer.count("rebuild:Progress") >= 1);
    assert_eq!((h.observer.count("rebuilt:"), h.observer.count("failed:")), (5, 0));
    let events = h.observer.events();
    let positions: Vec<&String> = events.iter().filter(|event| event.starts_with("advanced:")).collect();
    assert!(
        positions
            .windows(2)
            .all(|pair| pair[0] <= pair[1] || pair[0].len() < pair[1].len()),
        "monotonic"
    );
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_rebuild_request_reaches_an_owner_in_backoff() {
    let h = Harness::new();
    h.views.fail_next("a", 2);
    let node = h.node("n1");
    h.commit("a", 1).await;
    h.until(async || h.observer.count("failed:") == 1).await;
    let requested = Instant::now();
    assert_eq!(
        h.checkpoints.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::Accepted
    );
    h.until(async || h.observer.count("rebuild:Started") == 1).await;
    let started = h.observer.timed("rebuild:Started")[0];
    assert!(
        started - requested <= RETRY_BASE * 2 + POLL,
        "picked up after the pause: {:?}",
        started - requested
    );
    h.caught_up(&["a"]).await;
    h.until(async || h.observer.count("rebuild:Finished") == 1).await;
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_crash_during_a_rebuild_continues_from_the_cursor() {
    let h = Harness::with_config(config().with_rebuild_page(2).unwrap());
    let keys = ["a", "b", "c", "d", "e"];
    for key in keys {
        h.commit(key, 1).await;
        h.commit(key, 2).await;
    }
    let node = h.node("n1");
    h.caught_up(&keys).await;
    let before = versions(&h, &keys).await;
    let gate = h.checkpoints.gate_before("advance_rebuild");
    assert_eq!(
        h.checkpoints.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::Accepted
    );
    gate.reached().await; // the first page of keys is rebuilt and about to be recorded
    let during = versions(&h, &keys).await;
    assert_eq!(during, [before[0] + 1, before[1] + 1, before[2], before[3], before[4]]);
    node.crash();
    gate.release();
    let node = h.node("n2");
    h.until(async || {
        let status = h.status().await;
        status.rebuild_cursor.is_none() && status.rebuilt == 5
    })
    .await;
    let after = versions(&h, &keys).await;
    assert_eq!(
        after,
        [before[0] + 2, before[1] + 2, before[2] + 1, before[3] + 1, before[4] + 1],
        "the unrecorded page was repeated, the rest rebuilt once"
    );
    h.caught_up(&keys).await;
    assert_eq!(h.observer.count("rebuild:Finished"), 1);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_live_event_during_its_key_rebuild_is_applied_exactly_once() {
    let h = Harness::new();
    for value in 1..=3 {
        h.commit("a", value).await;
    }
    let node = h.node("n1");
    h.caught_up(&["a"]).await;
    let gate = h.views.gate_first("a");
    h.checkpoints.request_rebuild_key(&key(), "a").await.unwrap();
    gate.reached().await; // the rebuild of a holds its write
    h.commit("a", 4).await; // queued behind the rebuild in a's worker
    h.run_for(POLL * 2).await;
    assert_eq!(
        h.view("a").await.unwrap().view.history.len(),
        3,
        "the live event waits for the rebuild"
    );
    gate.release();
    h.caught_up(&["a"]).await;
    assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1), (2, 2), (3, 3), (4, 4)]);
    assert_eq!(
        (h.observer.count("rebuilt:a:Requested:3"), h.observer.count("applied:a:4")),
        (1, 1)
    );
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_second_rebuild_request_is_refused_while_one_runs() {
    let h = Harness::new();
    for key in ["a", "b"] {
        h.commit(key, 1).await;
    }
    let node = h.node("n1");
    h.caught_up(&["a", "b"]).await;
    let gate = h.checkpoints.gate_before("advance_rebuild");
    assert_eq!(
        h.checkpoints.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::Accepted
    );
    gate.reached().await;
    assert_eq!(
        h.checkpoints.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::AlreadyRunning
    );
    h.checkpoints.request_rebuild_key(&key(), "a").await.unwrap();
    gate.release();
    h.until(async || h.observer.count("rebuild:Finished") == 1).await;
    assert_eq!(
        h.observer.count("rebuild:Started"),
        1,
        "the first rebuild was not interrupted"
    );
    h.until(async || h.observer.count("rebuilt:a:") == 2).await;
    assert_eq!(
        h.checkpoints.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::Accepted
    );
    h.until(async || h.observer.count("rebuild:Finished") == 2).await;
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_single_key_rebuild_touches_nothing_else() {
    let h = Harness::new();
    for key in ["a", "b", "c"] {
        h.commit(key, 1).await;
        h.commit(key, 2).await;
    }
    let node = h.node("n1");
    let before = h.caught_up(&["a", "b", "c"]).await;
    let versions_before = versions(&h, &["a", "b", "c"]).await;
    h.checkpoints.request_rebuild_key(&key(), "b").await.unwrap();
    h.until(async || h.observer.count("rebuilt:b:Requested:2") == 1).await;
    assert_eq!(
        versions(&h, &["a", "b", "c"]).await,
        [versions_before[0], versions_before[1] + 1, versions_before[2]]
    );
    assert_eq!(h.view("b").await.unwrap().view.history, [(1, 1), (2, 2)]);
    let after = h.status().await;
    assert_eq!(
        (after.position, after.rebuild_cursor, after.rebuilt),
        (before.position, None, 0)
    );
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_gap_heals_itself_from_the_journal_without_stalling_the_projection() {
    let h = Harness::new();
    for key in ["a", "b"] {
        h.commit(key, 1).await;
        h.commit(key, 2).await;
    }
    let node = h.node("n1");
    let before = h.caught_up(&["a", "b"]).await;
    h.views.delete("a").await; // the wipe
    let gate = h.views.gate_first("a"); // holds the healing rebuild's write, to look around meanwhile
    h.commit("a", 3).await;
    gate.reached().await;
    h.commit("b", 3).await;
    h.commit("c", 1).await;
    h.until(async || h.view("b").await.is_some_and(|stored| stored.view.history.len() == 3) && h.view("c").await.is_some())
        .await;
    let during = h.status().await;
    assert_eq!(
        (during.position, during.failures),
        (before.position, 0),
        "the page waits for the heal; no failure"
    );
    assert_eq!(h.observer.count("failed:"), 0);
    gate.release();
    let after = h.caught_up(&["a", "b", "c"]).await;
    assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1), (2, 2), (3, 3)]);
    assert_eq!((h.observer.count("rebuilt:a:Healed:3"), after.failures), (1, 0));
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn an_empty_journal_finishes_a_rebuild_at_once() {
    let h = Harness::new();
    let node = h.node("n1");
    h.run_for(POLL).await;
    node.stop().await;
    assert_eq!(
        h.checkpoints.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::Accepted,
        "waits for the next owner"
    );
    let node = h.node("n2");
    h.until(async || h.observer.count("rebuild:Finished") == 1).await;
    assert_eq!(h.observer.count("rebuild:Progress"), 0);
    assert!(
        h.observer
            .events()
            .iter()
            .any(|event| event.starts_with("rebuild:Finished { keys: 0")),
        "{:#?}",
        h.observer.events()
    );
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_paced_rebuild_of_one_key_does_not_hold_the_other_keys_back() {
    let h = Harness::with_config(
        ProcessorConfig::try_new(
            POLL,
            1000,
            crate::projection::support::LEASE,
            DeliveryLimits::try_new(3, 32).unwrap(),
        )
        .unwrap(),
    );
    let history: Vec<_> = (1..=200).map(|sequence| delta("slow", sequence, 1)).collect();
    h.journal.inner.persist::<Ledger>(&history, None).await.unwrap();
    h.commit("fast", 1).await;
    let node = h.node("n1");
    h.caught_up(&["fast", "slow"]).await;
    let pace = Duration::from_millis(80);
    assert_eq!(
        h.checkpoints.request_rebuild(&key(), pace).await.unwrap(),
        RebuildRequest::Accepted
    );
    h.until(async || h.observer.count("rebuild:Started") == 1).await;
    for value in 2..=5 {
        h.commit("fast", value).await;
        h.until(async || {
            h.view("fast")
                .await
                .is_some_and(|stored| stored.view.history.len() == value as usize)
        })
        .await;
    }
    // Pages with the slow key fill the window; then the reading stops until the slow key is done.
    h.commit("slow", 1).await;
    h.run_for(Duration::from_millis(10)).await;
    h.commit("slow", 1).await;
    h.run_for(Duration::from_millis(10)).await;
    h.commit("fast", 6).await;
    h.run_for(POLL * 3).await;
    assert_eq!(
        h.view("fast").await.unwrap().view.history.len(),
        5,
        "the window is full: the boundary of the decision"
    );
    h.until(async || h.observer.count("rebuild:Finished") == 1).await;
    h.caught_up(&["fast", "slow"]).await;
    assert_eq!(h.view("slow").await.unwrap().view.history.len(), 202);
    assert_eq!(h.view("fast").await.unwrap().view.history.len(), 6);
    assert_eq!(h.observer.count("rebuilt:slow:Requested:200"), 1);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_requested_key_without_history_is_dropped_without_a_write() {
    let h = Harness::new();
    let node = h.node("n1");
    h.run_for(POLL).await;
    h.checkpoints.request_rebuild_key(&key(), "ghost").await.unwrap();
    h.until(async || h.observer.count("rebuilt:ghost:Requested:0") == 1).await;
    assert_eq!(h.views.writes(), 0);
    assert!(h.view("ghost").await.is_none());
    assert_eq!((h.status().await.failures, h.observer.count("failed:")), (0, 0));
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_rebuild_reads_old_events_through_the_upcasters() {
    let h = Harness::new();
    // Written straight to the journal: the command path cannot load the old event either.
    for event in [old_delta("a", 1), delta("a", 2, 5)] {
        h.journal.inner.persist::<Ledger>(&[event], None).await.unwrap();
    }
    let node = h.node_with(NAME, "n1", config(), vec![Box::new(Heal)]);
    h.until(async || h.view("a").await.is_some_and(|stored| stored.view.history.len() == 2))
        .await;
    let before = h.view("a").await.unwrap();
    h.checkpoints.request_rebuild_key(&key(), "a").await.unwrap();
    h.until(async || h.observer.count("rebuilt:a:Requested:2") == 1).await;
    let after = h.view("a").await.unwrap();
    assert_eq!(
        after.view.history,
        [(1, 0), (2, 5)],
        "the old event folds as the upcaster reads it"
    );
    assert_eq!((after.version, h.observer.count("failed:")), (before.version + 1, 0));
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_failed_history_read_is_retried_and_the_rebuild_finishes() {
    let h = Harness::new();
    h.commit("a", 1).await;
    h.commit("a", 2).await;
    let node = h.node("n1");
    h.caught_up(&["a"]).await;
    h.journal.fail("history", 1);
    h.checkpoints.request_rebuild_key(&key(), "a").await.unwrap();
    h.until(async || h.observer.count("rebuilt:a:Requested:2") == 1).await;
    assert_eq!(h.observer.count("failed:"), 1, "{:?}", h.observer.events());
    assert!(
        h.observer
            .events()
            .iter()
            .any(|event| event.starts_with("failed:") && event.contains(":connection:a:")),
        "{:?}",
        h.observer.events()
    );
    assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1), (2, 2)]);
    assert_eq!(h.status().await.failures, 1, "until the checkpoint moves again");
    h.commit("a", 3).await;
    assert_eq!(h.caught_up(&["a"]).await.failures, 0);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_rebuild_step_after_the_lease_is_lost_ends_the_ownership_and_the_new_owner_finishes_the_rebuild() {
    for step in ["begin_rebuild", "advance_rebuild", "finish_rebuild"] {
        let h = Harness::new();
        h.commit("a", 1).await;
        let a_store = h.fork_checkpoints();
        let a = h.node_isolated("A", a_store.clone());
        h.caught_up(&["a"]).await;
        a_store.fail("extend", usize::MAX); // A loses touch with the store: its lease runs out
        a_store.fail("claim", usize::MAX); // and never gets the row back
        let gate = a_store.gate_before(step);
        assert_eq!(
            h.checkpoints.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
            RebuildRequest::Accepted
        );
        gate.reached().await; // A is about to take this step of the rebuild
        let b = h.node("B");
        h.until(async || h.status().await.owner.as_deref() == Some("B")).await;
        gate.release(); // the step reaches the store with A's token, which is stale now
        h.until(async || h.observer.count("lease:Lost") == 1).await;
        h.until(async || h.observer.count("rebuild:Finished") >= 1).await;
        assert!(
            h.status().await.rebuild_cursor.is_none() && h.status().await.owner.as_deref() == Some("B"),
            "{step}: the rebuild is finished by B, who holds the row"
        );
        assert_eq!(
            h.view("a").await.unwrap().view.history,
            [(1, 1)],
            "{step}: the document is the fold of the journal"
        );
        b.stop().await;
        a.stop().await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_failed_key_listing_is_a_failure_of_the_round_and_the_rebuild_finishes_after_the_pause() {
    let h = Harness::new();
    h.commit("a", 1).await;
    h.commit("b", 2).await;
    let node = h.node("n1");
    h.caught_up(&["a", "b"]).await;
    h.journal.fail("ids", 1);
    h.checkpoints.request_rebuild(&key(), Duration::ZERO).await.unwrap();
    h.until(async || h.observer.count("rebuild:Finished") == 1).await;
    assert!(
        h.observer
            .events()
            .iter()
            .any(|event| event.starts_with("failed:") && event.contains(":connection:-:1")),
        "{:?}",
        h.observer.events()
    );
    assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1)]);
    assert_eq!(h.view("b").await.unwrap().view.history, [(1, 2)]);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_key_requested_while_its_live_job_runs_is_rebuilt_behind_the_job() {
    let h = Harness::new();
    h.commit("a", 1).await;
    let node = h.node("n1");
    h.caught_up(&["a"]).await;
    let gate = h.views.gate("a");
    h.commit("a", 2).await;
    gate.reached().await; // the live job of a:2 is in flight
    h.checkpoints.request_rebuild_key(&key(), "a").await.unwrap();
    h.run_for(POLL * 4).await; // the owner has seen the request and has to keep it waiting
    assert_eq!(h.observer.count("rebuilt:a:Requested"), 0, "not while the live job runs");
    gate.release();
    h.until(async || h.observer.count("rebuilt:a:Requested") == 1).await;
    assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1), (2, 2)]);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_key_requested_again_while_its_rebuild_runs_ends_in_the_fold_of_the_journal() {
    let h = Harness::new();
    h.commit("a", 1).await;
    h.commit("a", 2).await;
    let node = h.node("n1");
    h.caught_up(&["a"]).await;
    let read = h.journal.gate_after_history();
    h.checkpoints.request_rebuild_key(&key(), "a").await.unwrap();
    read.reached().await; // the first rebuild of `a` is in flight
    h.checkpoints.request_rebuild_key(&key(), "a").await.unwrap();
    h.run_for(POLL * 4).await; // the owner has seen the second request and keeps it waiting
    read.release();
    h.until(async || h.observer.count("rebuilt:a:Requested") >= 1).await;
    h.run_for(POLL * 4).await;
    assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1), (2, 2)]);
    assert_eq!(h.observer.count("failed:"), 0, "{:?}", h.observer.events());
    h.commit("a", 3).await;
    h.caught_up(&["a"]).await;
    assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1), (2, 2), (3, 3)]);
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_failing_checkpoint_statement_of_a_rebuild_is_retried_and_the_rebuild_finishes() {
    for statement in ["take_requests", "begin_rebuild", "advance_rebuild", "finish_rebuild"] {
        let h = Harness::new();
        h.commit("a", 1).await;
        let node = h.node("n1");
        h.caught_up(&["a"]).await;
        h.checkpoints.fail(statement, 1);
        h.checkpoints.request_rebuild(&key(), Duration::ZERO).await.unwrap();
        h.until(async || h.observer.count("rebuild:Finished") == 1).await;
        assert!(
            h.checkpoints.calls(statement) >= 2,
            "{statement} is called again after its failure"
        );
        assert!(
            h.observer.events().iter().any(|event| event.starts_with("failed:")),
            "{statement}: the failure is reported: {:?}",
            h.observer.events()
        );
        assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1)], "{statement}");
        node.stop().await;
    }
}
