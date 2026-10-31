//! Ownership: one owner, hand-over on a stop, a crash or a lost lease, stale owners, store outages.
use crate::projection::support::{Harness, LEASE, POLL, key};
use cqrsx::journal::feed::Position;
use std::time::Duration;
use tokio::time::Instant;

#[tokio::test(start_paused = true)]
async fn a_stale_owner_cannot_damage_the_view_or_the_checkpoint() {
    let h = Harness::new();
    h.commit("a", 1).await;
    let a_store = h.fork_checkpoints();
    let a = h.node_isolated("A", a_store.clone());
    h.caught_up(&["a"]).await;
    a_store.fail("extend", usize::MAX); // A loses touch with the store: its lease runs out
    a_store.fail("claim", usize::MAX); // and never gets the row back
    let gate = h.views.gate("a");
    h.commit("a", 2).await;
    gate.reached().await; // A holds the write of a:2 in its hands
    let b = h.node("B");
    h.until(async || h.status().await.owner.as_deref() == Some("B")).await;
    h.until(async || h.observer.count("lease:Lost") == 1).await;
    gate.release(); // the stale write lands now, next to B's work
    let status = h.caught_up(&["a"]).await;
    assert_eq!(
        h.view("a").await.unwrap().view.history,
        [(1, 1), (2, 2)],
        "applied exactly once"
    );
    assert_eq!((status.owner.as_deref(), status.position), (Some("B"), h.head().await));
    assert_eq!(
        a_store.calls("advance"),
        1,
        "A advanced once, for a:1; its stale job moved nothing"
    );
    b.stop().await;
    a.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_lost_store_is_retried_and_the_claim_is_renewed() {
    let h = Harness::new();
    h.commit("a", 1).await;
    let node = h.node("n1");
    h.caught_up(&["a"]).await;
    h.checkpoints.inner.set_unavailable(true);
    h.journal.fail("feed", usize::MAX);
    h.commit("b", 1).await;
    h.run_for(LEASE * 2).await;
    assert_eq!(
        h.observer.count("lease:Lost"),
        1,
        "the keeper could not extend past the deadline"
    );
    assert!(h.view("b").await.is_none());
    h.checkpoints.inner.set_unavailable(false);
    h.journal.fail("feed", 0);
    h.caught_up(&["a", "b"]).await;
    assert_eq!(h.observer.count("lease:Acquired"), 2, "claimed again after the outage");
    h.checkpoints.reply_lost("advance", 1);
    h.commit("c", 1).await;
    let status = h.caught_up(&["a", "b", "c"]).await;
    assert_eq!(
        h.observer.count("lease:Lost"),
        1,
        "a lost reply is repeated, not a lost lease"
    );
    assert_eq!(status.applied, 3, "the repeated advance counted the events once");
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn two_processors_share_one_checkpoint_one_works_the_other_waits() {
    let h = Harness::new();
    let a = h.node("A");
    let b = h.node("B");
    h.commit("a", 1).await;
    h.caught_up(&["a"]).await;
    h.run_for(POLL * 4).await;
    assert_eq!(h.observer.count("lease:Acquired"), 1, "one owner");
    let owner = h.status().await.owner.unwrap();
    let (owner, waiter) = if owner == "A" { (a, b) } else { (b, a) };
    waiter.stop().await;
    assert_eq!(h.observer.count("lease:Released"), 0, "the waiter never owned the row");
    owner.stop().await;
    assert_eq!(
        (h.observer.count("lease:Acquired"), h.observer.count("lease:Released")),
        (1, 1)
    );
}

#[tokio::test(start_paused = true)]
async fn a_graceful_stop_hands_the_checkpoint_over_at_once() {
    let h = Harness::new();
    let gate = h.views.gate("a");
    let a = h.node("A");
    h.commit("a", 1).await;
    gate.reached().await;
    let b = h.node("B");
    let stopping = tokio::spawn(a.stop());
    h.run_for(Duration::from_millis(50)).await;
    assert!(!stopping.is_finished(), "the stop waits for the job in flight");
    gate.release();
    stopping.await.unwrap();
    let released_at = Instant::now();
    assert_eq!(h.status().await.position, h.head().await, "advanced before the release");
    h.until(async || h.status().await.owner.as_deref() == Some("B")).await;
    assert!(
        Instant::now() - released_at <= POLL,
        "B took the row at its next poll, not after the lease"
    );
    assert_eq!(h.observer.count("lease:Released"), 1);
    b.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_stop_with_a_stuck_key_releases_after_the_drain_deadline() {
    let h = Harness::new();
    let gate = h.views.gate("a");
    let a = h.node("A");
    h.commit("a", 1).await;
    gate.reached().await;
    h.commit("b", 1).await;
    h.until(async || h.view("b").await.is_some()).await;
    let started = Instant::now();
    a.stop().await;
    let waited = Instant::now() - started;
    assert!(waited >= LEASE && waited < LEASE + POLL, "{waited:?}");
    let status = h.status().await;
    assert_eq!(
        (status.owner, status.position),
        (None, Position::START),
        "released without moving past the stuck key"
    );
    gate.release(); // the stuck job of the stopped node finishes on its own
    let b = h.node("B");
    h.caught_up(&["a", "b"]).await;
    assert_eq!(
        h.view("a").await.unwrap().view.history,
        [(1, 1)],
        "the stale write and the repeat met at the contiguity rule"
    );
    assert_eq!(h.view("b").await.unwrap().version, 1, "b was not written again");
    b.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_stop_during_backoff_or_a_lost_lease_during_drain_ends_cleanly() {
    // A stop in a pause: nothing to drain, released at once.
    let h = Harness::new();
    h.views.fail_next("a", 10);
    let node = h.node("A");
    h.commit("a", 1).await;
    h.until(async || h.observer.count("failed:") == 1).await;
    node.stop().await;
    let status = h.status().await;
    assert_eq!((status.owner, status.failures, status.position), (None, 1, Position::START));
    assert_eq!(h.observer.count("lease:Released"), 1);

    // A lease lost while the drain waits: no advance, no release, the new owner's row stays its own.
    let h = Harness::new();
    let a_store = h.fork_checkpoints();
    let a = h.node_isolated("A", a_store.clone());
    h.commit("a", 1).await;
    h.caught_up(&["a"]).await;
    a_store.extend_lies(true);
    let gate = h.views.gate("a");
    h.commit("a", 2).await;
    gate.reached().await;
    let stopping = tokio::spawn(a.stop());
    h.checkpoints.inner.expire_lease(&key());
    let b = h.node("B");
    h.until(async || h.status().await.owner.as_deref() == Some("B")).await;
    gate.release();
    stopping.await.unwrap();
    assert_eq!((h.observer.count("lease:Lost"), h.observer.count("lease:Released")), (1, 0));
    assert_eq!(a_store.calls("release"), 0, "a lost owner releases nothing");
    let status = h.caught_up(&["a"]).await;
    assert_eq!(
        (status.owner.as_deref(), h.view("a").await.unwrap().view.history),
        (Some("B"), vec![(1, 1), (2, 2)])
    );
    b.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_killed_owner_is_replaced_after_the_lease_expires() {
    let h = Harness::new();
    let a = h.node("A");
    h.commit("a", 1).await;
    h.caught_up(&["a"]).await;
    let b = h.node("B");
    h.run_for(POLL * 2).await;
    a.crash();
    let crashed_at = Instant::now();
    h.commit("a", 2).await;
    h.commit("b", 1).await;
    let status = h.caught_up(&["a", "b"]).await;
    assert_eq!(status.owner.as_deref(), Some("B"));
    let acquired = h.observer.timed("lease:Acquired");
    assert_eq!(acquired.len(), 2);
    let waited = acquired[1] - crashed_at;
    assert!(
        waited >= LEASE - LEASE / 3 && waited <= LEASE + POLL,
        "took over when the lease ran out: {waited:?}"
    );
    assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1), (2, 2)]);
    b.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_rejected_advance_stops_the_old_owner() {
    let h = Harness::new();
    let a_store = h.fork_checkpoints();
    let a = h.node_isolated("A", a_store.clone());
    h.commit("a", 1).await;
    h.caught_up(&["a"]).await;
    a_store.extend_lies(true); // A believes in a lease the store lets run out
    let gate = h.views.gate("a");
    h.commit("a", 2).await;
    gate.reached().await;
    h.run_for(LEASE + POLL).await;
    let b = h.node("B");
    h.until(async || h.status().await.owner.as_deref() == Some("B")).await;
    gate.release();
    h.until(async || h.observer.count("lease:Lost") == 1).await;
    let status = h.caught_up(&["a"]).await;
    assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1), (2, 2)]);
    assert_eq!(status.owner.as_deref(), Some("B"));
    assert!(!a.is_finished(), "A keeps asking for the row");
    a.stop().await;
    b.stop().await;
}

#[tokio::test(start_paused = true)]
async fn store_errors_at_register_claim_and_release_are_retried() {
    let h = Harness::new();
    h.checkpoints.fail("register", 1);
    h.checkpoints.fail("claim", 1);
    let node = h.node("n1");
    h.commit("a", 1).await;
    h.caught_up(&["a"]).await;
    assert_eq!(
        (h.checkpoints.calls("register"), h.checkpoints.calls("claim")),
        (3, 2),
        "a failed registration, then one before each claim"
    );
    h.checkpoints.fail("take_requests", 1);
    h.commit("b", 1).await;
    h.until(async || h.observer.count("failed:checkpoints:storage") == 1).await;
    h.caught_up(&["a", "b"]).await;
    h.checkpoints.fail("release", 1);
    node.stop().await;
    assert!(
        h.status().await.owner.is_some(),
        "the failed release leaves the row to its lease"
    );
    let node = h.node("n2");
    h.commit("c", 1).await;
    let status = h.caught_up(&["a", "b", "c"]).await;
    assert_eq!(status.owner.as_deref(), Some("n2"));
    node.stop().await;
}

#[tokio::test(start_paused = true)]
async fn a_wiped_checkpoint_row_is_registered_again_and_the_journal_is_read_from_the_start() {
    let h = Harness::new();
    let node = h.node("n1");
    for key in ["a", "b"] {
        h.commit(key, 1).await;
        h.commit(key, 2).await;
    }
    h.caught_up(&["a", "b"]).await;
    // The read side is dropped as a whole: the checkpoint row with the documents.
    h.checkpoints.inner.clear();
    h.views.clear();
    h.until(async || h.observer.count("lease:Lost") == 1).await;
    h.until(async || h.observer.count("lease:Acquired") == 2).await;
    let status = h.caught_up(&["a", "b"]).await;
    assert_eq!(
        (status.applied, status.failures),
        (4, 0),
        "the journal was read again from the start"
    );
    assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1), (2, 2)]);
    // Only the checkpoint row is dropped: the documents skip the events they already hold.
    let writes = h.views.writes();
    h.checkpoints.inner.clear();
    h.until(async || h.observer.count("lease:Acquired") == 3).await;
    h.commit("a", 3).await;
    let status = h.caught_up(&["a", "b"]).await;
    assert_eq!(status.applied, 5);
    assert_eq!(h.view("a").await.unwrap().view.history, [(1, 1), (2, 2), (3, 3)]);
    assert_eq!(h.views.writes(), writes + 1, "only the new event was written");
    assert_eq!(h.observer.count("failed:"), 0);
    node.stop().await;
}
