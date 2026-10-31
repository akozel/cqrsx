//! The processor against a plain model under random faults: commits, crashes at the dangerous points, lost
//! leases, failing keys, wiped documents and rebuilds, in any order. After every step the documents are
//! folds of contiguous prefixes and the checkpoint never passes an event its document has not applied; at
//! the end, without faults, everything converges on the journal.
use crate::projection::support::{Harness, Node, key};
use crate::support::ledger::Ledger;
use crate::support::property_config;
use cqrsx::{
    cqrs::projection::{
        checkpoint::{CheckpointStore, RebuildRequest},
        repository::StoredView,
    },
    journal::{feed::Position, repository::PersistedEventRepository},
};
use proptest::prelude::*;
use std::{collections::HashMap, time::Duration};

const KEYS: [&str; 3] = ["k0", "k1", "k2"];

#[derive(Clone, Copy, Debug)]
enum Point {
    FeedRead,
    BeforeAdvance,
    AfterAdvance,
    BeforeAdvanceRebuild,
}

#[derive(Clone, Debug)]
enum Op {
    Commit(usize, u8),
    Tick(u16),
    FailKey(usize, u8),
    DeleteDoc(usize),
    RequestKey(usize),
    RequestRebuild,
    Crash(Point),
    LoseLease,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        10 => (0..KEYS.len(), 1u8..=3).prop_map(|(k, n)| Op::Commit(k, n)),
        6 => (20u16..=1500).prop_map(Op::Tick),
        2 => (0..KEYS.len(), 1u8..=2).prop_map(|(k, n)| Op::FailKey(k, n)),
        2 => (0..KEYS.len()).prop_map(Op::DeleteDoc),
        2 => (0..KEYS.len()).prop_map(Op::RequestKey),
        1 => Just(Op::RequestRebuild),
        2 => prop_oneof![
            Just(Point::FeedRead),
            Just(Point::BeforeAdvance),
            Just(Point::AfterAdvance),
            Just(Point::BeforeAdvanceRebuild)
        ]
        .prop_map(Op::Crash),
        1 => Just(Op::LoseLease),
    ]
}

struct Cluster {
    nodes: Vec<Node>,
    next: usize,
}

impl Cluster {
    fn spawn(&mut self, h: &Harness) {
        self.next += 1;
        self.nodes.push(h.node(&format!("node-{}", self.next)));
    }

    fn crash_all(&mut self) {
        for node in self.nodes.drain(..) {
            node.crash();
        }
    }
}

/// The documents are folds of contiguous prefixes, and every event below the checkpoint that was committed
/// after the last wipe of its document is applied to it.
async fn invariants(h: &Harness, wiped: &HashMap<&str, Position>) -> Result<(), TestCaseError> {
    let status = h.status().await;
    let journal = h
        .journal
        .inner
        .feed_after::<Ledger>(Position::START, 1_000_000)
        .await
        .unwrap();
    for key in KEYS {
        let fold = h.fold(key).await;
        let history = fold.as_ref().map(|fold| &fold.history[..]).unwrap_or(&[]);
        let document = h.view(key).await;
        if let Some(StoredView { view, last_sequence, .. }) = &document {
            prop_assert!(*last_sequence <= history.len(), "{key}: document beyond the journal");
            prop_assert_eq!(&view.history[..], &history[..*last_sequence], "{}: not a prefix fold", key);
            let total: i64 = history[..*last_sequence].iter().map(|(_, value)| i64::from(*value)).sum();
            prop_assert_eq!(view.total, total);
        }
        let since = wiped.get(key).copied().unwrap_or(Position::START);
        let covered = journal
            .iter()
            .filter(|entry| entry.event.aggregate_id == key && entry.position > since && entry.position <= status.position)
            .map(|entry| entry.event.sequence)
            .max();
        if let Some(sequence) = covered {
            let applied = document.as_ref().map_or(0, |stored| stored.last_sequence);
            prop_assert!(
                applied >= sequence,
                "{key}: the checkpoint {:?} covers event {sequence} but the document is at {applied}",
                status.position
            );
        }
    }
    Ok(())
}

async fn check(ops: Vec<Op>) -> Result<(), TestCaseError> {
    let h = Harness::new();
    let mut cluster = Cluster {
        nodes: Vec::new(),
        next: 0,
    };
    cluster.spawn(&h);
    h.until(async || h.checkpoints.inner.status(None).await.unwrap().len() == 1)
        .await;
    let mut wiped: HashMap<&str, Position> = HashMap::new();
    let mut committed: HashMap<&str, i32> = HashMap::new();
    for op in ops {
        match op {
            Op::Commit(k, events) => {
                for _ in 0..events {
                    let value = committed.entry(KEYS[k]).or_insert(0);
                    *value += 1;
                    h.commit(KEYS[k], *value).await;
                }
            }
            Op::Tick(millis) => h.run_for(Duration::from_millis(u64::from(millis))).await,
            Op::FailKey(k, times) => h.views.fail_next(KEYS[k], usize::from(times)),
            Op::DeleteDoc(k) => {
                h.views.delete(KEYS[k]).await;
                wiped.insert(KEYS[k], h.head().await);
            }
            Op::RequestKey(k) => h.checkpoints.request_rebuild_key(&key(), KEYS[k]).await.unwrap(),
            Op::RequestRebuild => {
                let _ = h.checkpoints.request_rebuild(&key(), Duration::ZERO).await.unwrap();
            }
            Op::Crash(point) => {
                let gate = match point {
                    Point::FeedRead => h.journal.gate_feed(),
                    Point::BeforeAdvance => h.checkpoints.gate_before("advance"),
                    Point::AfterAdvance => h.checkpoints.gate_after("advance"),
                    Point::BeforeAdvanceRebuild => h.checkpoints.gate_before("advance_rebuild"),
                };
                let reached = tokio::time::timeout(Duration::from_secs(5), gate.reached()).await.is_ok();
                if reached {
                    cluster.crash_all();
                }
                gate.release();
                if cluster.nodes.is_empty() {
                    cluster.spawn(&h);
                }
            }
            Op::LoseLease => {
                h.checkpoints.inner.expire_lease(&key());
                if cluster.nodes.len() >= 2 {
                    cluster.nodes.remove(0).crash();
                }
                cluster.spawn(&h);
            }
        }
        h.run_for(Duration::from_millis(20)).await;
        invariants(&h, &wiped).await?;
    }
    // Without faults everything converges: a final rebuild of the journal recreates the wiped documents.
    for key in KEYS {
        h.views.fail_next(key, 0);
    }
    h.views.fail_next("*", 0);
    if cluster.nodes.is_empty() {
        cluster.spawn(&h);
    }
    let finished = h.observer.count("rebuild:Finished");
    loop {
        match h.checkpoints.request_rebuild(&key(), Duration::ZERO).await.unwrap() {
            RebuildRequest::Accepted => break,
            RebuildRequest::AlreadyRunning => h.run_for(Duration::from_millis(100)).await,
        }
    }
    h.until(async || h.observer.count("rebuild:Finished") > finished).await;
    let status = h.caught_up(&KEYS).await;
    prop_assert_eq!(status.position, h.head().await);
    prop_assert_eq!(status.rebuild_cursor, None);
    for key in KEYS {
        let fold = h.fold(key).await;
        prop_assert_eq!(h.view(key).await.map(|stored| stored.view), fold, "{}", key);
    }
    invariants(&h, &HashMap::new()).await?;
    for node in cluster.nodes.drain(..) {
        node.stop().await;
    }
    Ok(())
}

proptest! {
    #![proptest_config(proptest::test_runner::Config { cases: 128, ..property_config() })]
    #[test]
    fn property_processor_matches_a_model_under_random_faults(ops in proptest::collection::vec(op(), 1..40)) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
        runtime.block_on(check(ops))?;
    }
}
