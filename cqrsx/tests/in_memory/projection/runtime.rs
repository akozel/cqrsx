//! The projection runtime: several processors started together and stopped by one token.
use crate::projection::support::{Balance, Harness, Journal, config};
use crate::support::ledger::Ledger;
use cqrsx::memory::{checkpoint_store::MemCheckpointStore, view_repository::MemViewRepository};
use cqrsx::{
    projection::{
        checkpoint::CheckpointStore,
        processor::{
            ProjectionProcessor,
            observer::{Advanced, ProjectionObserver},
        },
        runtime::{ProjectionRuntime, ProjectionRuntimeError},
    },
    view::{projection::ViewProjection, repository::ViewRepository},
};
use std::{sync::Arc, time::Duration};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

type Views = MemViewRepository<Balance, Ledger>;

/// A processor of projection `name` over the journal of `h` with views and checkpoints of its own.
fn processor(
    h: &Harness,
    name: &str,
) -> (
    ProjectionProcessor<Ledger, Journal, MemCheckpointStore>,
    Arc<Views>,
    Arc<MemCheckpointStore>,
) {
    let views = Arc::new(Views::default());
    let checkpoints = Arc::new(MemCheckpointStore::default());
    let processor = ProjectionProcessor::new(
        name,
        h.journal.clone(),
        checkpoints.clone(),
        Arc::new(ViewProjection::new(views.clone())),
        config(),
    );
    (processor, views, checkpoints)
}

/// Waits (in virtual time) until the view of `key` has applied `events` events.
async fn applied(views: &Views, key: &str, events: usize) {
    for _ in 0..200 {
        if views
            .load(key)
            .await
            .unwrap()
            .is_some_and(|view| view.history.len() == events)
        {
            return;
        }
        sleep(Duration::from_millis(50)).await;
    }
    panic!("`{key}` did not reach {events} applied events");
}

/// Panics when its processor moves the checkpoint, which happens only after a commit.
struct PanicsOnAdvance;

impl ProjectionObserver for PanicsOnAdvance {
    fn advanced(&self, _: &Advanced) {
        panic!("observer bug");
    }
}

#[tokio::test(start_paused = true)]
async fn every_processor_of_a_runtime_runs_until_one_token_stops_them_all() {
    let h = Harness::new();
    let (first, first_views, first_rows) = processor(&h, "first");
    let (second, second_views, second_rows) = processor(&h, "second");
    let runtime = ProjectionRuntime::new().with_processor(first).with_processor(second);
    assert_eq!((runtime.len(), runtime.is_empty()), (2, false));
    h.commit("a", 3).await;
    h.commit("a", 4).await;

    let shutdown = CancellationToken::new();
    let running = tokio::spawn(runtime.run(shutdown.clone()));
    applied(&first_views, "a", 2).await;
    applied(&second_views, "a", 2).await;
    assert!(!running.is_finished(), "the runtime runs while the token is live");

    shutdown.cancel();
    assert_eq!(running.await.unwrap(), Ok(()));
    for (name, rows) in [("first", first_rows), ("second", second_rows)] {
        let status = rows.status(Some(name)).await.unwrap();
        assert_eq!(status.len(), 1);
        assert_eq!(status[0].owner, None, "{name} released its row on stop");
        assert_eq!(status[0].position, h.head().await, "{name} moved its checkpoint to the head");
    }
}

#[tokio::test(start_paused = true)]
async fn an_empty_runtime_ends_at_once_and_a_runtime_prints_the_names_of_its_processors() {
    let empty = ProjectionRuntime::new();
    assert_eq!((empty.len(), empty.is_empty()), (0, true));
    assert_eq!(empty.run(CancellationToken::new()).await, Ok(()));

    let h = Harness::new();
    let (first, _, _) = processor(&h, "first");
    let printed = format!("{:?}", first.with_owner("node-7"));
    assert!(
        printed.contains("\"first\"") && printed.contains("\"ledger\"") && printed.contains("\"node-7\""),
        "name, aggregate type and owner: {printed}"
    );
    let runtime = ProjectionRuntime::new()
        .with_processor(processor(&h, "first").0)
        .with_processor(processor(&h, "second").0);
    let printed = format!("{runtime:?}");
    assert!(printed.contains("first") && printed.contains("second"), "{printed}");
}

#[tokio::test(start_paused = true)]
async fn a_panicking_processor_stops_the_others_and_the_error_names_it() {
    let h = Harness::new();
    let (healthy, _, healthy_rows) = processor(&h, "healthy");
    let (broken, _, _) = processor(&h, "broken");
    let runtime = ProjectionRuntime::new()
        .with_processor(healthy)
        .with_processor(broken.with_observer(Arc::new(PanicsOnAdvance)));

    let shutdown = CancellationToken::new();
    let running = tokio::spawn(runtime.run(shutdown.clone()));
    // The healthy processor holds its row before the broken one gets anything to advance over.
    for _ in 0..200 {
        let rows = healthy_rows.status(Some("healthy")).await.unwrap();
        if rows.first().is_some_and(|row| row.owner.is_some()) {
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        healthy_rows.status(Some("healthy")).await.unwrap()[0].owner.is_some(),
        "the healthy processor claimed its row"
    );
    h.commit("a", 1).await;

    let result = timeout(Duration::from_secs(30), running)
        .await
        .expect("the panic must end the runtime, not leave the healthy processor running")
        .unwrap();
    let error = result.unwrap_err();
    assert_eq!(
        error,
        ProjectionRuntimeError::Panicked {
            projection: "broken".into()
        }
    );
    assert_eq!(error.to_string(), "projection processor broken panicked");
    assert!(shutdown.is_cancelled(), "the token stops the rest");
    let status = healthy_rows.status(Some("healthy")).await.unwrap();
    assert_eq!(status.len(), 1, "the healthy processor registered its row");
    assert_eq!(status[0].owner, None, "and released it when it stopped");
}

#[tokio::test(start_paused = true)]
async fn dropping_the_run_future_cancels_the_token() {
    let h = Harness::new();
    let (only, _, _) = processor(&h, "only");
    let runtime = ProjectionRuntime::new().with_processor(only);
    let shutdown = CancellationToken::new();
    // The runtime never ends by itself; the timeout drops its future.
    assert!(timeout(Duration::from_secs(1), runtime.run(shutdown.clone())).await.is_err());
    assert!(shutdown.is_cancelled());
}
