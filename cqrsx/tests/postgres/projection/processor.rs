//! The projection processor over PostgreSQL: the journal, the checkpoint rows and the views in one database.
use crate::{
    ConnectionPingPolicy, StatementTimeout, connect_pool_with,
    support::{Database, ledger::Ledger},
};
use cqrsx::{
    cqrs::{
        CqrsFramework,
        aggregate::Aggregate,
        command::{
            CommandExecutor, executor::CommandExecutorCore, locks::LockLimits, retry::AggregateConflictRetryPolicy,
            wakeup::CommitWakeup,
        },
        event::EventEnvelope,
        projection::{
            View, ViewProjection,
            checkpoint::{CheckpointKey, CheckpointStatus, CheckpointStore, RebuildRequest},
            processor::{
                ProjectionProcessor,
                config::{DeliveryLimits, ProcessorConfig},
                observer::{Advanced, Applied, Failed, LeaseEvent, ProjectionObserver, RebuildEvent, Rebuilt, Waiting},
            },
            repository::ViewRepository,
        },
        store::{EventStore, persisted::PersistedEventStore, repository::PersistedEventRepository},
    },
    postgres::{
        checkpoint_store::PostgresCheckpointStore,
        command::{PostgresCommandConfig, PostgresCommandExecutor},
        event_repository::PostgresEventRepository,
        outbox::PostgresEventRepositoryWithOutbox,
        view_repository::PostgresViewRepository,
    },
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct Balance {
    total: i64,
    history: Vec<(usize, i32)>,
}

impl View<Ledger> for Balance {
    const NAME: &'static str = NAME;
    fn update(&mut self, event: &EventEnvelope<Ledger>) {
        self.total += i64::from(event.payload.0);
        self.history.push((event.sequence, event.payload.0));
    }
}

const NAME: &str = "balances";

fn key() -> CheckpointKey {
    CheckpointKey::new(NAME, Ledger::TYPE)
}

#[derive(Default)]
struct Recording(Mutex<Vec<String>>);

impl Recording {
    fn push(&self, event: String) {
        self.0.lock().unwrap().push(event);
    }

    fn count(&self, prefix: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.starts_with(prefix))
            .count()
    }
}

impl ProjectionObserver for Recording {
    fn applied(&self, report: &Applied<'_>) {
        self.push(format!("applied:{}:{}", report.aggregate_id, report.last_sequence));
    }
    fn advanced(&self, report: &Advanced) {
        self.push(format!("advanced:{}", report.position));
    }
    fn failed(&self, report: &Failed<'_>) {
        self.push(format!("failed:{}:{}", report.handler, report.category));
    }
    fn rebuilt(&self, report: &Rebuilt<'_>) {
        self.push(format!("rebuilt:{}:{:?}", report.aggregate_id, report.reason));
    }
    fn rebuild(&self, event: RebuildEvent) {
        self.push(format!("rebuild:{event:?}"));
    }
    fn lease(&self, event: LeaseEvent) {
        self.push(format!("lease:{event:?}"));
    }
    fn waiting(&self, reason: Waiting) {
        self.push(format!("waiting:{reason:?}"));
    }
}

type Store = PersistedEventStore<PostgresEventRepositoryWithOutbox, Ledger>;

fn ledgers(pool: &PgPool, wakeup: &CommitWakeup) -> Arc<CommandExecutorCore<Ledger, Store>> {
    let repository = PostgresEventRepositoryWithOutbox::new(pool.clone());
    let framework = CqrsFramework::new(PersistedEventStore::new(repository), ());
    Arc::new(CommandExecutorCore::new(framework, Default::default(), LockLimits::default()).with_commit_wakeup(wakeup.clone()))
}

fn config() -> ProcessorConfig {
    ProcessorConfig::try_new(
        Duration::from_millis(50),
        100,
        Duration::from_secs(1),
        DeliveryLimits::default(),
    )
    .unwrap()
    .with_retry(Duration::from_millis(50), Duration::from_millis(200))
    .unwrap()
}

fn processor(
    pool: &PgPool,
    owner: &str,
    observer: &Arc<Recording>,
    wakeup: &CommitWakeup,
) -> ProjectionProcessor<Ledger, PostgresEventRepository, PostgresCheckpointStore> {
    let views = Arc::new(PostgresViewRepository::<Balance, Ledger>::new(pool.clone()));
    ProjectionProcessor::new(
        NAME,
        Arc::new(PostgresEventRepository::new(pool.clone())),
        Arc::new(PostgresCheckpointStore::new(pool.clone())),
        Arc::new(ViewProjection::new(views)),
        config(),
    )
    .with_owner(owner)
    .with_observer(observer.clone())
    .with_wakeup(wakeup.clone())
}

async fn create_views(pool: &PgPool) {
    sqlx::raw_sql("CREATE TABLE balances (view_id text PRIMARY KEY, version bigint NOT NULL, last_sequence bigint NOT NULL CHECK (last_sequence >= 0), payload json NOT NULL)")
        .execute(pool)
        .await
        .unwrap();
}

async fn fold(pool: &PgPool, id: &str) -> Option<Balance> {
    let store = PersistedEventStore::<_, Ledger>::new(PostgresEventRepository::new(pool.clone()));
    let events = store.load_events(id).await.unwrap();
    if events.is_empty() {
        return None;
    }
    let mut balance = Balance::default();
    for event in &events {
        balance.update(event);
    }
    Some(balance)
}

/// Waits until the checkpoint stands at the journal's head and every view equals the fold of its history.
async fn caught_up(pool: &PgPool, keys: &[&str]) -> CheckpointStatus {
    let repository = PostgresEventRepository::new(pool.clone());
    let store = PostgresCheckpointStore::new(pool.clone());
    let views = PostgresViewRepository::<Balance, Ledger>::new(pool.clone());
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let status = store.status(Some(NAME)).await.unwrap().pop();
            let head = repository.head::<Ledger>().await.unwrap();
            if let Some(status) = status
                && status.position == head
            {
                let mut same = true;
                for id in keys {
                    same &= views.load(id).await.unwrap() == fold(pool, id).await;
                }
                if same {
                    return status;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the processor catches up with the journal")
}

async fn commit_all(pool: &PgPool, wakeup: &CommitWakeup, keys: &[&str], per_key: usize) {
    let executor = ledgers(pool, wakeup);
    let mut commands = JoinSet::new();
    for id in keys {
        let (executor, id) = (executor.clone(), (*id).to_owned());
        commands.spawn(async move {
            for value in 1..=per_key {
                executor.execute(&id, value as i32).await.unwrap();
            }
        });
    }
    while let Some(result) = commands.join_next().await {
        result.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forty_commands_over_five_ledgers_reach_the_checkpointed_view_in_order() {
    let db = Database::new().await;
    create_views(&db.pool).await;
    let (observer, wakeup, shutdown) = (Arc::new(Recording::default()), CommitWakeup::new(), CancellationToken::new());
    let running = tokio::spawn(processor(&db.pool, "node-a", &observer, &wakeup).run(shutdown.clone()));
    let keys = ["l1", "l2", "l3", "l4", "l5"];
    commit_all(&db.pool, &wakeup, &keys, 8).await;
    let status = caught_up(&db.pool, &keys).await;
    assert_eq!(
        (status.applied, status.owner.as_deref(), status.failures),
        (40, Some("node-a"), 0)
    );
    let views = PostgresViewRepository::<Balance, Ledger>::new(db.pool.clone());
    for id in keys {
        let balance = views.load(id).await.unwrap().unwrap();
        assert_eq!(balance.history, (1..=8).map(|n| (n, n as i32)).collect::<Vec<_>>());
    }
    shutdown.cancel();
    running.await.unwrap();
    let after = PostgresCheckpointStore::new(db.pool.clone())
        .status(Some(NAME))
        .await
        .unwrap()
        .remove(0);
    assert_eq!((after.owner, after.position), (None, status.position), "released on stop");
    assert_eq!((observer.count("lease:Acquired"), observer.count("lease:Released")), (1, 1));
    assert!(observer.count("advanced:") >= 1 && observer.count("applied:") >= 5);
    db.close().await;
}

/// Forty parallel commands to one hot key through the PostgreSQL executor, which wakes the processor: the lock
/// orders them, so the view takes them in commit order. (A wake-up cannot be timed here: the snapshot horizon
/// is the cluster's, and a test that holds a transaction open next to this one delays the feed until a poll.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forty_parallel_commands_to_one_key_reach_the_view_in_commit_order() {
    let db = Database::new().await;
    create_views(&db.pool).await;
    let (observer, wakeup, shutdown) = (Arc::new(Recording::default()), CommitWakeup::new(), CancellationToken::new());
    let running = tokio::spawn(processor(&db.pool, "node-a", &observer, &wakeup).run(shutdown.clone()));
    let single_attempt = AggregateConflictRetryPolicy::try_new(1, Duration::ZERO, Duration::ZERO, 0.0).unwrap();
    let config = PostgresCommandConfig::new(
        std::num::NonZeroUsize::new(10).unwrap(),
        single_attempt,
        LockLimits::default(),
    );
    let executor = Arc::new(PostgresCommandExecutor::<Ledger>::new(db.pool.clone(), config, ()).with_commit_wakeup(wakeup));
    let mut commands = JoinSet::new();
    for value in 1..=40 {
        let executor = executor.clone();
        commands.spawn(async move { executor.execute(&"hot".into(), value).await });
    }
    while let Some(result) = commands.join_next().await {
        assert!(result.unwrap().is_ok(), "the lock orders the commands: nothing conflicts");
    }
    let status = caught_up(&db.pool, &["hot"]).await;
    assert_eq!((status.applied, status.failures), (40, 0));
    let balance = PostgresViewRepository::<Balance, Ledger>::new(db.pool.clone())
        .load("hot")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        balance.history.iter().map(|(sequence, _)| *sequence).collect::<Vec<_>>(),
        (1..=40).collect::<Vec<_>>()
    );
    shutdown.cancel();
    running.await.unwrap();
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_database_connection_is_retried_and_the_claim_is_renewed() {
    let db = Database::new().await;
    create_views(&db.pool).await;
    let own_pool = connect_pool_with(
        db.pool.connect_options().as_ref().clone().application_name("cqrsx-processor"),
        PgPoolOptions::new().max_connections(3),
        ConnectionPingPolicy::Default,
        StatementTimeout::Custom(Duration::from_secs(5)),
    )
    .await
    .unwrap();
    let (observer, wakeup, shutdown) = (Arc::new(Recording::default()), CommitWakeup::new(), CancellationToken::new());
    let running = tokio::spawn(processor(&own_pool, "node-a", &observer, &wakeup).run(shutdown.clone()));
    let keys = ["l1", "l2"];
    commit_all(&db.pool, &wakeup, &keys, 5).await;
    caught_up(&db.pool, &keys).await;
    let killed: Vec<bool> = sqlx::query_scalar(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'cqrsx-processor' AND pid <> pg_backend_pid()",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert!(!killed.is_empty(), "the processor held sessions");
    commit_all(&db.pool, &wakeup, &keys, 5).await;
    let status = caught_up(&db.pool, &keys).await;
    assert_eq!((status.applied, status.owner.as_deref()), (20, Some("node-a")));
    assert!(observer.count("lease:Acquired") >= 1);
    shutdown.cancel();
    running.await.unwrap();
    own_pool.close().await;
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_killed_owner_is_replaced_after_the_lease_expires() {
    let db = Database::new().await;
    create_views(&db.pool).await;
    let (observer, wakeup) = (Arc::new(Recording::default()), CommitWakeup::new());
    let (stop_a, stop_b) = (CancellationToken::new(), CancellationToken::new());
    let node_a = tokio::spawn(processor(&db.pool, "node-a", &observer, &wakeup).run(stop_a.clone()));
    let node_b = tokio::spawn(processor(&db.pool, "node-b", &observer, &wakeup).run(stop_b.clone()));
    let keys = ["l1", "l2", "l3"];
    commit_all(&db.pool, &wakeup, &keys, 3).await;
    let before = caught_up(&db.pool, &keys).await;
    let owner = before.owner.clone().unwrap();
    let (killed, survivor, stop) = if owner == "node-a" {
        (node_a, node_b, stop_b)
    } else {
        (node_b, node_a, stop_a)
    };
    killed.abort(); // no release: the lease runs out by itself
    assert!(killed.await.unwrap_err().is_cancelled());
    commit_all(&db.pool, &wakeup, &keys, 3).await;
    let after = caught_up(&db.pool, &keys).await;
    assert_ne!(after.owner.as_deref(), Some(owner.as_str()), "the survivor took the row");
    assert_eq!(after.applied, 18, "nothing was lost or applied twice at the checkpoint");
    stop.cancel();
    survivor.await.unwrap();
    assert_eq!(observer.count("lease:Acquired"), 2);
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_writing_transaction_pauses_the_projection_without_skipping() {
    let db = Database::new().await;
    create_views(&db.pool).await;
    let (observer, wakeup, shutdown) = (Arc::new(Recording::default()), CommitWakeup::new(), CancellationToken::new());
    let running = tokio::spawn(processor(&db.pool, "node-a", &observer, &wakeup).run(shutdown.clone()));
    commit_all(&db.pool, &wakeup, &["l1"], 1).await;
    let before = caught_up(&db.pool, &["l1"]).await;
    // A slower writer with a lower transaction id holds the horizon until it ends.
    let mut open = db.pool.begin().await.unwrap();
    sqlx::query("INSERT INTO events (aggregate_type, aggregate_id, sequence, event_type, event_version, payload, metadata) VALUES ($1, 'slow', 1, 'delta', '1', '5', '{}')")
        .bind(Ledger::TYPE)
        .execute(&mut *open)
        .await
        .unwrap();
    commit_all(&db.pool, &wakeup, &["l2"], 2).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while observer.count("waiting:Horizon") == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the processor reports the horizon");
    let store = PostgresCheckpointStore::new(db.pool.clone());
    assert_eq!(
        store.status(Some(NAME)).await.unwrap()[0].position,
        before.position,
        "nothing is skipped past the slow writer"
    );
    let views = PostgresViewRepository::<Balance, Ledger>::new(db.pool.clone());
    assert!(views.load("l2").await.unwrap().is_none());
    open.rollback().await.unwrap();
    let after = caught_up(&db.pool, &["l1", "l2"]).await;
    assert!(after.position > before.position);
    assert_eq!(views.load("l2").await.unwrap().unwrap().history, [(1, 1), (2, 2)]);
    assert!(views.load("slow").await.unwrap().is_none());
    shutdown.cancel();
    running.await.unwrap();
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rebuild_under_concurrent_commits_ends_in_the_fold_of_the_journal() {
    let db = Database::new().await;
    create_views(&db.pool).await;
    let (observer, wakeup, shutdown) = (Arc::new(Recording::default()), CommitWakeup::new(), CancellationToken::new());
    let running = tokio::spawn(processor(&db.pool, "node-a", &observer, &wakeup).run(shutdown.clone()));
    let keys = ["l1", "l2", "l3", "l4", "l5"];
    commit_all(&db.pool, &wakeup, &keys, 4).await;
    caught_up(&db.pool, &keys).await;
    let store = PostgresCheckpointStore::new(db.pool.clone());
    assert_eq!(
        store.request_rebuild(&key(), Duration::ZERO).await.unwrap(),
        RebuildRequest::Accepted
    );
    commit_all(&db.pool, &wakeup, &keys, 4).await;
    let status = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let status = caught_up(&db.pool, &keys).await;
            if status.rebuild_cursor.is_none() && status.rebuilt == 5 {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the rebuild finishes");
    assert_eq!(status.applied, 40);
    assert_eq!(
        (observer.count("rebuild:Started"), observer.count("rebuild:Finished")),
        (1, 1)
    );
    assert!(observer.count("rebuilt:") >= 5);
    assert_eq!(observer.count("failed:"), 0);
    shutdown.cancel();
    running.await.unwrap();
    db.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_processors_share_one_checkpoint_one_works_the_other_waits() {
    let db = Database::new().await;
    create_views(&db.pool).await;
    let (observer, wakeup) = (Arc::new(Recording::default()), CommitWakeup::new());
    let (stop_a, stop_b) = (CancellationToken::new(), CancellationToken::new());
    let node_a = tokio::spawn(processor(&db.pool, "node-a", &observer, &wakeup).run(stop_a.clone()));
    let node_b = tokio::spawn(processor(&db.pool, "node-b", &observer, &wakeup).run(stop_b.clone()));
    commit_all(&db.pool, &wakeup, &["l1"], 3).await;
    let status = caught_up(&db.pool, &["l1"]).await;
    assert_eq!(observer.count("lease:Acquired"), 1, "one owner");
    let (owner, waiter, stop_owner, stop_waiter) = if status.owner.as_deref() == Some("node-a") {
        (node_a, node_b, stop_a, stop_b)
    } else {
        (node_b, node_a, stop_b, stop_a)
    };
    stop_waiter.cancel();
    waiter.await.unwrap();
    stop_owner.cancel();
    owner.await.unwrap();
    assert_eq!((observer.count("lease:Acquired"), observer.count("lease:Released")), (1, 1));
    assert!(
        PostgresCheckpointStore::new(db.pool.clone())
            .status(Some(NAME))
            .await
            .unwrap()[0]
            .owner
            .is_none()
    );
    db.close().await;
}
