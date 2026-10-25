//! The fixtures of the MongoDB suite: a ledger with its balance view, processors whose views and checkpoint
//! live in MongoDB, commands over any journal, and the waits for the read side to catch up.
use cqrsx::{
    cqrs::{
        CqrsFramework,
        aggregate::Aggregate,
        command::{executor::CommandExecutorCore, locks::LockLimits, wakeup::CommitWakeup},
        event::{DomainEvent, EventEnvelope},
        event_sink::EventSink,
        projection::{
            View, ViewProjection,
            checkpoint::{CheckpointKey, CheckpointStatus, CheckpointStore},
            processor::{
                ProjectionProcessor,
                config::{DeliveryLimits, ProcessorConfig},
                observer::{Applied, Failed, LeaseEvent, ProjectionObserver, RebuildEvent, Rebuilt},
            },
            repository::ViewRepository,
        },
        store::{persisted::PersistedEventStore, repository::PersistedEventRepository},
        testing::mem_event_repository::MemEventRepository,
    },
    mongodb::{projection_checkpoints::MongoCheckpointStore, view_repository::MongoViewRepository},
    task::NewTask,
};
use mongodb::Database;
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Ledger(pub(crate) Vec<i32>);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Delta(pub(crate) i32);

impl DomainEvent for Delta {
    fn event_type(&self) -> String {
        "delta".into()
    }
    fn event_version(&self) -> String {
        "1".into()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("never")]
pub(crate) struct Never;

impl Aggregate for Ledger {
    const TYPE: &'static str = "ledger";
    type Command = Vec<i32>;
    type Event = Delta;
    type Error = Never;
    type Services = ();

    async fn handle(&mut self, changes: Vec<i32>, _: &(), sink: &EventSink<Self>) -> Result<(), Never> {
        for change in changes {
            sink.write(Delta(change), self).await;
        }
        Ok(())
    }

    fn apply(&mut self, event: Delta) {
        self.0.push(event.0);
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct Balance {
    pub(crate) total: i64,
    pub(crate) history: Vec<(usize, i32)>,
}

impl View<Ledger> for Balance {
    fn update(&mut self, event: &EventEnvelope<Ledger>) {
        self.total += i64::from(event.payload.0);
        self.history.push((event.sequence, event.payload.0));
    }
}

/// The projection's name: its collection and its checkpoint.
pub(crate) const NAME: &str = "balances";

pub(crate) fn key() -> CheckpointKey {
    CheckpointKey::new(NAME, Ledger::TYPE)
}

pub(crate) fn views(database: &Database) -> MongoViewRepository<Balance, Ledger> {
    MongoViewRepository::new(database, NAME)
}

/// The history `1..=values` of one ledger, event `n` adding `n`.
pub(crate) fn history(id: &str, values: usize) -> Vec<EventEnvelope<Ledger>> {
    (1..=values)
        .map(|sequence| EventEnvelope {
            aggregate_id: id.to_owned(),
            sequence,
            payload: Delta(sequence as i32),
            metadata: Default::default(),
        })
        .collect()
}

/// What a processor reported, one line per report.
#[derive(Default)]
pub(crate) struct Recording(Mutex<Vec<String>>);

impl Recording {
    fn push(&self, event: String) {
        self.0.lock().unwrap().push(event);
    }

    pub(crate) fn count(&self, prefix: &str) -> usize {
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
}

fn config() -> ProcessorConfig {
    ProcessorConfig::new(
        Duration::from_millis(50),
        100,
        Duration::from_secs(1),
        DeliveryLimits::default(),
    )
    .unwrap()
    .with_retry(Duration::from_millis(50), Duration::from_millis(200))
    .unwrap()
}

/// A running processor of [`NAME`].
pub(crate) struct Node {
    task: JoinHandle<()>,
    shutdown: CancellationToken,
}

impl Node {
    /// Starts a processor of [`NAME`] over `journal` whose views and checkpoint live in `database`.
    pub(crate) fn start<R: PersistedEventRepository + 'static>(
        journal: Arc<R>,
        database: &Database,
        owner: &str,
        observer: &Arc<Recording>,
        wakeup: &CommitWakeup,
    ) -> Self {
        let projection = Arc::new(ViewProjection::new(Arc::new(views(database))).with_name(NAME));
        let processor = ProjectionProcessor::new(
            NAME,
            journal,
            Arc::new(MongoCheckpointStore::new(database)),
            projection,
            config(),
        )
        .with_owner(owner)
        .with_observer(observer.clone())
        .with_wakeup(wakeup.clone());
        let shutdown = CancellationToken::new();
        Self {
            task: tokio::spawn(processor.run(shutdown.clone())),
            shutdown,
        }
    }

    pub(crate) async fn stop(self) {
        self.shutdown.cancel();
        self.task.await.unwrap();
    }
}

/// Runs `per_key` commands on every key, the keys at once and the commands of one key in order; command
/// `n` of a key adds `n`.
pub(crate) async fn commit_all<C, F>(keys: &[&str], per_key: i32, commit: C)
where
    C: Fn(String, i32) -> F + Clone + Send + 'static,
    F: Future<Output = ()> + Send,
{
    let mut commands = JoinSet::new();
    for id in keys {
        let (commit, id) = (commit.clone(), (*id).to_owned());
        commands.spawn(async move {
            for value in 1..=per_key {
                commit(id.clone(), value).await;
            }
        });
    }
    while let Some(result) = commands.join_next().await {
        result.unwrap();
    }
}

type MemStore = PersistedEventStore<MemEventRepository, Ledger>;
type MemFactory = Box<dyn Fn(Vec<NewTask>) -> CqrsFramework<Ledger, MemStore> + Send + Sync>;

/// Commands over an in-memory journal that wake its processors.
pub(crate) fn mem_ledgers(journal: &MemEventRepository, wakeup: &CommitWakeup) -> Arc<CommandExecutorCore<Ledger, MemFactory>> {
    let journal = journal.clone();
    let factory: MemFactory = Box::new(move |_| CqrsFramework::new(PersistedEventStore::new_event_store(journal.clone()), ()));
    Arc::new(CommandExecutorCore::new(factory, Default::default(), LockLimits::default()).with_commit_wakeup(wakeup.clone()))
}

/// The fold of the key's whole history, `None` without a history.
pub(crate) async fn fold<R: PersistedEventRepository>(journal: &R, id: &str) -> Option<Balance> {
    let events = journal.get_events::<Ledger>(id).await.unwrap();
    if events.is_empty() {
        return None;
    }
    let mut balance = Balance::default();
    for event in events {
        balance.update(&EventEnvelope::try_from(event).unwrap());
    }
    Some(balance)
}

/// Waits until the checkpoint stands at the journal's head and every view equals the fold of its history.
pub(crate) async fn caught_up<R: PersistedEventRepository>(journal: &R, database: &Database, keys: &[&str]) -> CheckpointStatus {
    let (checkpoints, views) = (MongoCheckpointStore::new(database), views(database));
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let status = checkpoints.status(Some(NAME)).await.unwrap().pop();
            let head = journal.head::<Ledger>().await.unwrap();
            if let Some(status) = status
                && status.position == head
            {
                let mut same = true;
                for id in keys {
                    same &= views.load(id).await.unwrap() == fold(journal, id).await;
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

/// Waits until `condition` holds.
pub(crate) async fn until(what: &str, condition: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} never happened"));
}
