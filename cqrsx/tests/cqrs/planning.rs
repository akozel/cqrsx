//! Tasks planned from decisions, end to end in memory: a process across two aggregates, the plans of
//! conflicted attempts, and two executors racing like two nodes.
use crate::support::ledger::{Command, Delta, Ledger, Rejected, Services};
use cqrsx::{
    cqrs::{
        CqrsFramework,
        aggregate::Aggregate,
        command::{
            CommandExecutor, CommandGatewayError,
            causation::Causation,
            decide::Decide,
            executor::CommandExecutorCore,
            locks::LockLimits,
            planner::{Decision, TaskPlanError, TaskPlanner},
            retry::AggregateConflictRetryPolicy,
        },
        event::DomainEvent,
        store::{
            PersistenceError,
            feed::{FeedEntry, Position},
            persisted::PersistedEventStore,
            repository::PersistedEventRepository,
            serialized_event::SerializedEvent,
            snapshot::{SerializedSnapshot, SnapshotUpdate},
        },
        testing::{command::TestCommandExecutor, mem_event_repository::MemEventRepository},
    },
    task::{
        NewTask,
        attempt::TaskAttempt,
        format::{TaskFormat, TaskPayload},
    },
};
use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

/// The box office of one show: a seat goes to one fan.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Show {
    seats: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Booked {
    seat: String,
    fan: String,
}

impl DomainEvent for Booked {
    fn event_type(&self) -> String {
        "booked".into()
    }
    fn event_version(&self) -> String {
        "1".into()
    }
}

#[derive(Clone, Debug)]
struct Book {
    seat: String,
    fan: String,
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("seat taken")]
struct SeatTaken;

impl Aggregate for Show {
    const TYPE: &'static str = "show";
    type Command = Book;
    type Event = Booked;
    type Error = SeatTaken;
    type Rules = ();

    fn apply(&mut self, Booked { seat, fan }: Booked) {
        self.seats.insert(seat, fan);
    }
}

impl Decide<Show> for Book {
    fn decide(self, show: &Show, _: &()) -> Result<Booked, SeatTaken> {
        let Book { seat, fan } = self;
        if show.seats.contains_key(&seat) {
            return Err(SeatTaken);
        }
        Ok(Booked { seat, fan })
    }
}

/// A fan's wallet: every ticket once, so a redelivered task is refused.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Wallet {
    tickets: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Recorded {
    ticket: String,
}

impl DomainEvent for Recorded {
    fn event_type(&self) -> String {
        "recorded".into()
    }
    fn event_version(&self) -> String {
        "1".into()
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
#[error("already recorded")]
struct AlreadyRecorded;

impl Aggregate for Wallet {
    const TYPE: &'static str = "wallet";
    type Command = String;
    type Event = Recorded;
    type Error = AlreadyRecorded;
    type Rules = ();

    fn apply(&mut self, Recorded { ticket }: Recorded) {
        self.tickets.push(ticket);
    }
}

impl Decide<Wallet> for String {
    fn decide(self, wallet: &Wallet, _: &()) -> Result<Recorded, AlreadyRecorded> {
        if wallet.tickets.contains(&self) {
            return Err(AlreadyRecorded);
        }
        Ok(Recorded { ticket: self })
    }
}

/// Step one of the delivery: a booked seat goes into the fan's wallet.
#[derive(Debug, Serialize, Deserialize)]
struct RecordInWallet {
    fan: String,
    ticket: String,
}

impl TaskPayload for RecordInWallet {
    const TASK_TYPE: &'static str = "record_in_wallet";
    const TASK_VERSION: i32 = 1;
}

/// Step two: a recorded ticket is mailed.
#[derive(Debug, Serialize, Deserialize)]
struct MailTicket {
    fan: String,
    ticket: String,
}

impl TaskPayload for MailTicket {
    const TASK_TYPE: &'static str = "mail_ticket";
    const TASK_VERSION: i32 = 1;
}

/// The whole delivery process, read in one place: what it does after the box office and after the wallet.
struct Delivery;

impl TaskPlanner<Show> for Delivery {
    fn plan(&self, decision: &Decision<'_, Show>) -> Result<Vec<NewTask>, TaskPlanError> {
        let mut tasks = Vec::new();
        for Booked { seat, fan } in decision.events {
            let ticket = format!("{}/{seat}", decision.aggregate_id);
            tasks.push(NewTask::new(RecordInWallet {
                fan: fan.clone(),
                ticket,
            })?);
        }
        Ok(tasks)
    }
}

impl TaskPlanner<Wallet> for Delivery {
    fn plan(&self, decision: &Decision<'_, Wallet>) -> Result<Vec<NewTask>, TaskPlanError> {
        let mut tasks = Vec::new();
        for Recorded { ticket } in decision.events {
            tasks.push(NewTask::new(MailTicket {
                fan: decision.aggregate_id.to_owned(),
                ticket: ticket.clone(),
            })?);
        }
        Ok(tasks)
    }
}

/// What the task runtime would hand the wallet step: a claim of `task`.
fn claim(task: &NewTask, number: i32) -> TaskAttempt {
    TaskAttempt::new(
        task.id(),
        task.format().clone(),
        task.payload().clone(),
        task.metadata().clone(),
        number,
    )
    .unwrap()
}

/// The wallet step: the task's payload becomes the wallet's command, which continues the task's chain.
async fn record_in_wallet(
    wallets: &TestCommandExecutor<Wallet, PersistedEventStore<MemEventRepository, Wallet>>,
    attempt: &TaskAttempt,
) -> Result<Vec<NewTask>, CommandGatewayError<AlreadyRecorded>> {
    let step: RecordInWallet = serde_json::from_value(attempt.payload().clone()).unwrap();
    let metadata = Causation::from_task(attempt).event_metadata();
    let report = wallets.execute_with_metadata(&step.fan, step.ticket, metadata).await?;
    Ok(report.tasks)
}

#[tokio::test]
async fn a_process_plans_across_two_aggregates_and_the_chain_keeps_its_correlation() {
    let journal = MemEventRepository::default();
    let shows = TestCommandExecutor::<Show, _>::over_journal(journal.clone(), ()).with_task_planner(Delivery);
    let wallets = TestCommandExecutor::<Wallet, _>::over_journal(journal.clone(), ()).with_task_planner(Delivery);

    let root = Causation::root("ui");
    let booking = shows
        .execute_with_metadata(
            "gig-1",
            Book {
                seat: "A1".into(),
                fan: "fox".into(),
            },
            root.event_metadata(),
        )
        .await
        .unwrap();
    let [first] = &booking.tasks[..] else {
        panic!("one task per booking: {:?}", booking.tasks);
    };
    assert_eq!(first.format(), &TaskFormat::of::<RecordInWallet>().unwrap());
    assert_eq!(first.id(), root.task_id(Show::TYPE, "gig-1", first.format(), 0));
    assert_eq!(first.payload(), &serde_json::json!({ "fan": "fox", "ticket": "gig-1/A1" }));

    let delivery = claim(first, 1);
    let follow_ups = record_in_wallet(&wallets, &delivery).await.unwrap();
    let [mail] = &follow_ups[..] else {
        panic!("one mail per recorded ticket: {follow_ups:?}");
    };
    assert_eq!(mail.payload(), &serde_json::json!({ "fan": "fox", "ticket": "gig-1/A1" }));
    let wallet_command = &journal.outbox::<Wallet>()[0];
    assert_eq!(wallet_command.aggregate_id, "fox");
    let stamped = mail.metadata();
    assert_eq!(
        stamped["correlation_id"],
        root.correlation_id.to_string(),
        "the chain keeps its correlation"
    );
    assert_eq!(
        stamped["causation_id"],
        first.id().to_string(),
        "the mail was caused by the first task"
    );

    // The runtime delivers the first task again (a lease expired): the wallet refuses the repeat, and the
    // process publishes no second mail.
    assert_eq!(
        record_in_wallet(&wallets, &claim(first, 2)).await,
        Err(CommandGatewayError::Domain(AlreadyRecorded))
    );
    assert_eq!(journal.outbox::<Show>().len(), 1);
    assert_eq!(journal.outbox::<Wallet>().len(), 1);
    assert_eq!(
        (shows.take_published_tasks().len(), wallets.take_published_tasks().len()),
        (1, 1)
    );
}

/// The balance a decision leaves, planned as one task.
#[derive(Serialize)]
struct Balance {
    after: i64,
}

impl TaskPayload for Balance {
    const TASK_TYPE: &'static str = "balance";
    const TASK_VERSION: i32 = 1;
}

struct BalancePlanner;

impl TaskPlanner<Ledger> for BalancePlanner {
    fn plan(&self, decision: &Decision<'_, Ledger>) -> Result<Vec<NewTask>, TaskPlanError> {
        Ok(vec![NewTask::new(Balance {
            after: decision.after.balance,
        })?])
    }
}

/// A journal that, when its schedule says so, lets "another node" commit a foreign event of `foreign` first,
/// so that the command's write conflicts and the executor tries again on the fresh state. With a gate, the
/// first two loads wait for each other.
#[derive(Clone)]
struct Interfering {
    inner: MemEventRepository,
    schedule: Arc<Mutex<Vec<bool>>>,
    foreign: i32,
    gate: Option<Arc<tokio::sync::Barrier>>,
    gated: Arc<AtomicUsize>,
}

impl Interfering {
    fn new(inner: MemEventRepository, schedule: Vec<bool>) -> Self {
        Self {
            inner,
            schedule: Arc::new(Mutex::new(schedule)),
            foreign: 1000,
            gate: None,
            gated: Arc::default(),
        }
    }
}

impl PersistedEventRepository for Interfering {
    async fn get_events<A: Aggregate>(&self, aggregate_id: &str) -> Result<Vec<SerializedEvent>, PersistenceError> {
        if let Some(gate) = &self.gate
            && self.gated.fetch_add(1, Ordering::SeqCst) < 2
        {
            gate.wait().await;
        }
        self.inner.get_events::<A>(aggregate_id).await
    }

    async fn get_events_after<A: Aggregate>(
        &self,
        aggregate_id: &str,
        after_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        self.inner.get_events_after::<A>(aggregate_id, after_sequence).await
    }

    async fn get_snapshot<A: Aggregate>(&self, aggregate_id: &str) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        self.inner.get_snapshot::<A>(aggregate_id).await
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<SnapshotUpdate>,
    ) -> Result<(), PersistenceError> {
        self.persist_with_tasks::<A>(events, snapshot_update, &[]).await
    }

    async fn persist_with_tasks<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<SnapshotUpdate>,
        tasks: &[NewTask],
    ) -> Result<(), PersistenceError> {
        let interfere = {
            let mut schedule = self.schedule.lock().unwrap();
            if schedule.is_empty() { false } else { schedule.remove(0) }
        };
        if interfere && let Some(first) = events.first() {
            let mut foreign = first.clone();
            foreign.payload = serde_json::to_value(Delta(self.foreign)).unwrap();
            self.inner.persist::<A>(&[foreign], None).await?;
        }
        self.inner.persist_with_tasks::<A>(events, snapshot_update, tasks).await
    }

    async fn feed_after<A: Aggregate>(&self, after: Position, limit: usize) -> Result<Vec<FeedEntry>, PersistenceError> {
        self.inner.feed_after::<A>(after, limit).await
    }

    async fn head<A: Aggregate>(&self) -> Result<Position, PersistenceError> {
        self.inner.head::<A>().await
    }

    async fn aggregate_ids_after<A: Aggregate>(&self, after: &str, limit: usize) -> Result<Vec<String>, PersistenceError> {
        self.inner.aggregate_ids_after::<A>(after, limit).await
    }
}

fn no_delay(attempts: usize) -> AggregateConflictRetryPolicy {
    AggregateConflictRetryPolicy::new(attempts, Duration::ZERO, Duration::ZERO, 0.0).unwrap()
}

fn ledger_executor(
    journal: Interfering,
    attempts: usize,
) -> CommandExecutorCore<Ledger, PersistedEventStore<Interfering, Ledger>> {
    CommandExecutorCore::new(
        CqrsFramework::new(PersistedEventStore::new_event_store(journal), Services::default()),
        no_delay(attempts),
        LockLimits::default(),
    )
    .with_task_planner(BalancePlanner)
}

/// The balance of `id` after the journal's first `sequence` events.
async fn balance_at(journal: &MemEventRepository, id: &str, sequence: usize) -> i64 {
    let events = journal.get_events::<Ledger>(id).await.unwrap();
    events
        .iter()
        .take(sequence)
        .map(|event| serde_json::from_value::<Delta>(event.payload.clone()).unwrap().0 as i64)
        .sum()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, ..ProptestConfig::default() })]
    #[test]
    fn committed_tasks_are_the_plan_of_the_attempt_that_committed(
        commands in proptest::collection::vec(1..20_i32, 1..8),
        schedule in proptest::collection::vec(any::<bool>(), 0..24),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let journal = MemEventRepository::default();
            let executor = ledger_executor(Interfering::new(journal.clone(), schedule), 3);
            let mut published = Vec::new();
            for value in commands {
                match executor.execute("account", Command::Adjust(value)).await {
                    Ok(report) => {
                        let last = report.events.last().expect("an accepted adjustment commits events");
                        for task in &report.tasks {
                            published.push((last.sequence, task.clone()));
                        }
                    }
                    Err(CommandGatewayError::Conflict) => {}
                    Err(other) => panic!("unexpected result {other:?}"),
                }
            }
            let outbox = journal.outbox::<Ledger>();
            prop_assert_eq!(
                outbox.iter().map(|entry| (entry.aggregate_sequence, entry.task.clone())).collect::<Vec<_>>(),
                published.clone(),
                "the outbox holds exactly the reports' tasks, each with its commit's last event"
            );
            for (sequence, task) in published {
                prop_assert_eq!(
                    task.payload()["after"].as_i64().unwrap(),
                    balance_at(&journal, "account", sequence).await,
                    "every task was planned from the state its own attempt committed on"
                );
            }
            Ok(())
        })?;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_executors_on_one_journal_replan_the_loser_on_the_winners_state() {
    let journal = MemEventRepository::default();
    let shared = Interfering {
        gate: Some(Arc::new(tokio::sync::Barrier::new(2))),
        ..Interfering::new(journal.clone(), Vec::new())
    };
    // Two executors are two nodes: both load the empty ledger at the gate, one commits, the other conflicts
    // and decides again on the winner's state.
    let nodes = [
        Arc::new(ledger_executor(shared.clone(), 2)),
        Arc::new(ledger_executor(shared, 2)),
    ];
    let mut commands = tokio::task::JoinSet::new();
    for (node, value) in nodes.iter().cloned().zip([5, 7]) {
        commands.spawn(async move { (value, node.execute("account", Command::Adjust(value)).await) });
    }
    let mut reports = HashMap::new();
    while let Some(result) = commands.join_next().await {
        let (value, result) = result.unwrap();
        reports.insert(value, result.unwrap());
    }
    let outbox = journal.outbox::<Ledger>();
    assert_eq!(outbox.len(), 2, "one task per committed event");
    let afters: Vec<i64> = outbox
        .iter()
        .map(|entry| entry.task.payload()["after"].as_i64().unwrap())
        .collect();
    assert_eq!(afters[1], 12, "the second commit planned on the first one's state");
    assert!(afters[0] == 5 || afters[0] == 7);
    let loser = if afters[0] == 5 { 7 } else { 5 };
    assert_eq!(reports[&loser].tasks[0].payload()["after"], 12);
    assert_eq!(reports[&loser].events[0].sequence, 2);
}

#[tokio::test]
async fn a_conflict_that_makes_the_decision_impossible_publishes_nothing() {
    // The first attempt plans a task, then another node overdraws the account: the retry is refused by the
    // domain, and the first attempt's plan was never published.
    let journal = MemEventRepository::default();
    let interfering = Interfering {
        foreign: -1000,
        ..Interfering::new(journal.clone(), vec![true])
    };
    let executor = ledger_executor(interfering, 3);
    assert_eq!(
        executor.execute("account", Command::Adjust(10)).await,
        Err(CommandGatewayError::Domain(Rejected))
    );
    assert!(journal.outbox::<Ledger>().is_empty());
    assert_eq!(balance_at(&journal, "account", 1).await, -1000, "only the other node's event");
}
