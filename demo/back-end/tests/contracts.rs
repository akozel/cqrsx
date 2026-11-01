//! The contracts of the demo the operator and the front-end rely on, against golden files written by hand
//! (`tests/golden/`):
//!
//! - `effective_config.txt`: the configuration without a single environment variable. A setting read from a variable
//!   under the variable's name, with the value the process uses; what has no variable of its own (derived values, the
//!   plan of the connections) under a dotted name in lower case. Every variable in it is documented in
//!   `demo/.env.example` with the same default, and every variable the process reads is in it;
//! - `activity_feed.txt`: every kind of activity entry and the keys of its JSON on the SSE stream; a kind whose entries
//!   differ by an enumerated field the front-end reads has a line per value (`kind[field=value]`);
//! - `retry_pauses.txt`: the pause the task runtime waits after attempt `n` of a task failed (`<case>.<n>`): an
//!   e-mail (`mail`), a step of the chain between the aggregates (`step`), and a task type without a policy
//!   (`redelivery`, bounds: the library's backoff is random within them). The pauses are the policies of the task
//!   types (`MAIL`, `STEP`), applied by the runtime.
//!
//! Every file is `key = value` lines sorted by key, every key once. The entries and the pauses come from their
//! producers: the command observer, the projection observer, the rebuild service, the mail client and the task
//! runtime. The values of the command entries on every outcome, of the task attempts' entries and the settlements of
//! the tasks are pinned by tests of their own below.
//!
//! A change of a contract changes its golden file by hand in the same commit: a failing test names the lines only
//! the golden file has and the lines only the code has.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use axum::{Json, Router, http::StatusCode, routing::post};
use chrono::NaiveDate;
use cqrsx::{
    command::{
        CommandExecutor,
        metadata::CommandMetadata,
        observer::{CommandObserver, CommandReport, Outcome},
    },
    domain::event::{EventEnvelope, Events},
    journal::{EventStore, PersistenceError, feed::Position},
    memory::checkpoint_store::MemoryCheckpointStore,
    projection::{
        checkpoint::CheckpointStore,
        processor::observer::{Applied, Failed, FailureCategory, LeaseChange, ProjectionObserver, RebuildReason, Rebuilt},
    },
    reactions::ReactionsPlanner,
    task::{NewTask, Task, attempt::TaskAttempt, format::TaskFormat, policy::redelivery_backoff},
    worker::{
        TaskHandler, TaskResult,
        adapter::boxed::BoxedTaskAdapter,
        context::TaskContext,
        dependencies::Dependencies,
        dispatcher::TaskDispatcher,
        executor::{TaskExecutor, default_executor::DefaultTaskExecutor},
        queue::{
            TaskProcessingError, TaskQueue,
            lease::{LeasedTask, TaskLease},
            settlement::{LeaseRecoveryReport, SettlementRequest, SettlementResult, TaskFailure, TaskFailureCause},
        },
        runtime::prepare,
    },
};
use cqrsx::{projection::checkpoint::CheckpointKey, task::policy::Policies};
use cqrsx_engine::{
    Cqrs,
    build::{BuildError, Builder},
    settings::{Settings, duration_text},
    setup::Setup,
    views::JournalTotals,
};
use cqrsx_testing::{
    command_executor::TestCommandExecutor,
    memory_store::{MemoryStore, MemoryStoreAggregateContext},
    unavailable_store::UnavailableStore,
};
use encore_backend::{
    DEMO_DEFAULTS, concert,
    concert::{
        application::views::concert_hall::ConcertHallView,
        domain::{
            Concert, ConcertCommand, ConcertError, ConcertEvent, ConcertRules,
            commands::{AnnounceConcert, BookSeat, CancelBooking},
        },
    },
    fan,
    fan::application::views::fan_wallet::FanWalletView,
    fan::application::{
        issue_refund::IssueRefundV1,
        record_cancellation::RecordCancellationV1,
        record_ticket::RecordTicketV1,
        send_e_ticket::{SendETicketV1, send_e_ticket},
    },
    fan::domain::{
        Fan, FanCommand,
        commands::{RecordCancellation, RecordTicket},
    },
    operations,
    operations::application::{
        activity_observer::{ActivityObserver, CommandActivityObserver},
        journal::JournalStats,
        rebuild::{ReadSideLimits, ReadSideService},
    },
    shared::{
        application::{
            StorageError, TicketLimit,
            activity::ActivityHub,
            mail::Mailer,
            policies::{MAIL, STEP},
        },
        domain::{
            ConcertId, FanId, FanLedgerId,
            venue::{SeatId, Zone},
        },
    },
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const EFFECTIVE_CONFIG: &str = include_str!("golden/effective_config.txt");
const ACTIVITY_FEED: &str = include_str!("golden/activity_feed.txt");
const RETRY_PAUSES: &str = include_str!("golden/retry_pauses.txt");

/// `key = value` lines sorted by key; a key twice fails.
fn render(lines: impl IntoIterator<Item = (String, String)>) -> String {
    let mut lines: Vec<(String, String)> = lines.into_iter().collect();
    lines.sort();
    for pair in lines.windows(2) {
        assert_ne!(pair[0].0, pair[1].0, "the key {} twice", pair[0].0);
    }
    lines.iter().map(|(key, value)| format!("{key} = {value}\n")).collect()
}

/// A golden file as key → value.
fn parse(golden: &str) -> BTreeMap<&str, &str> {
    golden
        .lines()
        .map(|line| {
            line.split_once(" = ")
                .unwrap_or_else(|| panic!("not `key = value`: {line:?}"))
        })
        .collect()
}

/// Equal byte for byte, or a failure that names the lines only one side has.
fn assert_golden(name: &str, golden: &str, actual: &str) {
    if golden == actual {
        return;
    }
    let (golden_lines, actual_lines): (BTreeSet<&str>, BTreeSet<&str>) = (golden.lines().collect(), actual.lines().collect());
    let only = |a: &BTreeSet<&str>, b: &BTreeSet<&str>| a.difference(b).map(|line| format!("    {line}\n")).collect::<String>();
    panic!(
        "tests/golden/{name} and the code differ\n  only in the golden file:\n{}  only in the code:\n{}  (no line differs: the order or the line ends do)",
        only(&golden_lines, &actual_lines),
        only(&actual_lines, &golden_lines),
    );
}

/// `30 s` or `250 ms`, as `duration_text` writes them.
fn duration(text: &str) -> Duration {
    match text.split_once(' ') {
        Some((n, "s")) => Duration::from_secs(n.parse().unwrap()),
        Some((n, "ms")) => Duration::from_millis(n.parse().unwrap()),
        _ => panic!("not a duration: {text:?}"),
    }
}

// --- the process ---------------------------------------------------------------------------------------------

/// A lookup of variables by their full names.
type Lookup = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// The variables of a process (full names), as the engine and its modules read them.
#[derive(Clone)]
struct Config {
    lookup: Lookup,
}

impl Config {
    /// The variables of `lookup`.
    fn from_lookup(lookup: impl Fn(&str) -> Option<String> + Send + Sync + 'static) -> Result<Self, BuildError> {
        Ok(Self {
            lookup: Arc::new(lookup),
        })
    }

    /// The engine of the process over these variables, as `main.rs` builds it, in memory.
    fn engine(&self) -> Builder {
        let lookup = self.lookup.clone();
        Cqrs::from_lookup("ENCORE", move |name| lookup(name))
            .defaults(DEMO_DEFAULTS)
            .module(concert::module)
            .module(fan::module)
            .module(operations::module)
    }

    /// The engine's settings of these variables.
    fn settings(&self) -> Settings {
        Settings::from_lookup("ENCORE", &*self.lookup, &DEMO_DEFAULTS).unwrap()
    }

    /// The ticket limit the box office reads.
    fn ticket_limit(&self) -> usize {
        ready(self.engine().build()).unwrap().dependency::<TicketLimit>().unwrap().0
    }
}

/// The output of `future`, which must not wait: an engine in memory builds without one.
fn ready<F: std::future::Future>(future: F) -> F::Output {
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    match std::pin::pin!(future).poll(&mut context) {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("the future waits"),
    }
}

/// The policies of the task types of the process.
fn policies(config: &Config) -> Result<Policies, BuildError> {
    ready(config.engine().build()).map(|cqrs| cqrs.policies().clone())
}

/// The mail service of the process, one for every worker.
fn mailer(config: &Config) -> Result<Arc<Mailer>, BuildError> {
    let lookup = config.lookup.clone();
    Ok(fan::mailer(&Setup::new(
        "ENCORE",
        move |name| lookup(name),
        config.settings(),
    ))?)
}

/// The handlers of the four task types the modules register, in one dispatcher every worker shares: the chain steps
/// run their commands on `fans`, the e-mails go through `mail` and are reported to `activity`.
fn handlers<E: CommandExecutor<Fan> + 'static>(
    fans: Arc<E>,
    mail: Arc<Mailer>,
    activity: Arc<ActivityHub>,
) -> Result<TaskDispatcher<BoxedTaskAdapter>, BuildError> {
    let modules = Config::from_lookup(|_| None)?.engine().provide(mail).provide(activity);
    Ok(modules.task_handlers()?.executor(fans).build()?)
}

/// The rows of the read models' checkpoints, the concert halls first.
fn checkpoint_keys() -> Vec<CheckpointKey> {
    vec![
        CheckpointKey::of::<ConcertHallView, Concert>(),
        CheckpointKey::of::<FanWalletView, Fan>(),
    ]
}

// --- effective configuration ---------------------------------------------------------------------------------

/// The settings of the engine and what its modules read, without a single variable.
async fn effective_configuration() -> String {
    let config = Config::from_lookup(|_| None).unwrap();
    let cqrs = config.engine().build().await.unwrap();
    render(cqrs.description())
}

/// The keys of the settings read from a variable: the names of the variables.
fn variables(golden: &str) -> BTreeSet<&str> {
    parse(golden)
        .into_keys()
        .filter(|key| key.starts_with(|c: char| c.is_ascii_uppercase()))
        .collect()
}

#[tokio::test]
async fn the_effective_configuration_without_environment_is_the_golden_one() {
    assert_golden("effective_config.txt", EFFECTIVE_CONFIG, &effective_configuration().await);
}

#[tokio::test]
async fn every_variable_the_process_reads_is_in_the_golden_configuration() {
    let read = Arc::new(Mutex::new(BTreeSet::new()));
    let recording = read.clone();
    Config::from_lookup(move |name| {
        recording.lock().unwrap().insert(name.to_owned());
        None
    })
    .unwrap()
    .engine()
    .build()
    .await
    .unwrap();
    let read = read.lock().unwrap().clone();
    let described: BTreeSet<String> = variables(EFFECTIVE_CONFIG).into_iter().map(str::to_owned).collect();
    assert_eq!(read, described, "the variables read and the variables described");
    assert_eq!(
        (
            read.iter().filter(|name| name.starts_with("ENCORE_")).count(),
            read.iter().filter(|name| name.starts_with("OUTBOX_")).count()
        ),
        (32, 7)
    );
}

#[test]
fn the_golden_configuration_is_what_env_example_documents() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../.env.example");
    let example = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    // `# NAME=value`: every variable has a default, the commented line shows it.
    let documented: BTreeMap<&str, &str> = example
        .lines()
        .filter_map(|line| line.strip_prefix("# ")?.split_once('='))
        .filter(|(name, _)| name.chars().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_'))
        .collect();
    let golden = parse(EFFECTIVE_CONFIG);
    for name in variables(EFFECTIVE_CONFIG) {
        let value = golden[name];
        // The golden file writes durations as `duration_text` does; `.env.example` in the variable's unit.
        let in_unit = if name.ends_with("_MS") {
            duration(value).as_millis().to_string()
        } else if name.ends_with("_PERCENT") {
            value.strip_suffix(" %").unwrap().to_owned()
        } else {
            value.to_owned()
        };
        assert_eq!(
            documented.get(name).copied(),
            Some(in_unit.as_str()),
            "{name}: .env.example and tests/golden/effective_config.txt"
        );
    }
}

// --- activity feed -------------------------------------------------------------------------------------------

/// Totals and a lag for the rebuild service.
struct Journal;

#[async_trait::async_trait]
impl JournalStats for Journal {
    async fn totals(&self) -> Result<JournalTotals, StorageError> {
        Ok(JournalTotals {
            events: 9,
            aggregates: 3,
        })
    }

    async fn count_after(&self, _: &str, _: Position, _: u64) -> Result<u64, StorageError> {
        Ok(0)
    }
}

/// The mail service, scripted: the status of every answer, oldest first, then deliveries.
async fn mail_service(statuses: impl IntoIterator<Item = u16>) -> String {
    let script = Arc::new(Mutex::new(statuses.into_iter().collect::<VecDeque<u16>>()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().route(
        "/mail",
        post(move |Json(mail): Json<Value>| {
            let script = script.clone();
            async move {
                let status = script.lock().unwrap().pop_front().unwrap_or(200);
                let body = match status {
                    200 => json!({ "inbox_id": mail["id"], "duplicate": false }),
                    503 => json!({ "reason_code": "mailbox_busy" }),
                    _ => json!({ "error": "rejected" }),
                };
                (StatusCode::from_u16(status).unwrap(), Json(body))
            }
        }),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

/// The context of an attempt of an e-mail task as the task runtime builds it: the pause of the mail policy and the
/// composition's mail service at the address of `config`, writing to `activity`.
fn mail_context(attempt: TaskAttempt, config: &Config, activity: &Arc<ActivityHub>) -> TaskContext {
    let pause = policies(config).unwrap().pause(attempt.format(), attempt.number());
    TaskContext::new(attempt, pause).with_dependencies(
        Dependencies::default()
            .provide(mailer(config).unwrap())
            .provide(activity.clone()),
    )
}

/// Attempt `number` of a task with `payload`, in the causation chain of a command.
fn attempt<P: Task + Serialize>(payload: &P, number: i32) -> TaskAttempt {
    TaskAttempt::try_new(
        Uuid::now_v7(),
        TaskFormat::try_new(P::TYPE, P::VERSION).unwrap(),
        serde_json::to_value(payload).unwrap(),
        json!({ "correlation_id": Uuid::now_v7() }),
        number,
    )
    .unwrap()
}

fn concert() -> ConcertId {
    ConcertId::parse("radiohead-2026-11-21").unwrap()
}

fn fan() -> FanId {
    FanId::parse("fox").unwrap()
}

fn seat() -> SeatId {
    SeatId::parse("B4").unwrap()
}

fn e_ticket() -> SendETicketV1 {
    SendETicketV1 {
        booking_id: Uuid::now_v7(),
        concert_id: concert(),
        seat: seat(),
        fan_id: fan(),
        fan_name: "Fox".into(),
        price: 80,
    }
}

/// Polls the activity log until an entry of `kind` is in it.
async fn wait_for(activity: &ActivityHub, kind: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !activity.subscribe().0.iter().any(|entry| entry.kind == kind) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no {kind} entry within 5 s"));
}

/// The field whose value makes a line of its own in `activity_feed.txt`, per kind.
fn variant_field(kind: &str) -> Option<&'static str> {
    match kind {
        "command.failed" => Some("outcome"),
        "projection.rebuilt" => Some("reason"),
        "projection.lease_changed" => Some("lease"),
        "task.attempt.failed" => Some("retry"),
        _ => None,
    }
}

// Real time, not a paused clock: the mail requests are real HTTP, and a paused clock would fire their timeout at once.
#[tokio::test]
async fn the_activity_feed_has_thirteen_kinds_with_the_golden_fields() {
    let activity = Arc::new(ActivityHub::new(128, 128));

    // The executors' reports (`command.*`): every outcome, with a fan and without one.
    let task = NewTask::from_payload(TaskFormat::try_new("encore_record_ticket", 1).unwrap(), json!({})).unwrap();
    let sold_out = ConcertError::SoldOut;
    let commands = CommandActivityObserver::new(activity.clone());
    let announce = ConcertCommand::AnnounceConcert(AnnounceConcert {
        artist: "Radiohead".into(),
        city: "Tbilisi".into(),
        date: NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
    });
    let booking = CommandMetadata::root("ui");
    let book = book("B4", "fox", &booking);
    let concert_id = concert();
    let metadata = booking.to_map();
    for (command, outcome) in [
        (&book, Outcome::Accepted { tasks: &[task] }),
        (&announce, Outcome::Accepted { tasks: &[] }),
        (&book, Outcome::Rejected(&sold_out)),
        (&book, Outcome::Conflict),
        (&announce, Outcome::Unavailable),
    ] {
        commands.observe(&CommandReport::<Concert> {
            aggregate_id: &concert_id,
            command,
            metadata: &metadata,
            outcome,
        });
    }

    // The projection processor's reports (`projection.*`).
    let observer = ActivityObserver::new("concert_halls", "node-a", activity.clone());
    observer.applied(&Applied {
        aggregate_id: "radiohead-2026-11-21",
        last_sequence: 7,
        events: 2,
        position: Position::new(9),
    });
    for aggregate_id in [Some("radiohead-2026-11-21"), None] {
        observer.failed(&Failed {
            aggregate_id,
            handler: "concert_halls",
            category: FailureCategory::Connection,
            position: Position::new(9),
            failures: 1,
        });
    }
    for reason in [RebuildReason::Requested, RebuildReason::Healed] {
        observer.rebuilt(&Rebuilt {
            aggregate_id: "radiohead-2026-11-21",
            events: 41,
            reason,
        });
    }
    for event in [LeaseChange::Acquired, LeaseChange::Lost, LeaseChange::Released] {
        observer.lease(event);
    }

    // Encore! (`rebuild.*`): asked, played by the owners of both rows, watched to its end.
    let store = MemoryCheckpointStore::default();
    for key in checkpoint_keys() {
        store.register(&key).await.unwrap();
    }
    let rebuilds = ReadSideService::new(
        Arc::new(store.clone()),
        checkpoint_keys(),
        Arc::new(Journal),
        activity.clone(),
        ReadSideLimits {
            lag_cap: 100,
            max_pace: Duration::from_secs(1),
            status_timeout: Duration::from_secs(1),
        },
        CancellationToken::new(),
    );
    rebuilds.rebuild(Duration::ZERO).await.unwrap();
    for key in checkpoint_keys() {
        let owner = store.claim(&key, "node-a", Duration::from_secs(10)).await.unwrap().unwrap();
        assert!(
            store
                .take_rebuild_requests(&key, owner.token, 10)
                .await
                .unwrap()
                .unwrap()
                .rebuild_pace
                .is_some()
        );
        assert!(store.begin_rebuild(&key, owner.token).await.unwrap());
        assert!(store.advance_rebuild(&key, owner.token, "k", 2).await.unwrap());
        assert!(store.finish_rebuild(&key, owner.token).await.unwrap());
        store.release(&key, owner.token).await.unwrap();
    }
    wait_for(&activity, "rebuild.finished").await;

    // The attempts of an e-mail task (`task.attempt.*`): delivered, refused for now, rejected.
    let config = config_with_mail(&mail_service([200, 503, 400]).await);
    let handler = send_e_ticket;
    let attempt = |payload: &SendETicketV1, number| mail_context(attempt(payload, number), &config, &activity);
    let payload = e_ticket();
    assert!(handler.handle(payload.clone(), &attempt(&payload, 1)).await == TaskResult::Done);
    assert!(matches!(
        handler.handle(payload.clone(), &attempt(&payload, 2)).await,
        TaskResult::Retry { .. }
    ));
    assert!(matches!(
        handler.handle(payload.clone(), &attempt(&payload, 3)).await,
        TaskResult::Fail { .. }
    ));

    let mut shapes: BTreeMap<String, String> = BTreeMap::new();
    let mut kinds = BTreeSet::new();
    for entry in activity.subscribe().0 {
        let Value::Object(json) = serde_json::to_value(&*entry).unwrap() else {
            panic!("an entry is a JSON object");
        };
        let keys: BTreeSet<&str> = json.keys().map(String::as_str).collect();
        let keys = keys.into_iter().collect::<Vec<_>>().join(", ");
        let key = match variant_field(&entry.kind) {
            Some(field) => match &json[field] {
                Value::String(value) => format!("{}[{field}={value}]", entry.kind),
                other => format!("{}[{field}={other}]", entry.kind),
            },
            None => entry.kind.clone(),
        };
        if let Some(seen) = shapes.insert(key.clone(), keys.clone()) {
            assert_eq!(seen, keys, "{key}: one shape per kind");
        }
        kinds.insert(entry.kind.clone());
    }
    assert_eq!(kinds.len(), 13, "{kinds:?}");
    assert_golden("activity_feed.txt", ACTIVITY_FEED, &render(shapes));
}

// --- command entries: the values -----------------------------------------------------------------------------

/// The box office's store in memory with switches: every commit conflicts, every load fails, every load waits at a
/// closed gate (holding the concert's lock).
#[derive(Clone, Default)]
struct Switched {
    inner: MemoryStore<Concert>,
    conflict: Arc<AtomicBool>,
    unavailable: Arc<AtomicBool>,
    gate: Arc<Mutex<Option<Arc<Semaphore>>>>,
}

impl Switched {
    /// From now on every load waits for a permit of the returned gate.
    fn close_gate(&self) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        *self.gate.lock().unwrap() = Some(gate.clone());
        gate
    }
}

impl EventStore<Concert> for Switched {
    type Context = MemoryStoreAggregateContext<Concert>;

    async fn load_events(&self, id: &str) -> Result<Vec<EventEnvelope<Concert>>, PersistenceError> {
        self.inner.load_events(id).await
    }

    async fn load_aggregate(&self, id: &str) -> Result<Self::Context, PersistenceError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(PersistenceError::Connection("unreachable".into()));
        }
        let gate = self.gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }
        self.inner.load_aggregate(id).await
    }

    async fn commit(
        &self,
        events: Vec<ConcertEvent>,
        context: Self::Context,
        metadata: HashMap<String, String>,
        tasks: &[NewTask],
    ) -> Result<Vec<EventEnvelope<Concert>>, PersistenceError> {
        if self.conflict.load(Ordering::SeqCst) {
            return Err(PersistenceError::Conflict);
        }
        self.inner.commit(events, context, metadata, tasks).await
    }
}

/// What produces the command entries: the executor of the box office over `Switched` and the one of the ledgers over
/// memory, both planning with their reactions and reporting to the activity feed's observer as the process does.
struct Feed {
    concerts: TestCommandExecutor<Concert, Switched>,
    fans: TestCommandExecutor<Fan>,
}

impl Feed {
    fn new(store: Switched, activity: &Arc<ActivityHub>) -> Self {
        let config = Config::from_lookup(|_| None).unwrap();
        let policies = policies(&config).unwrap();
        let observer = Arc::new(CommandActivityObserver::new(activity.clone()));
        let rules = ConcertRules {
            ticket_limit: config.ticket_limit(),
        };
        let concerts = TestCommandExecutor::configured(store, rules, |executor| {
            executor
                .with_task_planner(ReactionsPlanner::<Concert>::new(policies.clone()))
                .with_observer(observer.clone())
        });
        let fans = TestCommandExecutor::configured(MemoryStore::default(), (), |executor| {
            executor
                .with_task_planner(ReactionsPlanner::<Fan>::new(policies))
                .with_observer(observer)
        });
        Self { concerts, fans }
    }

    /// Runs `command` on the concert.
    async fn concert(&self, command: ConcertCommand, metadata: CommandMetadata) {
        let _ = self
            .concerts
            .execute_with_metadata(&concert(), command, metadata.to_map())
            .await;
    }

    /// Runs `command` on Fox's ledger for the concert.
    async fn fan(&self, command: FanCommand, metadata: CommandMetadata) {
        let _ = self.fans.execute_with_metadata(&ledger(), command, metadata.to_map()).await;
    }
}

fn ledger() -> FanLedgerId {
    FanLedgerId::new(&fan(), &concert())
}

fn book(seat: &str, fan_id: &str, metadata: &CommandMetadata) -> ConcertCommand {
    ConcertCommand::BookSeat(BookSeat {
        seat: seat.into(),
        fan_id: FanId::parse(fan_id).unwrap(),
        fan_name: fan_id.to_uppercase(),
        booking_id: metadata.causation.command_id,
    })
}

fn cancel(seat: &str, fan_id: &str, metadata: &CommandMetadata) -> ConcertCommand {
    ConcertCommand::CancelBooking(CancelBooking {
        seat: seat.into(),
        fan_id: FanId::parse(fan_id).unwrap(),
        cancellation_id: metadata.causation.command_id,
    })
}

/// The metadata of the command a task of `payload` issues, in the causation chain of `cause`.
fn step<P: Task + Serialize>(payload: &P, cause: &CommandMetadata) -> CommandMetadata {
    let attempt = TaskAttempt::try_new(
        Uuid::now_v7(),
        TaskFormat::of::<P>(),
        serde_json::to_value(payload).unwrap(),
        json!({ "correlation_id": cause.causation.correlation_id }),
        1,
    )
    .unwrap();
    CommandMetadata::from_task(&attempt)
}

/// A `command.*` entry as the feed shows it: `kind`, the aggregate (type and id), the ids and the source of `metadata`,
/// the command (type and fan) and the fields of its outcome.
fn command_entry(
    kind: &str,
    aggregate: (&str, &str),
    metadata: &CommandMetadata,
    command: (&str, Value),
    outcome: Value,
) -> (String, Value) {
    let mut fields = json!({
        "aggregate_type": aggregate.0,
        "aggregate_id": aggregate.1,
        "command_id": metadata.causation.command_id,
        "correlation_id": metadata.causation.correlation_id,
        "source": metadata.causation.source,
        "command_type": command.0,
        "fan_id": command.1,
    });
    if let (Value::Object(fields), Value::Object(outcome)) = (&mut fields, outcome) {
        fields.extend(outcome);
    }
    (kind.to_owned(), fields)
}

/// Every outcome of a command of both aggregates, and the values of its entry: accepted with and without tasks,
/// rejected with its code and params, a conflict, an unreachable store, a refused lock; a fan from the command's label,
/// from the ledger's id, nobody's.
#[tokio::test(start_paused = true)]
async fn command_entries_carry_their_values_on_every_outcome_of_both_aggregates() {
    let activity = Arc::new(ActivityHub::new(64, 64));
    let store = Switched::default();
    let feed = Feed::new(store.clone(), &activity);
    let concert_id = concert();
    let box_office = ("Concert", concert_id.as_str());

    let seed = CommandMetadata::root("seed");
    let announce = ConcertCommand::AnnounceConcert(AnnounceConcert {
        artist: "Radiohead".into(),
        city: "Tbilisi".into(),
        date: NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
    });
    feed.concert(announce, seed.clone()).await;
    let booking = CommandMetadata::root("ui");
    feed.concert(book("A1", "fox", &booking), booking.clone()).await;
    let taken = CommandMetadata::root("ui");
    feed.concert(book("A1", "panda", &taken), taken.clone()).await;
    let cancellation = CommandMetadata::root("ui");
    feed.concert(cancel("A1", "fox", &cancellation), cancellation.clone()).await;
    let not_booked = CommandMetadata::root("ui");
    feed.concert(cancel("A1", "panda", &not_booked), not_booked.clone()).await;
    // Every attempt meets a concurrent change; the clock is paused, the backoff passes at once.
    store.conflict.store(true, Ordering::SeqCst);
    let conflict = CommandMetadata::root("ui");
    feed.concert(book("B4", "fox", &conflict), conflict.clone()).await;
    store.conflict.store(false, Ordering::SeqCst);
    store.unavailable.store(true, Ordering::SeqCst);
    let unreachable = CommandMetadata::root("ui");
    feed.concert(cancel("A1", "owl", &unreachable), unreachable.clone()).await;
    store.unavailable.store(false, Ordering::SeqCst);
    // The holder keeps the concert's lock while its load waits at the gate; the next command waits for the lock as
    // long as it may (the idle runtime advances the paused clock) and is refused.
    let gate = store.close_gate();
    let holding = CommandMetadata::root("ui");
    let mut holder = Box::pin(feed.concert(book("C1", "cat", &holding), holding.clone()));
    assert!(futures::poll!(holder.as_mut()).is_pending(), "the holder waits at the gate");
    let refused = CommandMetadata::root("ui");
    feed.concert(book("C2", "dog", &refused), refused.clone()).await;
    gate.add_permits(1);
    holder.await;

    // The ledger: the steps of the chain, as the task handlers issue them.
    let ticket = RecordTicketV1 {
        booking_id: booking.causation.command_id,
        concert_id: concert(),
        seat: SeatId::parse("A1").unwrap(),
        zone: Zone::Vip,
        price: 120,
        fan_id: fan(),
        fan_name: "FOX".into(),
    };
    let record = || {
        FanCommand::RecordTicket(RecordTicket {
            booking_id: ticket.booking_id,
            concert_id: ticket.concert_id.clone(),
            seat: ticket.seat.clone(),
            zone: ticket.zone,
            price: ticket.price,
            fan_name: ticket.fan_name.clone(),
        })
    };
    let recorded = step(&ticket, &booking);
    feed.fan(record(), recorded.clone()).await;
    let redelivered = step(&ticket, &booking);
    feed.fan(record(), redelivered.clone()).await;
    let refund = RecordCancellationV1 {
        booking_id: ticket.booking_id,
        cancellation_id: cancellation.causation.command_id,
        concert_id: concert(),
        seat: ticket.seat.clone(),
        fan_id: fan(),
        refund: 120,
    };
    let refunded = step(&refund, &cancellation);
    let record_cancellation = FanCommand::RecordCancellation(RecordCancellation {
        booking_id: refund.booking_id,
        cancellation_id: refund.cancellation_id,
        refund: refund.refund,
    });
    feed.fan(record_cancellation, refunded.clone()).await;

    let ledger_id = ledger();
    let ledger = ("Fan", ledger_id.as_str());
    let entries: Vec<(String, Value)> = activity
        .subscribe()
        .0
        .iter()
        .map(|entry| (entry.kind.clone(), Value::Object(entry.fields.clone())))
        .collect();
    let unavailable = json!({ "outcome": "unavailable", "message": "not run: storage unavailable or aggregate busy" });
    assert_eq!(
        entries,
        [
            command_entry(
                "command.accepted",
                box_office,
                &seed,
                ("AnnounceConcert", Value::Null),
                json!({ "tasks": [] })
            ),
            command_entry(
                "command.accepted",
                box_office,
                &booking,
                ("BookSeat", json!("fox")),
                json!({ "tasks": ["encore_record_ticket v1"] })
            ),
            command_entry(
                "command.rejected",
                box_office,
                &taken,
                ("BookSeat", json!("panda")),
                json!({ "code": "seat_taken", "params": { "seat": "A1", "by": "FOX" } })
            ),
            command_entry(
                "command.accepted",
                box_office,
                &cancellation,
                ("CancelBooking", json!("fox")),
                json!({ "tasks": ["encore_record_cancellation v1"] })
            ),
            command_entry(
                "command.rejected",
                box_office,
                &not_booked,
                ("CancelBooking", json!("panda")),
                json!({ "code": "seat_not_booked", "params": { "seat": "A1" } })
            ),
            command_entry(
                "command.failed",
                box_office,
                &conflict,
                ("BookSeat", json!("fox")),
                json!({ "outcome": "conflict", "message": "concurrent change; retries exhausted" })
            ),
            command_entry(
                "command.failed",
                box_office,
                &unreachable,
                ("CancelBooking", json!("owl")),
                unavailable.clone()
            ),
            command_entry(
                "command.failed",
                box_office,
                &refused,
                ("BookSeat", json!("dog")),
                unavailable
            ),
            command_entry(
                "command.accepted",
                box_office,
                &holding,
                ("BookSeat", json!("cat")),
                json!({ "tasks": ["encore_record_ticket v1"] })
            ),
            command_entry(
                "command.accepted",
                ledger,
                &recorded,
                ("RecordTicket", json!("fox")),
                json!({ "tasks": ["encore_send_e_ticket v1"] })
            ),
            command_entry(
                "command.rejected",
                ledger,
                &redelivered,
                ("RecordTicket", json!("fox")),
                json!({ "code": "already_recorded", "params": { "booking_id": ticket.booking_id } })
            ),
            command_entry(
                "command.accepted",
                ledger,
                &refunded,
                ("RecordCancellation", json!("fox")),
                json!({ "tasks": ["encore_issue_refund v1"] })
            ),
        ]
    );
    assert_eq!(
        (recorded.causation.source.as_str(), refunded.causation.source.as_str()),
        ("task:encore_record_ticket", "task:encore_record_cancellation"),
        "a step's entry names its task as the source"
    );
    assert_eq!(
        (recorded.causation.correlation_id, refunded.causation.correlation_id),
        (booking.causation.correlation_id, cancellation.causation.correlation_id),
        "and the correlation of the command that published the task"
    );
}

// --- retry pauses --------------------------------------------------------------------------------------------

/// The pause a failed attempt was settled with.
fn pause(settled: &[(Uuid, SettlementRequest)], task: &LeasedTask) -> Duration {
    match settled.iter().find(|(id, _)| *id == task.attempt().task_id()) {
        Some((_, SettlementRequest::Retry { delay, .. })) => *delay,
        other => panic!("expected a retry with a pause, got {other:?}"),
    }
}

/// Runs the tasks of `queue` through the task runtime of the composition (`dispatcher`, the policies and the runtime
/// settings of `config`) until `count` of them are settled; their settlements.
async fn run_through_the_runtime(
    queue: &Arc<MemQueue>,
    dispatcher: TaskDispatcher<BoxedTaskAdapter>,
    config: &Config,
    count: usize,
) -> Vec<(Uuid, SettlementRequest)> {
    let runtime = prepare(
        queue.clone(),
        Arc::new(dispatcher),
        config.settings().task_runtime().clone(),
        policies(config).unwrap(),
    );
    let shutdown = CancellationToken::new();
    let running = tokio::spawn(runtime.run(shutdown.clone()));
    tokio::time::timeout(Duration::from_secs(20), async {
        while queue.settled.lock().unwrap().len() < count {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("every task settled");
    shutdown.cancel();
    running.await.unwrap().unwrap();
    queue.settled()
}

#[tokio::test]
async fn retry_pauses_by_attempt_are_the_golden_ones() {
    let activity = Arc::new(ActivityHub::new(256, 256));
    // E-mails: the provider refuses every delivery for now.
    let config = config_with_mail(&mail_service(std::iter::repeat_n(503, 16)).await);
    // Chain steps: the fan ledger is unreachable.
    let fans = Arc::new(TestCommandExecutor::new(UnavailableStore, ()));
    let ticket = RecordTicketV1 {
        booking_id: Uuid::now_v7(),
        concert_id: concert(),
        seat: seat(),
        zone: Zone::Standard,
        price: 80,
        fan_id: fan(),
        fan_name: "Fox".into(),
    };
    let cancellation = RecordCancellationV1 {
        booking_id: ticket.booking_id,
        cancellation_id: Uuid::now_v7(),
        concert_id: concert(),
        seat: seat(),
        fan_id: fan(),
        refund: 80,
    };
    let refund = IssueRefundV1 {
        cancellation_id: cancellation.cancellation_id,
        booking_id: ticket.booking_id,
        concert_id: concert(),
        seat: seat(),
        fan_id: fan(),
        amount: 80,
    };
    let e_ticket = e_ticket();
    // Attempt n of every task, all through the task runtime: its handlers, its policies, its settlements.
    let queue = Arc::new(MemQueue::default());
    let attempts: Vec<[LeasedTask; 4]> = (1..=8)
        .map(|n| {
            [
                leased(&e_ticket, n),
                leased(&refund, n),
                leased(&ticket, n),
                leased(&cancellation, n),
            ]
        })
        .collect();
    queue.incoming.lock().unwrap().extend(attempts.iter().flatten().cloned());
    let dispatcher = handlers(fans, mailer(&config).unwrap(), activity.clone()).unwrap();
    let settled = run_through_the_runtime(&queue, dispatcher, &config, 32).await;

    let mut lines = Vec::new();
    for (n, [e_ticket_attempt, refund_attempt, ticket_attempt, cancellation_attempt]) in (1..).zip(&attempts) {
        let mail = pause(&settled, e_ticket_attempt);
        let refund_mail = pause(&settled, refund_attempt);
        assert_eq!(refund_mail, mail, "attempt {n}: both e-mail tasks pause alike");
        let step = pause(&settled, ticket_attempt);
        let cancellation_step = pause(&settled, cancellation_attempt);
        assert_eq!(cancellation_step, step, "attempt {n}: both chain steps pause alike");
        lines.push((format!("mail.{n}"), duration_text(mail)));
        lines.push((format!("step.{n}"), duration_text(step)));
    }

    // A task type without a policy waits a draw of the queue's backoff, within bounds the library has no function for:
    // the golden bounds, checked by sampling. The draws fill the range: its lowest and highest tenth both occur.
    let golden = parse(RETRY_PAUSES);
    for n in 1..=8 {
        let key = format!("redelivery.{n}");
        let (low, high) = golden[key.as_str()].split_once(" ..= ").unwrap();
        let (low, high) = (duration(low), duration(high));
        let tenth = (high - low) / 10;
        let draws: Vec<Duration> = (0..2000).map(|_| redelivery_backoff(n)).collect();
        assert!(
            draws.iter().all(|draw| (low..=high).contains(draw)),
            "{key}: a draw outside {low:?}..={high:?}"
        );
        assert!(draws.iter().min().unwrap() <= &(low + tenth), "{key}: no draw near {low:?}");
        assert!(draws.iter().max().unwrap() >= &(high - tenth), "{key}: no draw near {high:?}");
        lines.push((key.clone(), golden[key.as_str()].to_owned()));
    }
    assert_golden("retry_pauses.txt", RETRY_PAUSES, &render(lines));
}

/// The pauses of the task policies, in the lines of the golden file: an e-mail is `MAIL`, a step of the chain `STEP`,
/// both under the configuration without environment; the runtime applies them (above).
#[test]
fn the_task_policies_pause_as_the_golden_ones() {
    let config = Config::from_lookup(|_| None).unwrap();
    let policies = policies(&config).unwrap();
    let mut lines = Vec::new();
    for (case, name) in [("mail", MAIL), ("step", STEP)] {
        let retry = policies.policy(name).unwrap().retry();
        for n in 1..=8 {
            lines.push((format!("{case}.{n}"), duration_text(retry.pause(n))));
        }
    }
    let golden: String = RETRY_PAUSES
        .lines()
        .filter(|line| line.starts_with("mail.") || line.starts_with("step."))
        .map(|line| format!("{line}\n"))
        .collect();
    assert_eq!(golden.lines().count(), 16, "eight attempts of each");
    assert_golden("retry_pauses.txt", &golden, &render(lines));
}

// --- task attempts: the values -------------------------------------------------------------------------------

/// The outbox in memory: hands out the tasks it was given, oldest first, by format and up to the limit, under a lease
/// from the claim; records every settlement request. Recovery finds nothing.
#[derive(Default)]
struct MemQueue {
    incoming: Mutex<VecDeque<LeasedTask>>,
    settled: Mutex<Vec<(Uuid, SettlementRequest)>>,
}

impl MemQueue {
    fn settled(&self) -> Vec<(Uuid, SettlementRequest)> {
        self.settled.lock().unwrap().clone()
    }
}

impl TaskQueue for MemQueue {
    async fn claim(
        &self,
        format: &TaskFormat,
        _: &str,
        limit: usize,
        lease: Duration,
    ) -> Result<Vec<LeasedTask>, TaskProcessingError> {
        let deadline = tokio::time::Instant::now() + lease;
        let mut incoming = self.incoming.lock().unwrap();
        let (mut claimed, mut rest) = (Vec::new(), VecDeque::new());
        while let Some(task) = incoming.pop_front() {
            if claimed.len() < limit && task.attempt().format() == format {
                claimed.push(LeasedTask::new(
                    task.attempt().clone(),
                    TaskLease::new(task.lease().token(), deadline),
                ));
            } else {
                rest.push_back(task);
            }
        }
        *incoming = rest;
        Ok(claimed)
    }

    async fn settle(&self, task: &LeasedTask, request: SettlementRequest) -> Result<SettlementResult, TaskProcessingError> {
        let result = match &request {
            SettlementRequest::Completed => SettlementResult::Completed,
            SettlementRequest::Retry { .. } => SettlementResult::RetryScheduled,
            SettlementRequest::Failed { .. } => SettlementResult::Failed(TaskFailure {
                task_id: task.attempt().task_id(),
                format: task.attempt().format().clone(),
                attempt_number: task.attempt().number(),
                cause: TaskFailureCause::HandlerFailure,
            }),
        };
        self.settled.lock().unwrap().push((task.attempt().task_id(), request));
        Ok(result)
    }

    async fn recover_expired(&self, _: usize) -> Result<LeaseRecoveryReport, TaskProcessingError> {
        Ok(LeaseRecoveryReport::default())
    }
}

/// Attempt `number` of a task with `payload` under a lease of a minute.
fn leased<P: Task + Serialize>(payload: &P, number: i32) -> LeasedTask {
    LeasedTask::new(
        attempt(payload, number),
        TaskLease::new(Uuid::new_v4(), tokio::time::Instant::now() + Duration::from_secs(60)),
    )
}

/// The mail service, scripted: the status and the body of every answer, oldest first.
async fn scripted_mail(answers: impl IntoIterator<Item = (u16, Value)>) -> String {
    let script = Arc::new(Mutex::new(answers.into_iter().collect::<VecDeque<(u16, Value)>>()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().route(
        "/mail",
        post(move || {
            let script = script.clone();
            async move {
                let (status, body) = script.lock().unwrap().pop_front().expect("an answer for every request");
                (StatusCode::from_u16(status).unwrap(), Json(body))
            }
        }),
    );
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    url
}

/// An address nobody listens on.
async fn closed_mail() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    format!("http://{}", listener.local_addr().unwrap())
}

/// The configuration without environment, with the mail service at `url`.
fn config_with_mail(url: &str) -> Config {
    let url = url.to_owned();
    Config::from_lookup(move |name| (name == "ENCORE_MAIL_SERVICE_URL").then(|| url.clone())).unwrap()
}

/// The handlers of the composition with its mail service at the address of `config`, writing to `activity`; the
/// steps of the ledger on a ledger nobody reaches.
fn mail_dispatcher(config: &Config, activity: &Arc<ActivityHub>) -> TaskDispatcher<BoxedTaskAdapter> {
    let fans = Arc::new(TestCommandExecutor::new(UnavailableStore, ()));
    handlers(fans, mailer(config).unwrap(), activity.clone()).unwrap()
}

/// The handlers of the composition, the steps of the ledger on `fans`.
fn step_dispatcher<E: CommandExecutor<Fan> + 'static>(fans: Arc<E>) -> TaskDispatcher<BoxedTaskAdapter> {
    let config = Config::from_lookup(|_| None).unwrap();
    handlers(fans, mailer(&config).unwrap(), Arc::new(ActivityHub::new(1, 1))).unwrap()
}

/// The executor of the task runtime over `queue`, with the storage budget and the policies of `config`.
fn task_executor(queue: &Arc<MemQueue>, config: &Config) -> DefaultTaskExecutor<MemQueue> {
    DefaultTaskExecutor::new(queue.clone(), *config.settings().task_storage()).with_policies(policies(config).unwrap())
}

/// Runs `task` through `dispatcher` as a worker does and settles its result in the executor's queue.
async fn run_attempt(
    executor: &DefaultTaskExecutor<MemQueue>,
    dispatcher: &TaskDispatcher<BoxedTaskAdapter>,
    task: LeasedTask,
    config: &Config,
) {
    let handler_timeout = config.settings().handler_timeout();
    executor.execute_and_settle(dispatcher, task, handler_timeout).await.unwrap();
}

/// The `task.attempt.*` entries of `activity`, each as its kind and its fields.
fn attempt_entries(activity: &ActivityHub) -> Vec<(String, Value)> {
    activity
        .subscribe()
        .0
        .iter()
        .filter(|entry| entry.kind.starts_with("task.attempt."))
        .map(|entry| (entry.kind.clone(), Value::Object(entry.fields.clone())))
        .collect()
}

/// An entry of attempt `task` of an e-mail with `inbox_id`: `kind` with the fields of every attempt and `extra`.
fn attempt_entry(kind: &str, task: &LeasedTask, inbox_id: &str, extra: Value) -> (String, Value) {
    let attempt = task.attempt();
    let mut fields = json!({
        "task_id": attempt.task_id(),
        "format": attempt.format().to_string(),
        "attempt": attempt.number(),
        "correlation_id": attempt.metadata()["correlation_id"],
        "inbox_id": inbox_id,
    });
    if let (Value::Object(fields), Value::Object(extra)) = (&mut fields, extra) {
        fields.extend(extra);
    }
    (kind.to_owned(), fields)
}

/// Every answer of the mail service, through the executor of the runtime: the values of every `task.attempt.*` entry
/// the mail client writes (the pause of a retry by the attempt's number, the provider's reason, a duplicate, the inbox
/// id, the message of a rejection) and the settlement of every attempt, whose pause is the one the entry shows.
#[tokio::test]
async fn every_mail_attempt_is_logged_with_its_values_and_settled_with_the_pause_it_logs() {
    let activity = Arc::new(ActivityHub::new(64, 64));
    let ticket_mail = "e_ticket:00000000-0000-0000-0000-000000000001";
    let refund_mail = "refund:00000000-0000-0000-0000-000000000002";
    let config = config_with_mail(
        &scripted_mail([
            (200, json!({ "inbox_id": ticket_mail, "duplicate": false })),
            (200, json!({ "inbox_id": ticket_mail, "duplicate": true })),
            (200, json!({})),
            (503, json!({ "reason_code": "mailbox_busy" })),
            (503, json!("busy")),
            (500, json!({})),
            (429, json!({})),
            (502, json!({})),
            (400, json!({ "error": "bad address" })),
        ])
        .await,
    );
    let unreachable = config_with_mail(&closed_mail().await);
    let mails = mail_dispatcher(&config, &activity);
    let closed = mail_dispatcher(&unreachable, &activity);
    let queue = Arc::new(MemQueue::default());
    let executor = task_executor(&queue, &config);
    let e_ticket = SendETicketV1 {
        booking_id: Uuid::from_u128(1),
        concert_id: concert(),
        seat: seat(),
        fan_id: fan(),
        fan_name: "Fox".into(),
        price: 80,
    };
    let refund = IssueRefundV1 {
        cancellation_id: Uuid::from_u128(2),
        booking_id: Uuid::from_u128(1),
        concert_id: concert(),
        seat: seat(),
        fan_id: fan(),
        amount: 80,
    };
    let delivered = leased(&e_ticket, 1);
    let duplicate = leased(&e_ticket, 2);
    let refunded = leased(&refund, 1);
    let refused: Vec<LeasedTask> = (1..=5).map(|number| leased(&e_ticket, number)).collect();
    let unreached = leased(&e_ticket, 3);
    let rejected = leased(&e_ticket, 1);
    for task in [&delivered, &duplicate, &refunded].into_iter().chain(&refused) {
        run_attempt(&executor, &mails, task.clone(), &config).await;
    }
    run_attempt(&executor, &closed, unreached.clone(), &config).await;
    run_attempt(&executor, &mails, rejected.clone(), &config).await;

    let started = |task: &LeasedTask, inbox_id: &str| attempt_entry("task.attempt.started", task, inbox_id, json!({}));
    let retried = |task: &LeasedTask, reason: &str, pause_ms: u64| {
        attempt_entry(
            "task.attempt.failed",
            task,
            ticket_mail,
            json!({ "reason_code": reason, "retry": true, "retry_after_ms": pause_ms }),
        )
    };
    let mut expected = vec![
        started(&delivered, ticket_mail),
        attempt_entry(
            "task.attempt.succeeded",
            &delivered,
            ticket_mail,
            json!({ "duplicate": false }),
        ),
        started(&duplicate, ticket_mail),
        attempt_entry(
            "task.attempt.succeeded",
            &duplicate,
            ticket_mail,
            json!({ "duplicate": true }),
        ),
        started(&refunded, refund_mail),
        attempt_entry(
            "task.attempt.succeeded",
            &refunded,
            refund_mail,
            json!({ "duplicate": false }),
        ),
    ];
    let refusals = [
        ("mailbox_busy", 1000),
        ("mail_unavailable", 2000),
        ("mail_unavailable", 4000),
        ("mail_unavailable", 8000),
        ("mail_unavailable", 8000),
    ];
    for (task, (reason, pause_ms)) in refused.iter().zip(refusals) {
        expected.extend([started(task, ticket_mail), retried(task, reason, pause_ms)]);
    }
    expected.extend([
        started(&unreached, ticket_mail),
        retried(&unreached, "mail_unreachable", 4000),
        started(&rejected, ticket_mail),
        attempt_entry(
            "task.attempt.failed",
            &rejected,
            ticket_mail,
            json!({ "reason_code": "mail_rejected", "retry": false, "message": "mail service answered 400 Bad Request" }),
        ),
    ]);
    let entries = attempt_entries(&activity);
    assert_eq!(entries, expected);

    let id = |task: &LeasedTask| task.attempt().task_id();
    let retry = |task: &LeasedTask, reason: &str, seconds: u64| {
        (
            id(task),
            SettlementRequest::Retry {
                reason: reason.to_owned(),
                delay: Duration::from_secs(seconds),
            },
        )
    };
    let mut settled = vec![
        (id(&delivered), SettlementRequest::Completed),
        (id(&duplicate), SettlementRequest::Completed),
        (id(&refunded), SettlementRequest::Completed),
    ];
    for (task, (reason, pause_ms)) in refused.iter().zip(refusals) {
        settled.push(retry(task, reason, pause_ms / 1000));
    }
    settled.extend([
        retry(&unreached, "mail_unreachable", 4),
        (
            id(&rejected),
            SettlementRequest::Failed {
                reason: "mail service answered 400 Bad Request".into(),
            },
        ),
    ]);
    assert_eq!(queue.settled(), settled);
    // The pause a retry was settled with is the one its entry shows.
    let logged: Vec<Value> = entries
        .iter()
        .filter_map(|(_, fields)| fields.get("retry_after_ms").cloned())
        .collect();
    let applied: Vec<Value> = queue
        .settled()
        .iter()
        .filter_map(|(_, request)| match request {
            SettlementRequest::Retry { delay, .. } => Some(json!(delay.as_millis() as u64)),
            _ => None,
        })
        .collect();
    assert_eq!(logged, applied);
}

/// Every answer of the ledger to a step of the chain, through the executor of the runtime: R9 and R11 are done, R10
/// and an unavailable ledger are retried after the step's pause, with the reasons the outbox keeps.
#[tokio::test]
async fn every_answer_of_the_ledger_settles_its_step_as_the_table_of_answers_says() {
    let config = Config::from_lookup(|_| None).unwrap();
    let store = MemoryStore::<Fan>::default();
    let fans = Arc::new(TestCommandExecutor::configured(store.clone(), (), |executor| {
        executor.with_task_planner(ReactionsPlanner::<Fan>::new(policies(&config).unwrap()))
    }));
    let steps = step_dispatcher(fans.clone());
    let unavailable = step_dispatcher(Arc::new(TestCommandExecutor::new(UnavailableStore, ())));
    let queue = Arc::new(MemQueue::default());
    let executor = task_executor(&queue, &config);
    let ticket = RecordTicketV1 {
        booking_id: Uuid::from_u128(1),
        concert_id: concert(),
        seat: seat(),
        zone: Zone::Standard,
        price: 80,
        fan_id: fan(),
        fan_name: "Fox".into(),
    };
    let cancellation = RecordCancellationV1 {
        booking_id: Uuid::from_u128(1),
        cancellation_id: Uuid::from_u128(2),
        concert_id: concert(),
        seat: seat(),
        fan_id: fan(),
        refund: 80,
    };
    let overtaking = leased(&cancellation, 3);
    let recorded = leased(&ticket, 1);
    let redelivered = leased(&ticket, 2);
    let cancelled = leased(&cancellation, 4);
    let cancelled_again = leased(&cancellation, 5);
    let ticket_down = leased(&ticket, 1);
    let cancellation_down = leased(&cancellation, 6);
    for task in [&overtaking, &recorded, &redelivered, &cancelled, &cancelled_again] {
        run_attempt(&executor, &steps, task.clone(), &config).await;
    }
    for task in [&ticket_down, &cancellation_down] {
        run_attempt(&executor, &unavailable, task.clone(), &config).await;
    }

    let id = |task: &LeasedTask| task.attempt().task_id();
    let retry = |task: &LeasedTask, reason: &str| {
        (
            id(task),
            SettlementRequest::Retry {
                reason: reason.to_owned(),
                delay: Duration::from_secs(1),
            },
        )
    };
    assert_eq!(
        queue.settled(),
        [
            retry(&overtaking, "unknown_ticket"),
            (id(&recorded), SettlementRequest::Completed),
            (id(&redelivered), SettlementRequest::Completed),
            (id(&cancelled), SettlementRequest::Completed),
            (id(&cancelled_again), SettlementRequest::Completed),
            retry(&ticket_down, "unavailable"),
            retry(&cancellation_down, "unavailable"),
        ]
    );
    let events = store.load_events(ledger().as_str()).await.unwrap();
    assert_eq!(
        events.iter().map(|event| event.payload.event_type()).collect::<Vec<_>>(),
        ["TicketRecorded", "TicketCancelled"],
        "R9 and R11 wrote nothing"
    );
    let published: Vec<String> = fans
        .take_published_tasks()
        .iter()
        .map(|task| task.format().to_string())
        .collect();
    assert_eq!(published, ["encore_send_e_ticket v1", "encore_issue_refund v1"]);
}
