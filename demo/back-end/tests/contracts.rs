//! What the migration to the engine API (backlog epic 05) must not change, against golden files written by hand from
//! `demo/.env.example`, `cqrsx/doc/components.md` and ticket 05/02 (`tests/golden/`):
//!
//! - `effective_config.txt`: the configuration without a single environment variable. A setting read from a variable
//!   under the variable's name, with the value the process uses; what has no variable of its own (derived values, the
//!   plan of the connections) under a dotted name in lower case;
//! - `activity_feed.txt`: every kind of activity entry and the keys of its JSON on the SSE stream; a kind whose entries
//!   differ by an enumerated field the front-end reads has a line per value (`kind[field=value]`);
//! - `retry_pauses.txt`: the pause a task asks the queue for after its attempt `n` failed (`<case>.<n>`): an e-mail
//!   (`mail`), a step of the chain between the aggregates (`step`), and a handler that names no pause
//!   (`redelivery`, bounds: the library's backoff is random within them).
//!
//! Every file is `key = value` lines sorted by key, every key once. The entries and the pauses come from today's
//! producers: the command service, the projection observer, the rebuild service, the mail client and the task
//! handlers.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use axum::{Json, Router, http::StatusCode, routing::post};
use chrono::NaiveDate;
use cqrsx::{
    cqrs::{
        CommitReport,
        command::{CommandExecutor, CommandGatewayError},
        projection::{
            checkpoint::CheckpointStore,
            processor::observer::{Applied, Failed, LeaseEvent, ProjectionObserver, RebuildReason, Rebuilt},
        },
        store::feed::Position,
        testing::{command::TestCommandExecutor, mem_checkpoint_store::MemCheckpointStore, unavailable_store::UnavailableStore},
    },
    task::{
        NewTask,
        attempt::TaskAttempt,
        format::{TaskFormat, TaskPayload},
        handler::{TaskHandler, TaskHandlerError},
        queue::retry::redelivery_backoff,
    },
};
use encore_backend::{
    bootstrap::{projections::checkpoint_keys, tasks::mail_settings},
    concert::domain::{Concert, ConcertCommand, ConcertError, commands::AnnounceConcert},
    config::{Config, duration_text},
    fan::application::{
        app::FanApp,
        issue_refund::{IssueRefundHandler, IssueRefundV1},
        record_cancellation::{RecordCancellationHandler, RecordCancellationV1},
        record_ticket::{RecordTicketHandler, RecordTicketV1},
        send_e_ticket::{SendETicketHandler, SendETicketV1},
    },
    operations::application::{
        activity_observer::ActivityObserver,
        ledger::{EventLedgerStats, LedgerTotals},
        rebuild::{ReadSide, ReadSideLimits, RebuildService},
    },
    process::{mongo_plan, pool_plan},
    shared::{
        application::{
            StorageError,
            activity::ActivityHub,
            command_service::{CommandLabel, CommandMetadata, CommandService},
        },
        domain::{
            ConcertId, FanId,
            venue::{SeatId, Zone},
        },
        infrastructure::http_mailer::{HttpMailer, MailClientSettings},
    },
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};
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

/// `30 s` or `250 ms`, as `config::duration_text` writes them.
fn duration(text: &str) -> Duration {
    match text.split_once(' ') {
        Some((n, "s")) => Duration::from_secs(n.parse().unwrap()),
        Some((n, "ms")) => Duration::from_millis(n.parse().unwrap()),
        _ => panic!("not a duration: {text:?}"),
    }
}

// --- effective configuration ---------------------------------------------------------------------------------

fn effective_configuration() -> String {
    let config = Config::from_lookup(|_| None).unwrap();
    let mut lines = config.describe();
    lines.extend(pool_plan(&config).describe());
    lines.extend(mongo_plan(&config).describe());
    render(lines)
}

/// The keys of the settings read from a variable: the names of the variables.
fn variables(golden: &str) -> BTreeSet<&str> {
    parse(golden)
        .into_keys()
        .filter(|key| key.starts_with(|c: char| c.is_ascii_uppercase()))
        .collect()
}

#[test]
fn the_effective_configuration_without_environment_is_the_golden_one() {
    assert_golden("effective_config.txt", EFFECTIVE_CONFIG, &effective_configuration());
}

#[test]
fn every_variable_the_process_reads_is_in_the_golden_configuration() {
    let read = RefCell::new(BTreeSet::new());
    Config::from_lookup(|name| {
        read.borrow_mut().insert(name.to_owned());
        None
    })
    .unwrap();
    let read = read.into_inner();
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
        // The golden file writes durations as `config::duration_text` does; `.env.example` in the variable's unit.
        let in_unit = if name.ends_with("_MS") {
            duration(value).as_millis().to_string()
        } else if name.ends_with("_SECS") {
            duration(value).as_secs().to_string()
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

/// Answers every command with the next of its results, oldest first.
struct Scripted(Mutex<VecDeque<Result<CommitReport<Concert>, CommandGatewayError<ConcertError>>>>);

impl CommandExecutor<Concert> for Scripted {
    async fn execute_with_metadata(
        &self,
        _: &str,
        _: ConcertCommand,
        _: HashMap<String, String>,
    ) -> Result<CommitReport<Concert>, CommandGatewayError<ConcertError>> {
        self.0.lock().unwrap().pop_front().expect("a result for every command")
    }
}

/// Totals and a lag for the rebuild service.
struct Ledger;

#[async_trait::async_trait]
impl EventLedgerStats for Ledger {
    async fn totals(&self) -> Result<LedgerTotals, StorageError> {
        Ok(LedgerTotals {
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

fn mailer(settings: &MailClientSettings, activity: &Arc<ActivityHub>) -> HttpMailer {
    HttpMailer::new(HttpMailer::client(settings).unwrap(), settings.clone(), activity.clone())
}

/// Attempt `number` of a task with `payload`, in the causation chain of a command.
fn attempt<P: TaskPayload + Serialize>(payload: &P, number: i32) -> TaskAttempt {
    TaskAttempt::new(
        Uuid::now_v7(),
        TaskFormat::new(P::TASK_TYPE, P::TASK_VERSION).unwrap(),
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
        "projection.lease" => Some("lease"),
        "task.attempt.failed" => Some("retry"),
        _ => None,
    }
}

// Real time, not a paused clock: the mail requests are real HTTP, and a paused clock would fire their timeout at once.
#[tokio::test]
async fn the_activity_feed_has_thirteen_kinds_with_the_golden_fields() {
    let activity = Arc::new(ActivityHub::new(128, 128));

    // The outcomes of a command (`command.*`), with a fan and without one.
    let task = NewTask::from_payload(TaskFormat::new("encore_record_ticket", 1).unwrap(), json!({})).unwrap();
    let results = [
        Ok(CommitReport {
            events: Vec::new(),
            tasks: vec![task],
        }),
        Ok(CommitReport {
            events: Vec::new(),
            tasks: Vec::new(),
        }),
        Err(CommandGatewayError::Domain(ConcertError::SoldOut)),
        Err(CommandGatewayError::Conflict),
        Err(CommandGatewayError::Unavailable),
    ];
    let commands = CommandService::<Concert, _>::new(Scripted(Mutex::new(results.into())), activity.clone());
    let announce = || {
        ConcertCommand::AnnounceConcert(AnnounceConcert {
            artist: "Radiohead".into(),
            city: "Tbilisi".into(),
            date: NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
        })
    };
    for fan_id in [Some("fox"), None, Some("fox"), Some("fox"), None] {
        let label = CommandLabel {
            aggregate_id: "radiohead-2026-11-21",
            command_type: if fan_id.is_some() { "BookSeat" } else { "AnnounceConcert" },
            fan_id,
        };
        commands.run(label, announce(), CommandMetadata::ui()).await;
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
            category: "connection",
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
    for event in [LeaseEvent::Acquired, LeaseEvent::Lost, LeaseEvent::Released] {
        observer.lease(event);
    }

    // Encore! (`rebuild.*`): asked, played by the owners of both rows, watched to its end.
    let store = MemCheckpointStore::default();
    for key in checkpoint_keys() {
        store.register(&key).await.unwrap();
    }
    let rebuilds = RebuildService::new(
        Arc::new(store.clone()),
        checkpoint_keys(),
        Arc::new(Ledger),
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
                .take_requests(&key, owner.token, 10)
                .await
                .unwrap()
                .unwrap()
                .rebuild
                .is_some()
        );
        assert!(store.begin_rebuild(&key, owner.token).await.unwrap());
        assert!(store.advance_rebuild(&key, owner.token, "k", 2).await.unwrap());
        assert!(store.finish_rebuild(&key, owner.token).await.unwrap());
        store.release(&key, owner.token).await.unwrap();
    }
    wait_for(&activity, "rebuild.finished").await;

    // The attempts of an e-mail task (`task.attempt.*`): delivered, refused for now, rejected.
    let settings = MailClientSettings {
        base_url: mail_service([200, 503, 400]).await,
        ..mail_settings(&Config::from_lookup(|_| None).unwrap())
    };
    let mut handler = SendETicketHandler {
        mailer: mailer(&settings, &activity),
    };
    let payload = e_ticket();
    assert!(handler.handle(payload.clone(), &attempt(&payload, 1)).await.is_ok());
    assert!(matches!(
        handler.handle(payload.clone(), &attempt(&payload, 2)).await,
        Err(TaskHandlerError::Retry { .. })
    ));
    assert!(matches!(
        handler.handle(payload.clone(), &attempt(&payload, 3)).await,
        Err(TaskHandlerError::Permanent { .. })
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

// --- retry pauses --------------------------------------------------------------------------------------------

/// The pause a failed attempt asks for.
fn pause(result: Result<(), TaskHandlerError>) -> Duration {
    match result {
        Err(TaskHandlerError::Retry {
            retry_after: Some(pause),
            ..
        }) => pause,
        other => panic!("expected a retry with a pause, got {other:?}"),
    }
}

#[tokio::test]
async fn retry_pauses_by_attempt_are_the_golden_ones() {
    let activity = Arc::new(ActivityHub::new(256, 256));
    let config = Config::from_lookup(|_| None).unwrap();
    // E-mails: the provider refuses every delivery for now.
    let settings = MailClientSettings {
        base_url: mail_service(std::iter::repeat_n(503, 16)).await,
        ..mail_settings(&config)
    };
    let mut e_ticket_handler = SendETicketHandler {
        mailer: mailer(&settings, &activity),
    };
    let mut refund_handler = IssueRefundHandler {
        mailer: mailer(&settings, &activity),
    };
    // Chain steps: the fan ledger is unreachable.
    let fans = Arc::new(FanApp::new(CommandService::new(
        TestCommandExecutor::new(UnavailableStore, ()),
        activity.clone(),
    )));
    let mut record_ticket = RecordTicketHandler { fans: fans.clone() };
    let mut record_cancellation = RecordCancellationHandler { fans };
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

    let mut lines = Vec::new();
    for n in 1..=8 {
        let mail = pause(e_ticket_handler.handle(e_ticket.clone(), &attempt(&e_ticket, n)).await);
        let refund_mail = pause(refund_handler.handle(refund.clone(), &attempt(&refund, n)).await);
        assert_eq!(refund_mail, mail, "attempt {n}: both e-mail tasks pause alike");
        let step = pause(record_ticket.handle(ticket.clone(), &attempt(&ticket, n)).await);
        let cancellation_step = pause(
            record_cancellation
                .handle(cancellation.clone(), &attempt(&cancellation, n))
                .await,
        );
        assert_eq!(cancellation_step, step, "attempt {n}: both chain steps pause alike");
        lines.push((format!("mail.{n}"), duration_text(mail)));
        lines.push((format!("step.{n}"), duration_text(step)));
    }

    // Without a pause of the handler's the queue draws one within bounds the library has no function for: the golden
    // bounds, checked by sampling. The draws fill the range: its lowest and highest tenth both occur.
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
