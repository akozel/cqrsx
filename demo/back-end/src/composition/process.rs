//! The process without a single variable and with some: the engine's settings of the demo ([`DEMO_DEFAULTS`]), the
//! values the modules read, one executor per aggregate type and the activity log the e-mails report to.
use crate::{
    DEMO_DEFAULTS,
    concert::{
        self,
        domain::{Concert, ConcertCommand, commands::AnnounceConcert},
    },
    fan::{
        self,
        application::send_e_ticket::SendETicketV1,
        domain::{Fan, FanCommand, commands::RecordTicket},
    },
    shared::{
        application::{
            TicketLimit,
            activity::ActivityHub,
            policies::{MAIL, STEP},
        },
        domain::{
            ConcertId, FanId, FanLedgerId,
            venue::{SeatId, Zone},
        },
        infrastructure::http_mailer::MailClientSettings,
    },
    testing::{app, test_app},
};
use chrono::NaiveDate;
use cqrsx::{
    cqrs::{
        command::{
            CommandGatewayError,
            locks::{DEFAULT_LOCK_MAX_WAIT, DEFAULT_LOCK_MAX_WAITERS, LockLimits},
            retry::AggregateConflictRetryPolicy,
        },
        projection::processor::config::ProcessorConfig,
    },
    engine::{
        Cqrs,
        build::BuildError,
        settings::{DEFAULT_SNAPSHOT_EVERY, Settings, SettingsError},
        setup::Setup,
    },
    task::{
        attempt::TaskAttempt,
        format::TaskFormat,
        policy::{Retry, TaskPolicy},
        result::TaskResult,
    },
};
use std::{collections::HashMap, sync::Arc, time::Duration};

/// The owner name a process writes on the checkpoints it holds, unless `ENCORE_NODE_NAME` names it.
const DEFAULT_NODE_NAME: &str = "encore-backend";

/// What the process makes of the variables `pairs`: the engine's settings and the client of the mail service.
struct Configured {
    settings: Settings,
    mail: MailClientSettings,
}

async fn from_pairs(pairs: &[(&str, &str)]) -> Result<Configured, BuildError> {
    let cqrs = app(pairs).build().await?;
    let owned: HashMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    let setup = Setup::new("ENCORE", move |name| owned.get(name).cloned(), cqrs.settings().clone());
    Ok(Configured {
        settings: cqrs.settings().clone(),
        mail: fan::mail_settings(&setup).unwrap(),
    })
}

/// G-01, G-05, G-07: the engine of the process builds its executors and processors with the settings of the demo,
/// which are the library's lock limits and conflict retry, a snapshot every ten events (the engine's default too), the
/// library's projection settings but the demo's settle, this node and the task runtime's budgets of the demo.
#[test]
fn the_engine_settings_are_the_configuration_and_the_library_constants() {
    let settings = Settings::from_lookup("ENCORE", |_| None, &DEMO_DEFAULTS).unwrap();
    let library = LockLimits::try_new(DEFAULT_LOCK_MAX_WAIT, DEFAULT_LOCK_MAX_WAITERS.get()).unwrap();
    assert_eq!(
        (settings.snapshot_every(), settings.conflict_retry(), settings.lock_limits()),
        (DEFAULT_SNAPSHOT_EVERY, AggregateConflictRetryPolicy::default(), library)
    );
    assert_eq!(
        (settings.snapshot_every().get(), settings.lock_limits(), settings.processor()),
        (
            10,
            library,
            ProcessorConfig::default().with_settle(Duration::from_millis(10)).unwrap()
        )
    );
    assert_eq!(
        (settings.node_name(), settings.handler_timeout(), settings.task_lease()),
        (Some(DEFAULT_NODE_NAME), Duration::from_secs(5), Duration::from_secs(30))
    );
    assert_eq!(settings.task_storage().storage_timeout(), Duration::from_secs(3));
}

#[tokio::test]
async fn from_lookup_trims_encore_variables_and_passes_outbox_variables_as_they_are() {
    let config = from_pairs(&[
        ("ENCORE_SNAPSHOT_EVERY", " 25 "),
        ("ENCORE_NODE_NAME", "   "),
        ("ENCORE_MAIL_SERVICE_URL", "http://mail:8025/"),
        ("OUTBOX_WORKER_CONCURRENCY", "4"),
        ("OUTBOX_QUEUE_CAPACITY", "4"),
    ])
    .await
    .unwrap();
    assert_eq!(config.settings.snapshot_every().get(), 25, "an ENCORE_* value is trimmed");
    assert_eq!(
        config.settings.node_name(),
        Some(DEFAULT_NODE_NAME),
        "a blank ENCORE_* value counts as unset"
    );
    assert_eq!(config.mail.base_url, "http://mail:8025", "without the trailing slash");
    assert_eq!(config.settings.task_runtime().worker_pool().concurrency(), 4);
    assert_eq!(
        config.settings.task_runtime().worker_pool().lease_duration(),
        Duration::from_secs(30),
        "an absent OUTBOX_* variable takes the demo's default"
    );
    for (name, raw) in [("OUTBOX_LEASE_SECS", ""), ("OUTBOX_LEASE_SECS", " 30")] {
        assert!(
            matches!(
                from_pairs(&[(name, raw)]).await,
                Err(BuildError::Settings(SettingsError::TaskRuntime(_)))
            ),
            "an OUTBOX_* value reaches the library untrimmed: {raw:?}"
        );
    }
    let error = from_pairs(&[("ENCORE_TICKET_LIMIT", "0")]).await.err().unwrap().to_string();
    assert_eq!(error, "ENCORE_TICKET_LIMIT: must be positive");
}

/// G-18: the demo's task runtime fits the lease it checks at start-up.
#[test]
fn outbox_defaults_satisfy_the_lease_budget() {
    let settings = Settings::from_lookup("ENCORE", |_| None, &DEMO_DEFAULTS).expect("demo OUTBOX_* defaults must validate");
    let runtime = settings.task_runtime();
    assert_eq!(runtime.worker_pool().concurrency(), 2);
    assert_eq!(runtime.worker_pool().lease_duration(), Duration::from_secs(30));
    assert_eq!(runtime.worker_pool().handler_timeout(), Duration::from_secs(5));
}

/// The modules read their own variables: without them their defaults (the configuration's of before), with them the
/// variables' values.
#[tokio::test]
async fn the_modules_default_to_the_values_of_the_configuration() {
    let defaults = app(&[]).build().await.unwrap();
    let configured = app(&[("ENCORE_TICKET_LIMIT", "7"), ("ENCORE_MAIL_SERVICE_URL", "http://mail:8025")])
        .build()
        .await
        .unwrap();
    let limit = |cqrs: &Cqrs| *cqrs.dependency::<TicketLimit>().unwrap();
    assert_eq!((limit(&defaults), limit(&configured)), (TicketLimit(4), TicketLimit(7)));
    let formats = [
        "encore_record_ticket",
        "encore_record_cancellation",
        "encore_send_e_ticket",
        "encore_issue_refund",
    ]
    .map(|name| TaskFormat::new(name, 1).unwrap());
    let step = TaskPolicy::new(Retry::fixed(Duration::from_secs(1)), 6).unwrap();
    let mail = TaskPolicy::new(Retry::backoff(Duration::from_secs(1), Duration::from_secs(8)), 6).unwrap();
    assert_eq!(
        formats.clone().map(|format| defaults.policies().of(&format).cloned()),
        [Some(step), Some(step), Some(mail), Some(mail)]
    );
    assert_eq!(
        (
            defaults.policies().name_of(&formats[0]),
            defaults.policies().name_of(&formats[2])
        ),
        (Some(STEP), Some(MAIL))
    );
    let mail_of = |cqrs: &Cqrs, url: &'static str| {
        let lookup = move |name: &str| (name == "ENCORE_MAIL_SERVICE_URL" && !url.is_empty()).then(|| url.to_owned());
        fan::mail_settings(&Setup::new("ENCORE", lookup, cqrs.settings().clone())).unwrap()
    };
    let default_mail = mail_of(&defaults, "");
    assert_eq!(
        (default_mail.base_url, default_mail.timeout),
        ("http://127.0.0.1:8090".to_owned(), Duration::from_secs(4))
    );
    assert_eq!(mail_of(&configured, "http://mail:8025").base_url, "http://mail:8025");
}

/// G-11: the engine of the process runs one executor per aggregate type the modules register, and the views they
/// register; an aggregate type no module registers runs nowhere.
#[tokio::test]
async fn the_engine_runs_every_aggregate_and_view_the_modules_register() {
    let (cqrs, _) = test_app().await;
    let names: Vec<String> = cqrs.views().keys().iter().map(|key| key.projection().to_owned()).collect();
    assert_eq!(names, ["concert_halls", "fan_wallets"]);
    let concert_id = ConcertId::parse("radiohead-2026-11-21").unwrap();
    let announce = AnnounceConcert {
        artist: "Radiohead".into(),
        city: "Tbilisi".into(),
        date: NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
    };
    let ui = cqrs.from("ui");
    ui.execute::<Concert>(&concert_id, ConcertCommand::AnnounceConcert(announce))
        .await
        .unwrap();
    let ledger = FanLedgerId::new(&FanId::parse("fox").unwrap(), &concert_id);
    let record = RecordTicket {
        booking_id: uuid::Uuid::from_u128(1),
        concert_id: concert_id.clone(),
        seat: SeatId::parse("A1").unwrap(),
        zone: Zone::Vip,
        price: 120,
        fan_name: "Fox".into(),
    };
    ui.execute::<Fan>(&ledger, FanCommand::RecordTicket(record.clone()))
        .await
        .unwrap();
    let without_fan = Cqrs::in_memory().module(concert::module).build().await.unwrap();
    let refused = without_fan
        .from("ui")
        .execute::<Fan>(&ledger, FanCommand::RecordTicket(record))
        .await;
    assert!(matches!(refused, Err(CommandGatewayError::Unavailable)), "{refused:?}");
}

/// The e-mails of the process report their attempts to the hub of the activity log the HTTP API streams.
#[tokio::test]
async fn the_task_handlers_report_their_attempts_to_the_feeds_hub() {
    let cqrs = app(&[("ENCORE_MAIL_SERVICE_URL", "http://127.0.0.1:1")])
        .build()
        .await
        .unwrap();
    let task = SendETicketV1 {
        booking_id: uuid::Uuid::from_u128(1),
        concert_id: ConcertId::parse("radiohead-2026-11-21").unwrap(),
        seat: SeatId::parse("B4").unwrap(),
        fan_id: FanId::parse("fox").unwrap(),
        fan_name: "Fox".into(),
        price: 80,
    };
    let attempt = TaskAttempt::first(&task, serde_json::json!({})).unwrap();
    let answer = cqrs
        .dispatcher()
        .unwrap()
        .dispatch(Arc::new(attempt), Duration::from_secs(1))
        .await;
    assert_eq!(
        answer,
        TaskResult::retry("mail_unreachable"),
        "nobody listens on the test's mail address"
    );
    let kinds: Vec<String> = cqrs
        .dependency::<ActivityHub>()
        .unwrap()
        .subscribe()
        .0
        .iter()
        .map(|entry| entry.kind.clone())
        .collect();
    assert_eq!(kinds, ["task.attempt.started", "task.attempt.failed"]);
}
