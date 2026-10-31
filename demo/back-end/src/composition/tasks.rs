//! The tasks of the box office as the modules register them (moved from `bootstrap/tasks.rs` with the composition):
//! their formats and payloads, the policies of their types from the variables, the rows the reactions publish, the
//! handlers in one dispatcher with what they depend on.
use crate::{
    concert::{
        self,
        domain::{
            Concert, ConcertCommand, ConcertRules,
            commands::{AnnounceConcert, BookSeat, CancelBooking},
        },
    },
    fan::{
        self,
        application::{
            issue_refund::IssueRefundV1,
            record_cancellation::RecordCancellationV1,
            record_ticket::RecordTicketV1,
            send_e_ticket::{SendETicketV1, send_e_ticket},
        },
        domain::{
            Fan, FanCommand,
            commands::{RecordCancellation, RecordTicket},
        },
    },
    shared::{
        application::{
            activity::ActivityHub,
            mail::Mailer,
            policies::{CHAIN_RETRY_AFTER, MAIL, STEP},
            test_support::RecordingMailer,
        },
        domain::{
            ConcertId, FanId, FanLedgerId,
            venue::{SeatId, Zone},
        },
        infrastructure::http_mailer::MailClientSettings,
    },
    testing::{app, ready},
};
use chrono::NaiveDate;
use cqrsx::{
    command::{CommandExecutor, causation::Causation},
    engine::{
        Cqrs,
        build::BuildError,
        settings::Settings,
        setup::{Setup, SetupError},
    },
    reactions::ReactionsPlanner,
    task::{
        TaskSchedule,
        adapter::boxed::BoxedTaskAdapter,
        dispatcher::TaskDispatcher,
        format::TaskFormat,
        handlers::TaskHandlers,
        policy::{Policies, Retry, TaskPolicy},
    },
    testing::{command_executor::TestCommandExecutor, mem_store::MemStore},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;
use std::{num::NonZeroU16, sync::Arc, time::Duration};
use uuid::Uuid;

/// The policies of the four task types under the variables `pairs`, as the `fan` module declares them.
fn policies(pairs: &[(&str, &str)]) -> Result<Policies, BuildError> {
    ready(app(pairs).build()).map(|cqrs| cqrs.policies().clone())
}

/// The setup of a module under the variables `pairs` in the demo's engine.
fn setup(pairs: &'static [(&'static str, &'static str)]) -> Setup {
    let settings = Settings::from_lookup("ENCORE", |_| None, &crate::DEMO_DEFAULTS).unwrap();
    Setup::new(
        "ENCORE",
        move |name| pairs.iter().find(|(k, _)| *k == name).map(|(_, v)| (*v).to_owned()),
        settings,
    )
}

/// The client of the external mail service under the variables `pairs`.
fn mail_settings(pairs: &'static [(&'static str, &'static str)]) -> Result<MailClientSettings, SetupError> {
    fan::mail_settings(&setup(pairs))
}

/// The mail service of the process, one for every worker.
fn mailer() -> Result<Arc<Mailer>, SetupError> {
    fan::mailer(&setup(&[]))
}

/// The handlers of the four task types the `fan` module registers, in one dispatcher every worker shares: the chain
/// steps between the two aggregates run their commands on `fans`, the e-mails they end in go through `mail` and are
/// reported to `activity`.
fn handlers<E: CommandExecutor<Fan> + 'static>(
    fans: Arc<E>,
    mail: Arc<Mailer>,
    activity: Arc<ActivityHub>,
) -> Result<TaskDispatcher<BoxedTaskAdapter>, BuildError> {
    let modules = app(&[]).provide(mail).provide(activity);
    Ok(modules.task_handlers()?.executor(fans).build()?)
}

/// The formats of the four task types, in the order of the policies.
fn formats() -> [TaskFormat; 4] {
    [
        TaskFormat::of::<RecordTicketV1>(),
        TaskFormat::of::<RecordCancellationV1>(),
        TaskFormat::of::<SendETicketV1>(),
        TaskFormat::of::<IssueRefundV1>(),
    ]
}

/// The mail client's former backoff after a refused delivery (`MailClientSettings::retry_after` until backlog
/// 05/09), word for word: the oracle the mail policy is compared with.
fn mail_client_backoff(base: Duration, max: Duration, attempt: i32) -> Duration {
    let exponent = u32::try_from(attempt.max(1) - 1).unwrap_or(u32::MAX).min(16);
    base.saturating_mul(2u32.saturating_pow(exponent)).min(max)
}

#[test]
fn formats_are_snake_case_v1_and_tasks_carry_the_budget_of_their_policy() {
    for (format, expected) in [
        (TaskFormat::of::<RecordTicketV1>(), "encore_record_ticket v1"),
        (TaskFormat::of::<SendETicketV1>(), "encore_send_e_ticket v1"),
        (TaskFormat::of::<RecordCancellationV1>(), "encore_record_cancellation v1"),
        (TaskFormat::of::<IssueRefundV1>(), "encore_issue_refund v1"),
    ] {
        assert_eq!(format.to_string(), expected);
    }
    let policies = policies(&[]).unwrap();
    for format in formats() {
        assert_eq!(policies.max_attempts(&format).get(), 6, "{format}");
    }
}

fn round_trip<T: Serialize + DeserializeOwned>(document: serde_json::Value) {
    let payload: T = serde_json::from_value(document.clone()).unwrap();
    assert_eq!(serde_json::to_value(&payload).unwrap(), document);
}

/// Wire contract of the task payloads (`transactional_outbox.payload`): tasks already queued
/// must still decode after a deploy, so the literals must never change.
#[test]
fn task_payloads_keep_their_queued_json() {
    let booking_id = "00000000-0000-0000-0000-000000000001";
    let cancellation_id = "00000000-0000-0000-0000-000000000002";
    let concert_id = "radiohead-2026-11-21";
    round_trip::<RecordTicketV1>(json!({ "booking_id": booking_id, "concert_id": concert_id, "seat": "B4",
        "zone": "standard", "price": 80, "fan_id": "fox", "fan_name": "Fox" }));
    round_trip::<SendETicketV1>(json!({ "booking_id": booking_id, "concert_id": concert_id, "seat": "B4",
        "fan_id": "fox", "fan_name": "Fox", "price": 80 }));
    round_trip::<RecordCancellationV1>(json!({ "booking_id": booking_id, "cancellation_id": cancellation_id,
        "concert_id": concert_id, "seat": "B4", "fan_id": "fox", "refund": 80 }));
    round_trip::<IssueRefundV1>(json!({ "cancellation_id": cancellation_id, "booking_id": booking_id,
        "concert_id": concert_id, "seat": "B4", "fan_id": "fox", "amount": 80 }));
}

/// The steps of the chain are `STEP`, the e-mails `MAIL`; their pauses and their budget come from the
/// configuration.
#[test]
fn every_task_type_has_its_policy_from_the_configuration() {
    let defaults = policies(&[]).unwrap();
    assert_eq!(
        formats().map(|format| defaults.name_of(&format)),
        [Some(STEP), Some(STEP), Some(MAIL), Some(MAIL)]
    );
    assert_eq!(
        (defaults.policy(STEP), defaults.policy(MAIL)),
        (
            Some(&TaskPolicy::new(
                Retry::fixed(Duration::from_secs(1)),
                NonZeroU16::new(6).unwrap()
            )),
            Some(&TaskPolicy::new(
                Retry::backoff(Duration::from_secs(1), Duration::from_secs(8)),
                NonZeroU16::new(6).unwrap()
            ))
        )
    );
    let config = [
        ("ENCORE_TASK_MAX_ATTEMPTS", "3"),
        ("ENCORE_TASK_RETRY_BASE_MS", "300"),
        ("ENCORE_TASK_RETRY_MAX_MS", "5000"),
    ];
    let changed = policies(&config).unwrap();
    assert_eq!(
        (changed.policy(STEP), changed.policy(MAIL)),
        (
            Some(&TaskPolicy::new(
                Retry::fixed(Duration::from_secs(1)),
                NonZeroU16::new(3).unwrap()
            )),
            Some(&TaskPolicy::new(
                Retry::backoff(Duration::from_millis(300), Duration::from_secs(5)),
                NonZeroU16::new(3).unwrap()
            ))
        )
    );
    let refused = policies(&[("ENCORE_TASK_MAX_ATTEMPTS", "0")]).unwrap_err();
    assert!(matches!(
        refused,
        BuildError::Setup(SetupError::Invalid { ref variable, .. }) if variable == "ENCORE_TASK_MAX_ATTEMPTS"
    ));
    assert_eq!(
        refused.to_string(),
        r#"ENCORE_TASK_MAX_ATTEMPTS: cannot parse "0": number would be zero for non-zero type"#,
        "an attempt budget is positive"
    );
}

/// The mail policy pauses exactly as the mail client asked until it stopped choosing its pauses (backlog 05/09), and
/// the step policy as a step did.
#[test]
fn the_mail_policy_pauses_as_the_mail_client_did() {
    let attempts: Vec<i32> = (-3..=40).chain([i32::MIN, i32::MAX]).collect();
    for (base, max) in [
        (Duration::from_secs(1), Duration::from_secs(8)),
        (Duration::from_millis(300), Duration::from_secs(5)),
        (Duration::from_millis(1), Duration::from_secs(3600)),
        (Duration::ZERO, Duration::ZERO),
        (Duration::from_secs(5), Duration::from_secs(2)),
    ] {
        // A base above the maximum is refused by the module, as the configuration refused it: the policy of such a
        // pair is the one the module would declare.
        let (base_ms, max_ms) = (base.as_millis().to_string(), max.as_millis().to_string());
        let declared = policies(&[("ENCORE_TASK_RETRY_BASE_MS", &base_ms), ("ENCORE_TASK_RETRY_MAX_MS", &max_ms)]);
        let mail = match declared {
            Ok(policies) => policies.policy(MAIL).unwrap().retry(),
            Err(_) if base > max => Retry::backoff(base, max),
            Err(error) => panic!("{error}"),
        };
        for &attempt in &attempts {
            assert_eq!(
                mail.pause(attempt),
                mail_client_backoff(base, max, attempt),
                "{base:?} to {max:?}, attempt {attempt}"
            );
        }
    }
    let step = policies(&[]).unwrap().policy(STEP).unwrap().retry();
    for &attempt in &attempts {
        assert_eq!(step.pause(attempt), CHAIN_RETRY_AFTER, "attempt {attempt}");
    }
}

/// The outbox row of every task as an executor publishes it: its format, its body, its metadata (the causation of
/// the command that planned it), its schedule, its attempt budget and its id, derived from the command. The four
/// tasks the two aggregates plan, under the configuration without environment.
#[tokio::test]
async fn every_task_row_keeps_its_format_body_schedule_budget_and_derived_id() {
    let policies = policies(&[]).unwrap();
    let concerts = TestCommandExecutor::new(MemStore::<Concert>::default(), ConcertRules::default())
        .with_task_planner(ReactionsPlanner::<Concert>::new(policies.clone()));
    let fans = TestCommandExecutor::new(MemStore::<Fan>::default(), ()).with_task_planner(ReactionsPlanner::<Fan>::new(policies));
    let concert_id = ConcertId::parse("radiohead-2026-11-21").unwrap();
    let fox = FanId::parse("fox").unwrap();
    let ledger = FanLedgerId::new(&fox, &concert_id);
    let (booking_id, cancellation_id) = (Uuid::from_u128(1), Uuid::from_u128(2));
    let announce = AnnounceConcert {
        artist: "Radiohead".into(),
        city: "Tbilisi".into(),
        date: NaiveDate::from_ymd_opt(2026, 11, 21).unwrap(),
    };
    concerts
        .execute(&concert_id, ConcertCommand::AnnounceConcert(announce))
        .await
        .unwrap();

    // The box office books Fox's A1: the ledger records the ticket.
    let booking = Causation::root("ui");
    let book = BookSeat {
        seat: "A1".into(),
        fan_id: fox.clone(),
        fan_name: "Fox".into(),
        booking_id,
    };
    let booked = concerts
        .execute_with_metadata(&concert_id, ConcertCommand::BookSeat(book), booking.event_metadata())
        .await
        .unwrap();
    let [task] = &booked.tasks[..] else {
        panic!("one task per booking: {:?}", booked.tasks);
    };
    let format = TaskFormat::try_new("encore_record_ticket", 1).unwrap();
    assert_eq!(
        (task.id(), task.format(), task.schedule(), task.max_attempts().get()),
        (
            booking.task_id("Concert", "radiohead-2026-11-21", &format, 0),
            &format,
            &TaskSchedule::Now,
            6
        )
    );
    assert_eq!(
        task.payload(),
        &json!({ "booking_id": booking_id, "concert_id": "radiohead-2026-11-21", "seat": "A1", "zone": "vip",
                 "price": 120, "fan_id": "fox", "fan_name": "Fox" })
    );
    assert_eq!(
        task.metadata(),
        &json!({ "command_id": booking.command_id, "correlation_id": booking.correlation_id,
                 "causation_id": booking.causation_id })
    );

    // The box office cancels it: the ledger records the cancellation with the price paid.
    let cancelling = Causation::root("ui");
    let cancel = CancelBooking {
        seat: "A1".into(),
        fan_id: fox.clone(),
        cancellation_id,
    };
    let cancelled = concerts
        .execute_with_metadata(
            &concert_id,
            ConcertCommand::CancelBooking(cancel),
            cancelling.event_metadata(),
        )
        .await
        .unwrap();
    let [task] = &cancelled.tasks[..] else {
        panic!("one task per cancellation: {:?}", cancelled.tasks);
    };
    let format = TaskFormat::try_new("encore_record_cancellation", 1).unwrap();
    assert_eq!(
        (task.id(), task.format(), task.schedule(), task.max_attempts().get()),
        (
            cancelling.task_id("Concert", "radiohead-2026-11-21", &format, 0),
            &format,
            &TaskSchedule::Now,
            6
        )
    );
    assert_eq!(
        task.payload(),
        &json!({ "booking_id": booking_id, "cancellation_id": cancellation_id, "concert_id": "radiohead-2026-11-21",
                 "seat": "A1", "fan_id": "fox", "refund": 120 })
    );
    assert_eq!(
        task.metadata(),
        &json!({ "command_id": cancelling.command_id, "correlation_id": cancelling.correlation_id,
                 "causation_id": cancelling.causation_id })
    );

    // The ledger records the ticket: the fan gets the e-ticket.
    let recording = Causation::root("ui");
    let record = RecordTicket {
        booking_id,
        concert_id: concert_id.clone(),
        seat: SeatId::parse("A1").unwrap(),
        zone: Zone::Vip,
        price: 120,
        fan_name: "Fox".into(),
    };
    let recorded = fans
        .execute_with_metadata(&ledger, FanCommand::RecordTicket(record), recording.event_metadata())
        .await
        .unwrap();
    let [task] = &recorded.tasks[..] else {
        panic!("one e-ticket per recorded ticket: {:?}", recorded.tasks);
    };
    let format = TaskFormat::try_new("encore_send_e_ticket", 1).unwrap();
    assert_eq!(
        (task.id(), task.format(), task.schedule(), task.max_attempts().get()),
        (
            recording.task_id("Fan", "fox:radiohead-2026-11-21", &format, 0),
            &format,
            &TaskSchedule::Now,
            6
        )
    );
    assert_eq!(
        task.payload(),
        &json!({ "booking_id": booking_id, "concert_id": "radiohead-2026-11-21", "seat": "A1", "fan_id": "fox",
                 "fan_name": "Fox", "price": 120 })
    );
    assert_eq!(
        task.metadata(),
        &json!({ "command_id": recording.command_id, "correlation_id": recording.correlation_id,
                 "causation_id": recording.causation_id })
    );

    // The ledger records the cancellation: the fan gets the refund.
    let refunding = Causation::root("ui");
    let record = RecordCancellation {
        booking_id,
        cancellation_id,
        refund: 120,
    };
    let refunded = fans
        .execute_with_metadata(&ledger, FanCommand::RecordCancellation(record), refunding.event_metadata())
        .await
        .unwrap();
    let [task] = &refunded.tasks[..] else {
        panic!("one refund per cancelled ticket: {:?}", refunded.tasks);
    };
    let format = TaskFormat::try_new("encore_issue_refund", 1).unwrap();
    assert_eq!(
        (task.id(), task.format(), task.schedule(), task.max_attempts().get()),
        (
            refunding.task_id("Fan", "fox:radiohead-2026-11-21", &format, 0),
            &format,
            &TaskSchedule::Now,
            6
        )
    );
    assert_eq!(
        task.payload(),
        &json!({ "cancellation_id": cancellation_id, "booking_id": booking_id, "concert_id": "radiohead-2026-11-21",
                 "seat": "A1", "fan_id": "fox", "amount": 120 })
    );
    assert_eq!(
        task.metadata(),
        &json!({ "command_id": refunding.command_id, "correlation_id": refunding.correlation_id,
                 "causation_id": refunding.causation_id })
    );
}

#[test]
fn the_mail_settings_come_from_the_configuration() {
    let settings = mail_settings(&[("ENCORE_MAIL_SERVICE_URL", "http://mail:8025")]).unwrap();
    assert_eq!(settings.base_url, "http://mail:8025");
    assert_eq!(
        settings.timeout,
        Duration::from_secs(4),
        "a second before the handler timeout of the demo"
    );
}

/// The four task types have one handler each, in one dispatcher: the steps need the ledgers' executor, the e-mails
/// the mail service, which the assembly refuses to leave out.
#[tokio::test]
async fn the_four_task_types_have_one_handler_each_with_the_mailer_and_the_ledgers_executor() {
    let fans = Arc::new(TestCommandExecutor::new(MemStore::<Fan>::default(), ()));
    let mail: Arc<Mailer> = Arc::new(RecordingMailer::delivering());
    let activity = Arc::new(ActivityHub::new(1, 1));
    let dispatcher = handlers(fans.clone(), mail.clone(), activity.clone()).unwrap();
    assert_eq!(dispatcher.formats(), formats());
    let without_mail = TaskHandlers::new()
        .handler(send_e_ticket)
        .executor(fans)
        .build()
        .err()
        .unwrap();
    assert_eq!(
        without_mail.to_string(),
        "the handler of encore_send_e_ticket v1 needs `dyn encore_backend::shared::application::mail::MailService`, \
         which nothing provides"
    );
    let mailer = mailer().unwrap();
    assert!(
        handlers(
            Arc::new(TestCommandExecutor::new(MemStore::<Fan>::default(), ())),
            mailer,
            activity
        )
        .is_ok()
    );
}

/// The e-mail handlers report every attempt to the activity log: an assembly without its hub is refused before a
/// worker could run one, whether it assembles the handlers itself or the engine does.
#[test]
fn an_assembly_without_the_activity_hub_is_refused() {
    let mail: Arc<Mailer> = Arc::new(RecordingMailer::delivering());
    let without_hub = TaskHandlers::new()
        .handler(send_e_ticket)
        .provide(mail)
        .build()
        .err()
        .unwrap();
    assert_eq!(
        without_hub.to_string(),
        "the handler of encore_send_e_ticket v1 needs `encore_backend::shared::application::activity::ActivityHub`, \
         which nothing provides"
    );
    let without_operations = ready(Cqrs::in_memory().module(concert::module).module(fan::module).build());
    assert_eq!(
        without_operations.err().unwrap().to_string(),
        "task handlers: the handler of encore_send_e_ticket v1 needs \
         `encore_backend::shared::application::activity::ActivityHub`, which nothing provides"
    );
}
